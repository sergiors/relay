package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"relay/internal/processlock"
	"relay/internal/runtime"
)

// This file implements the worker-owned LIVE runtime-pool query socket at
// /run/relay/relay.sock, alongside the process lock this worker runs under.
//
// It carries ONLY the live warm-container pool gauges (capacity and container
// counts by lease state). The cumulative acquire/discard counters are PERSISTED
// per function under /var/lib/relay (state.FunctionStats) and read there by the
// standalone CLI; they are deliberately never sent over the socket, because the
// socket's whole purpose is the ephemeral worker-local view the persisted state
// cannot provide. Conversely the live gauges are never persisted, because a
// persisted gauge would go stale between flushes.
//
// Lifecycle ownership is the worker's: it creates the runtime directory,
// removes a STALE socket left by a SIGKILLed worker, binds, serves, stops
// serving, closes the listener, and removes the socket file on graceful
// shutdown. Removing a stale socket is safe ONLY because `relay start` has
// already taken the process lock (internal/processlock) before worker.Run runs:
// a second Relay process fails that lock and never reaches socket setup, so the
// socket it would find cannot belong to an active worker.

// SocketPath is the fixed live query socket `relay function inspect` dials for
// live runtime-pool gauges. It lives beside the process lock in the ephemeral
// runtime directory (internal/processlock.DefaultDir), NOT under the persisted
// /var/lib/relay state volume: a socket is process state that can neither
// outlive the worker nor be meaningfully persisted.
const SocketPath = processlock.DefaultDir + "/relay.sock"

// ensureSocketDir creates the socket's parent directory (the ephemeral runtime
// dir) with mode 0o755. It is called BEFORE binding so a fresh container without
// /run/relay can never fail the bind, and it is the socket file's own
// directory-creation helper so socket.go needs no shared path package.
func ensureSocketDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

const (
	// runtimeStateDialTimeout bounds the CLI's dial to the worker socket, so a
	// missing/unresponsive worker cannot stall `relay function inspect`.
	runtimeStateDialTimeout = 1 * time.Second
	// runtimeStateRequestTimeout bounds one query end to end: it is set as the
	// connection deadline on BOTH sides (the CLI read and the worker read/write),
	// so a wedged peer can never hold a handler open indefinitely.
	runtimeStateRequestTimeout = 2 * time.Second
	// runtimeStateAcceptBackoff spaces retries of a transient accept failure so
	// the serve loop cannot spin.
	runtimeStateAcceptBackoff = 100 * time.Millisecond
	// runtimeStateMaxRequest / runtimeStateMaxResponse bound the bytes a single
	// local query may carry, so a misbehaving peer cannot make either side
	// allocate unboundedly.
	runtimeStateMaxRequest  = 64 << 10
	runtimeStateMaxResponse = 64 << 10
)

// Wire error codes. They are stable strings (not prose) so the CLI can
// distinguish "the worker has no pool for this function" from any other
// failure without parsing a message.
const (
	errCodeMalformedRequest = "malformed_request"
	errCodeUnknownFunction  = "unknown_function"
)

// ErrRuntimeStateUnavailable reports that the live worker query socket could not
// be reached or did not answer — no worker is running, the socket is stale, or
// the exchange failed. The CLI treats it as "render the live fields unknown".
var ErrRuntimeStateUnavailable = errors.New("runtime state unavailable")

// ErrUnknownFunction reports that the worker answered but has no live pool for
// the requested function (it was never warmed, or was already removed).
var ErrUnknownFunction = errors.New("unknown function")

// RuntimeState is the live, worker-local runtime-pool gauge view returned by the
// query socket. Every field is a valid zero for a function with an existing but
// empty pool, so callers must key "known" on the query succeeding, never on the
// values.
type RuntimeState struct {
	Capacity   int `json:"capacity"`
	Containers int `json:"containers"`
	Busy       int `json:"busy"`
	Idle       int `json:"idle"`
	Starting   int `json:"starting"`
}

// socketRequest is the newline-JSON request frame: the function whose live pool
// gauges are requested.
type socketRequest struct {
	Function string `json:"function"`
}

// socketResponse is the newline-JSON response frame. Exactly one of the
// embedded RuntimeState (success) or Error (failure) is present. Embedding
// flattens the gauges to the top level, so a success frame carries only
// capacity/containers/busy/idle/starting and a failure frame only error.
type socketResponse struct {
	*RuntimeState
	Error string `json:"error,omitempty"`
}

// PoolSnapshotter is the minimal manager view the socket needs. *runtime.Manager
// satisfies it via its live PoolSnapshot method, which keeps the socket
// decoupled from Docker and from the pool internals.
type PoolSnapshotter interface {
	PoolSnapshot(name string) (runtime.PoolSnapshot, bool)
}

// SocketServer is the worker-owned live query socket. It accepts one request
// per connection, answers with a single newline-JSON frame, and is stopped as a
// unit: Close stops accepting, closes every in-flight connection (each already
// bounded by its own deadline), waits for handlers, and unlinks the socket file.
type SocketServer struct {
	path    string
	log     *slog.Logger
	manager PoolSnapshotter
	ln      net.Listener

	mu     sync.Mutex
	closed bool
	conns  map[net.Conn]struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewSocketServer creates path's parent directory, removes any stale socket
// file, binds the Unix socket at path, and starts serving in a goroutine. It
// returns an error only for an unrecoverable setup failure (directory, stale
// removal, or bind); a later transient accept failure is logged and retried,
// never fatal. The bound path is stored in the returned server, so every later
// operation (serve, close, unlink) uses that instance path and never a
// package-level one.
//
// The stale removal is safe because the caller (`relay start` via worker.Run)
// holds the process lock: see the file comment.
func NewSocketServer(path string, manager PoolSnapshotter, logger *slog.Logger) (*SocketServer, error) {
	if err := ensureSocketDir(path); err != nil {
		return nil, fmt.Errorf("create runtime dir: %w", err)
	}
	if err := removeStaleSocket(path); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", path, err)
	}
	s := &SocketServer{
		path:    path,
		log:     logger,
		manager: manager,
		ln:      ln,
		conns:   make(map[net.Conn]struct{}),
		done:    make(chan struct{}),
	}
	go s.serve()
	return s, nil
}

// removeStaleSocket unlinks a leftover socket file from a previous worker so the
// subsequent bind cannot fail with "address already in use". A missing file is
// not an error.
func removeStaleSocket(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale socket %q: %w", path, err)
	}
	return nil
}

// serve accepts and dispatches connections until Close. The listener's own
// Close surfaces as an accept error, which isClosed turns into a clean return.
func (s *SocketServer) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.isClosed() {
				return
			}
			// A transient accept error (e.g. the process is out of file
			// descriptors) must not kill the socket: back off, then retry, unless
			// Close raced in.
			s.log.Warn("Runtime state socket: accept failed", "error", err)
			select {
			case <-time.After(runtimeStateAcceptBackoff):
			case <-s.done:
				return
			}
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.wg.Add(1)
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// handle serves exactly one query on conn. Every exit path is bounded by the
// connection deadline set here, so a peer that never sends or never reads
// cannot leak the handler.
func (s *SocketServer) handle(conn net.Conn) {
	defer s.wg.Done()
	defer s.untrack(conn)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(runtimeStateRequestTimeout))

	line, err := bufio.NewReader(io.LimitReader(conn, runtimeStateMaxRequest)).ReadBytes('\n')
	if err != nil && len(bytes.TrimSpace(line)) == 0 {
		// The peer vanished or sent nothing before the deadline: there is no
		// request to answer.
		return
	}
	var req socketRequest
	if err := json.Unmarshal(line, &req); err != nil || req.Function == "" {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}
	snap, ok := s.manager.PoolSnapshot(req.Function)
	if !ok {
		s.respond(conn, socketResponse{Error: errCodeUnknownFunction})
		return
	}
	s.respond(conn, socketResponse{RuntimeState: &RuntimeState{
		Capacity:   snap.Capacity,
		Containers: snap.Containers,
		Busy:       snap.Busy,
		Idle:       snap.Idle,
		Starting:   snap.Starting,
	}})
}

// respond writes one newline-JSON frame. json.Encoder appends the trailing
// newline the line protocol expects; a write error is ignored because the peer
// is already gone and there is nothing to recover.
func (s *SocketServer) respond(conn net.Conn, resp socketResponse) {
	_ = json.NewEncoder(conn).Encode(resp)
}

// untrack removes conn from the in-flight set after its handler returns.
func (s *SocketServer) untrack(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

// isClosed reports whether Close has run.
func (s *SocketServer) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close stops serving, closes every in-flight connection and the listener, waits
// for handlers (each bounded by its own deadline), and removes the socket file.
// It is idempotent and nil-safe, so the worker can both stop it explicitly in
// the shutdown tail and defer it as a safety net. Removing the file is what lets
// the next start bind cleanly and stops the CLI from resolving a dead endpoint.
func (s *SocketServer) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	ln := s.ln
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	var firstErr error
	if ln != nil {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			firstErr = err
		}
	}
	for _, c := range conns {
		_ = c.Close()
	}
	s.wg.Wait()
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// QueryRuntimeState dials the live worker query socket at path and asks for
// function's live pool gauges. It is the CLI's whole client surface: callers
// treat ErrRuntimeStateUnavailable and ErrUnknownFunction (detected with
// errors.Is) as "live fields unknown" and render the persisted counters.
//
// It is deliberately in internal/worker (which the CLI already imports for
// `relay start`) and returns only the wire value type, so there is no package
// cycle and the CLI never holds a worker or manager.
func QueryRuntimeState(path, function string) (RuntimeState, error) {
	d := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := d.Dial("unix", path)
	if err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(runtimeStateRequestTimeout))

	if err := json.NewEncoder(conn).Encode(socketRequest{Function: function}); err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	if resp.Error != "" {
		if resp.Error == errCodeUnknownFunction {
			return RuntimeState{}, fmt.Errorf("%w: %s", ErrUnknownFunction, function)
		}
		return RuntimeState{}, fmt.Errorf("%w: %s", ErrRuntimeStateUnavailable, resp.Error)
	}
	if resp.RuntimeState == nil {
		return RuntimeState{}, fmt.Errorf("%w: empty response", ErrRuntimeStateUnavailable)
	}
	return *resp.RuntimeState, nil
}
