package worker

import (
	"bufio"
	"bytes"
	"context"
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

	"relay/internal/function"
	"relay/internal/processlock"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// This file implements the worker-owned LIVE runtime-pool query socket at
// /run/relay/relay.sock, alongside the process lock this worker runs under.
//
// It carries four semantic operations: the live warm-container pool gauges
// (capacity and container counts by lease state), the operator-facing
// "reset stats" command, the synchronous manual function invocation
// (`relay function invoke`), and the synchronous DLQ replay
// (`relay dlq replay`), which re-executes one dead-lettered entry's exact
// recorded function/handler once against the live registry. The cumulative
// acquire/discard counters are
// PERSISTED per function under /var/lib/relay (state.FunctionStats) and read
// there by the standalone CLI; they are deliberately never sent over the
// socket, because the socket's whole purpose is the ephemeral worker-local view
// the persisted state cannot provide. Conversely the live gauges are never
// persisted, because a persisted gauge would go stale between flushes. The reset
// command carries no storage details: the worker resets its own in-memory source
// and persisted rows behind its StatsResetter, so the socket never learns about
// SQLite. Manual invocation likewise carries no broker details: it dispatches to
// the live runner, which executes the function's matching handlers but never
// touches Redis streams, ACK/retry, invocation state, or the DLQ.
//
// Lifecycle ownership is the worker's: it creates the runtime directory,
// removes a STALE socket left by a SIGKILLed worker, binds, serves, stops
// serving, closes the listener, and removes the socket file on graceful
// shutdown. Removing a stale socket is safe ONLY because `relay start` has
// already taken the process lock (internal/processlock) before worker.Run runs:
// a second Relay process fails that lock and never reaches socket setup, so the
// socket it would find cannot belong to an active worker.

// SocketPath is the fixed live query socket `relay function inspect` dials for
// live runtime-pool gauges, `relay stats reset` dials to reset a running
// worker's statistics, `relay function invoke` dials to run a function's
// matching handlers synchronously against the live runtime, and `relay dlq
// replay` dials to re-execute one dead-lettered handler. It lives beside the
// process lock in the ephemeral runtime directory
// (internal/processlock.DefaultDir), NOT under the persisted /var/lib/relay
// state volume: a socket is process state that can neither outlive the worker
// nor be meaningfully persisted.
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
	// invokeTimeout bounds one manual invocation end to end on the WORKER side:
	// it is the context deadline the runner's executor runs under, so a wedged
	// container cannot hold the socket handler (and the CLI's dial) open
	// indefinitely. It deliberately exceeds the largest allowed rule timeout
	// (function.MaxTimeout, 5m) plus a margin for container start/model load, so
	// a legitimate long-running handler is never cut short by the socket itself;
	// the per-rule timeout still bounds the actual execution.
	invokeTimeout = function.MaxTimeout + 2*time.Minute
)

// Wire error codes. They are stable strings (not prose) so the CLI can
// distinguish "the worker has no pool for this function" from any other
// failure without parsing a message.
const (
	errCodeMalformedRequest = "malformed_request"
	errCodeUnknownFunction  = "unknown_function"
	errCodeStatsUnavailable = "stats_unavailable"
	errCodeStatsFailed      = "stats_reset_failed"
	// Manual-invocation wire codes. errCodeInvokeUnavailable reports that no
	// runner is wired (the worker is not serving invocations);
	// errCodeFunctionUnavailable reports a registered function whose image could
	// not be built; errCodeInvokeFailed reports that every matching handler was
	// attempted but at least one failed (or the invocation could not start).
	errCodeInvokeUnavailable   = "invoke_unavailable"
	errCodeFunctionUnavailable = "function_unavailable"
	errCodeInvokeFailed        = "invoke_failed"
	// DLQ-replay wire codes. errCodeHandlerNotFound reports that the function is
	// currently configured but the exact handler recorded in the DLQ entry is no
	// longer in its template (an intentional configuration change);
	// errCodeReplayFailed reports that the single replay attempt executed and
	// failed. The CLI keeps the DLQ entry in both cases.
	errCodeHandlerNotFound = "handler_not_found"
	errCodeReplayFailed    = "replay_failed"
)

// Wire commands. Every request frame MUST name its command explicitly:
// cmdRuntimeState requests function's live pool gauges, cmdResetStats asks the
// worker to reset its Relay statistics (the command name is the operator-facing
// semantic, not a DB operation — the socket never exposes storage details), and
// cmdInvokeFunction asks the worker to run function's matching event handlers
// synchronously against the live runner. An absent or unknown command is
// malformed.
const (
	cmdRuntimeState   = "runtime_state"
	cmdResetStats     = "reset_stats"
	cmdInvokeFunction = "invoke_function"
	cmdReplayDLQ      = "replay_dlq"
)

// ErrRuntimeStateUnavailable reports that the live worker query socket could not
// be reached or did not answer — no worker is running, the socket is stale, or
// the exchange failed. The CLI treats it as "render the live fields unknown".
var ErrRuntimeStateUnavailable = errors.New("runtime state unavailable")

// ErrUnknownFunction reports that the worker answered but has no live pool for
// the requested function (it was never warmed, or was already removed).
var ErrUnknownFunction = errors.New("unknown function")

// ErrRuntimeStatsUnavailable reports that the worker socket could not reset
// Relay's statistics because no worker answered — it is unreachable, the socket
// is stale, or the exchange failed. The CLI treats it as "no worker reset; fall
// back to resetting the state database directly".
var ErrRuntimeStatsUnavailable = errors.New("runtime stats unavailable")

// ErrRuntimeStatsFailed reports that a worker DID answer the reset command but
// could not complete it (e.g. its state handle is unavailable). The CLI surfaces
// it rather than silently falling back, so a failed worker reset is not masked.
var ErrRuntimeStatsFailed = errors.New("runtime stats reset failed")

// ErrInvokeUnavailable reports that the live worker query socket could not be
// reached — no worker is running, the socket is stale, or the exchange failed —
// or that the worker has no runner wired to serve manual invocations. The CLI
// surfaces it as an error (a manual invocation has no offline fallback: it must
// run through the live runtime pool).
var ErrInvokeUnavailable = errors.New("function invocation unavailable")

// ErrInvokeFailed reports that the worker attempted the manual invocation but at
// least one matching handler failed, or the invocation could not start (an
// unknown/unavailable function, or a concurrency slot timeout). The worker's
// error text is preserved (the CLI prints it) and no handler count is reported.
var ErrInvokeFailed = errors.New("function invocation failed")

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

// InvokeResult is the synchronous manual-invocation result returned by the query
// socket: the number of matching handlers the runner executed. It is only
// meaningful on success; a failed invocation is reported through Error instead.
type InvokeResult struct {
	Invoked int `json:"invoked"`
}

// socketRequest is the newline-JSON request frame. Command selects the
// operation: cmdRuntimeState requests function's live pool gauges (Function is
// required); cmdResetStats asks the worker to reset its Relay statistics;
// cmdInvokeFunction asks the worker to run function's matching event handlers
// synchronously (Function is required and Event carries the operator-supplied
// JSON event object). A frame carries exactly one command, and an absent or
// unknown command is malformed.
type socketRequest struct {
	Command  string          `json:"command,omitempty"`
	Function string          `json:"function,omitempty"`
	Handler  string          `json:"handler,omitempty"`
	Event    json.RawMessage `json:"event,omitempty"`
}

// socketResponse is the newline-JSON response frame. Exactly one of the
// embedded RuntimeState (success), Error (failure), ResetStats=true
// (reset success), or *InvokeResult (manual-invocation success) is present.
// Message optionally carries human-readable detail for an Error, so the CLI can
// surface a clear cause (e.g. a handler failure reason) while Error remains the
// stable machine code it classifies on.
type socketResponse struct {
	*RuntimeState
	*InvokeResult
	ResetStats bool   `json:"reset_stats,omitempty"`
	Replayed   bool   `json:"replayed,omitempty"`
	Error      string `json:"error,omitempty"`
	Message    string `json:"message,omitempty"`
}

// PoolSnapshotter is the minimal manager view the runtime-state query needs.
// *runtime.Manager satisfies it via its live PoolSnapshot method, which keeps
// the socket decoupled from Docker and from the pool internals.
type PoolSnapshotter interface {
	PoolSnapshot(name string) (runtime.PoolSnapshot, bool)
}

// StatsResetter resets Relay's accumulated in-memory statistics so the persisted
// flush continues from zero. It is implemented by the worker's own stats source
// (the stats flusher): the implementation MUST perform the reset and capture the
// new worker-owned baseline under the SAME lock the flush holds, so a flush that
// already captured a pre-reset snapshot cannot write it after the reset. It
// deliberately carries no DB details: the socket only knows the semantic "reset
// stats" operation, never storage. A non-nil error means the worker could not
// complete the reset (e.g. its state handle is unavailable or a payload is
// corrupt), so the socket reports stats_reset_failed and the CLI surfaces it
// rather than falling back.
type StatsResetter interface {
	ResetStats() error
}

// FunctionInvoker is the minimal live-runner view the manual-invocation command
// needs. *runner.Runner satisfies it via its InvokeFunction method, which
// selects one named function's matching event rules and executes them without
// touching the broker lifecycle. Keeping the interface local (and the event as a
// plain map) means the socket dispatches through a narrow seam rather than the
// runner's concrete type. The socket does import runner to classify the
// sentinel errors the runner returns (ErrFunctionNotFound /
// ErrFunctionUnavailable) onto stable wire codes; that import is for the error
// contract, not dispatch.
type FunctionInvoker interface {
	InvokeFunction(ctx context.Context, name string, event map[string]any) (int, error)
}

// HandlerReplayer is the minimal live-runner view the DLQ-replay command needs.
// *runner.Runner satisfies it via its ReplayDLQ method, which validates the
// exact recorded function/handler against the CURRENT registry and executes
// exactly that one handler once, with no broker lifecycle. Keeping the interface
// local (and the event as raw bytes replayed verbatim) means the socket
// dispatches through a narrow seam rather than the runner's concrete type. The
// socket imports runner only to classify the sentinel errors ReplayDLQ returns
// (ErrFunctionNotFound / ErrFunctionUnavailable / ErrHandlerNotFound) onto stable
// wire codes; that import is for the error contract, not dispatch.
type HandlerReplayer interface {
	ReplayDLQ(ctx context.Context, name, handler string, event []byte) error
}

// SocketServer is the worker-owned live query socket. It accepts one request
// per connection, answers with a single newline-JSON frame, and is stopped as a
// unit: Close stops accepting, closes every in-flight connection (each already
// bounded by its own deadline), waits for handlers, and unlinks the socket file.
type SocketServer struct {
	path     string
	log      *slog.Logger
	manager  PoolSnapshotter
	resetter StatsResetter
	// invoker serves the synchronous manual-invocation command against the LIVE
	// runner. It is wired after the socket is created (see SetInvoker) because
	// the runner does not exist yet at socket construction; a nil invoker
	// answers invoke_unavailable. Both the setter and the handler read it under
	// s.mu, so a request that races the wiring sees either nil (unavailable) or
	// the fully built runner — never a torn value.
	invoker FunctionInvoker
	// replayer serves the synchronous DLQ-replay command against the LIVE
	// runner. It is the SAME runner instance SetInvoker wires, set via
	// SetReplayer; a nil replayer answers invoke_unavailable (there is no
	// offline fallback). Both the setter and the handler read it under s.mu.
	replayer HandlerReplayer
	ln       net.Listener

	// baseCtx is cancelled by Close, so an in-flight manual invocation (whose
	// own context bounds it to invokeTimeout) is cancelled promptly on shutdown
	// instead of holding Close's handler wait for up to invokeTimeout. It is
	// created once in NewSocketServer.
	baseCtx context.Context
	cancel  context.CancelFunc

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
// resetter handles the semantic "reset stats" command. Run always supplies the
// stats flusher (constructed with the state handle even when metrics are
// disabled), so production always has a resetter; a nil resetter answers
// stats_unavailable and the CLI falls back to resetting the state database
// directly.
//
// The stale removal is safe because the caller (`relay start` via worker.Run)
// holds the process lock: see the file comment.
func NewSocketServer(
	path string,
	manager PoolSnapshotter,
	resetter StatsResetter,
	logger *slog.Logger,
) (*SocketServer, error) {
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
		path:     path,
		log:      logger,
		manager:  manager,
		resetter: resetter,
		ln:       ln,
		conns:    make(map[net.Conn]struct{}),
		done:     make(chan struct{}),
	}
	s.baseCtx, s.cancel = context.WithCancel(context.Background())
	go s.serve()
	return s, nil
}

// SetInvoker wires the live runner that serves the synchronous manual-invocation
// command. It is called after the socket is constructed and the runner is built
// (the socket must start before the runner so a pool query works as early as
// possible, while the runner needs the reconciler/stream wiring to exist first).
// The assignment is mutex-guarded so a request racing the wiring reads either nil
// (invoke_unavailable) or the fully built runner. Passing nil leaves manual
// invocation unavailable, which is the correct pre-wiring state.
func (s *SocketServer) SetInvoker(invoker FunctionInvoker) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.invoker = invoker
	s.mu.Unlock()
}

// currentInvoker returns the wired manual invoker, or nil when none is set. It
// takes the mutex so a concurrent SetInvoker is race-free.
func (s *SocketServer) currentInvoker() FunctionInvoker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invoker
}

// SetReplayer wires the live runner that serves the DLQ-replay command. It is
// called alongside SetInvoker (the same runner implements both seams); passing
// nil leaves replay unavailable. The assignment is mutex-guarded so a request
// racing the wiring reads either nil or the fully built runner.
func (s *SocketServer) SetReplayer(replayer HandlerReplayer) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.replayer = replayer
	s.mu.Unlock()
}

// currentReplayer returns the wired DLQ replayer, or nil when none is set. It
// takes the mutex so a concurrent SetReplayer is race-free.
func (s *SocketServer) currentReplayer() HandlerReplayer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replayer
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
	if err := json.Unmarshal(line, &req); err != nil {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}
	switch req.Command {
	case cmdResetStats:
		s.handleResetStats(conn)
	case cmdRuntimeState:
		s.handleRuntimeState(conn, req.Function)
	case cmdInvokeFunction:
		s.handleInvokeFunction(conn, req.Function, req.Event)
	case cmdReplayDLQ:
		s.handleReplayDLQ(conn, req.Function, req.Handler, req.Event)
	default:
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
	}
}

// handleResetStats resets the worker's accumulated Relay statistics through the
// StatsResetter. A nil resetter answers stats_unavailable so the CLI falls back
// to the state database; a reset error answers stats_reset_failed so the CLI
// surfaces it instead of masking a failed worker reset. On success the frame
// carries reset_stats.
func (s *SocketServer) handleResetStats(conn net.Conn) {
	if s.resetter == nil {
		s.respond(conn, socketResponse{Error: errCodeStatsUnavailable})
		return
	}
	if err := s.resetter.ResetStats(); err != nil {
		s.respond(conn, socketResponse{Error: errCodeStatsFailed})
		return
	}
	s.respond(conn, socketResponse{ResetStats: true})
}

// handleRuntimeState answers the live pool-gauges query for one function. An
// empty function name or an unknown function is reported without gauges so the
// CLI renders the live fields unknown.
func (s *SocketServer) handleRuntimeState(conn net.Conn, function string) {
	if function == "" {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}
	snap, ok := s.manager.PoolSnapshot(function)
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

// handleInvokeFunction answers the synchronous manual-invocation command for one
// function. An empty function name or an absent/invalid event object is
// malformed. A nil runner answers invoke_unavailable (the CLI has no offline
// fallback, so this is a hard error). The runner selects the function's matching
// event rules and executes them through the live runtime pool, bounded by
// invokeTimeout on the worker side; a handler count answers with an
// invoke_result, while a runner error is classified:
//
//   - an unknown function answers unknown_function;
//   - a registered but unrunnable function answers function_unavailable;
//   - any other error answers invoke_failed.
//
// The error's text is carried in Message (and logged) so the CLI can surface a
// clear cause, while Error stays the stable code it classifies on. It
// deliberately never touches Redis, ACK/retry, invocation state, or the DLQ:
// dispatch is entirely within the runner's broker-free manual path.
func (s *SocketServer) handleInvokeFunction(conn net.Conn, function string, rawEvent json.RawMessage) {
	if function == "" || len(rawEvent) == 0 {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}
	// The event must be a JSON object because the matcher is defined over a
	// map[string]any; reject arrays/scalars/null with the malformed code so the
	// CLI's validation and the worker's agree.
	var event map[string]any
	if err := json.Unmarshal(rawEvent, &event); err != nil || event == nil {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}

	invoker := s.currentInvoker()
	if invoker == nil {
		s.respond(conn, socketResponse{Error: errCodeInvokeUnavailable})
		return
	}

	// A manual invocation can legitimately run a handler far longer than the
	// short query exchange the connection deadline was set for at the top of
	// handle; extend the deadline for the invocation so the response write is not
	// cut off, while still bounding the whole exchange so a wedged peer can never
	// hold the handler open. The runner enforces its own per-rule timeout.
	_ = conn.SetDeadline(time.Now().Add(invokeTimeout + runtimeStateRequestTimeout))

	// Bound the worker side of a manual invocation so a wedged container can
	// never hold the handler (and the CLI's dial) open indefinitely. The
	// per-rule timeout still bounds the actual execution. The server's base
	// context is also a parent, so Close cancels an in-flight invocation
	// promptly instead of waiting for invokeTimeout.
	ctx, cancel := context.WithTimeout(s.baseCtx, invokeTimeout)
	defer cancel()
	invoked, err := invoker.InvokeFunction(ctx, function, event)
	if err != nil {
		code := invokeErrorCode(err)
		s.log.Warn("Function invoke: manual invocation failed",
			"function", function,
			"code", code,
			"error", err,
		)
		s.respond(conn, socketResponse{Error: code, Message: err.Error()})
		return
	}
	s.respond(conn, socketResponse{InvokeResult: &InvokeResult{Invoked: invoked}})
}

// handleReplayDLQ answers the synchronous DLQ-replay command for the exact
// recorded function/handler. An empty function or handler, or an absent event
// payload, is malformed. A nil replayer answers invoke_unavailable (there is no
// offline fallback). The runner validates the function and handler against the
// CURRENT registry and executes exactly that one handler once, bounded by
// invokeTimeout on the worker side; it never touches Redis, ACK/retry,
// invocation state, event classification, or the DLQ.
//
// The runner's errors are classified:
//
//   - an unknown function answers unknown_function;
//   - a registered but unrunnable function answers function_unavailable;
//   - a handler no longer in the current template answers handler_not_found;
//   - any other error answers replay_failed.
//
// The error's text is carried in Message (and logged) so the CLI can surface a
// clear cause, while Error stays the stable code it classifies on. On success the
// frame carries replayed=true; the CLI deletes the DLQ entry only then.
func (s *SocketServer) handleReplayDLQ(conn net.Conn, function, handler string, rawEvent json.RawMessage) {
	if function == "" || handler == "" || len(rawEvent) == 0 {
		s.respond(conn, socketResponse{Error: errCodeMalformedRequest})
		return
	}

	replayer := s.currentReplayer()
	if replayer == nil {
		s.respond(conn, socketResponse{Error: errCodeInvokeUnavailable})
		return
	}

	// A replay can legitimately run a handler for up to the rule timeout, so
	// extend the connection deadline exactly as a manual invocation does, and
	// bound the worker side with the same invokeTimeout. Close still cancels an
	// in-flight replay promptly via the server's base context.
	_ = conn.SetDeadline(time.Now().Add(invokeTimeout + runtimeStateRequestTimeout))

	ctx, cancel := context.WithTimeout(s.baseCtx, invokeTimeout)
	defer cancel()
	if err := replayer.ReplayDLQ(ctx, function, handler, rawEvent); err != nil {
		code := replayErrorCode(err)
		s.log.Warn("DLQ replay: handler execution failed",
			"function", function,
			"handler", handler,
			"code", code,
			"error", err,
		)
		s.respond(conn, socketResponse{Error: code, Message: err.Error()})
		return
	}
	s.respond(conn, socketResponse{Replayed: true})
}

// replayErrorCode maps a runner DLQ-replay error onto a stable wire code. The
// runner's sentinels are recognized with errors.Is; everything else is a handler
// failure. It keeps the wire classification independent of error prose.
func replayErrorCode(err error) string {
	switch {
	case errors.Is(err, runner.ErrFunctionNotFound):
		return errCodeUnknownFunction
	case errors.Is(err, runner.ErrFunctionUnavailable):
		return errCodeFunctionUnavailable
	case errors.Is(err, runner.ErrHandlerNotFound):
		return errCodeHandlerNotFound
	default:
		return errCodeReplayFailed
	}
}

// invokeErrorCode maps a runner manual-invocation error onto a stable wire code.
// The runner's unknown/unavailable sentinels are recognized with errors.Is;
// everything else is a handler failure. It keeps the wire classification
// independent of error prose.
func invokeErrorCode(err error) string {
	switch {
	case errors.Is(err, runner.ErrFunctionNotFound):
		return errCodeUnknownFunction
	case errors.Is(err, runner.ErrFunctionUnavailable):
		return errCodeFunctionUnavailable
	default:
		return errCodeInvokeFailed
	}
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

	// Cancel any in-flight manual invocation so Close's handler wait is bounded
	// by the connection teardown below rather than by invokeTimeout.
	if s.cancel != nil {
		s.cancel()
	}

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

	if err := json.NewEncoder(conn).Encode(socketRequest{Command: cmdRuntimeState, Function: function}); err != nil {
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

// ResetRuntimeStats asks the live worker at path to reset its accumulated Relay
// statistics through the socket's semantic "reset stats" command, so the
// worker's in-memory snapshot source and persisted stats both continue from
// zero without losing the flush race. It is the CLI's running-worker path. A
// missing/unresponsive worker (or one with no stats source) reports
// ErrRuntimeStatsUnavailable, and the CLI falls back to resetting the state
// database directly; a worker that answered but failed the reset reports
// ErrRuntimeStatsFailed so the CLI surfaces it. The worker's Prometheus counters
// are deliberately left monotonic (the worker resets its Relay-side baseline
// only).
func ResetRuntimeStats(path string) error {
	d := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := d.Dial("unix", path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRuntimeStatsUnavailable, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(runtimeStateRequestTimeout))

	if err := json.NewEncoder(conn).Encode(socketRequest{Command: cmdResetStats}); err != nil {
		return fmt.Errorf("%w: %v", ErrRuntimeStatsUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		return fmt.Errorf("%w: %v", ErrRuntimeStatsUnavailable, err)
	}
	if resp.Error != "" {
		if resp.Error == errCodeStatsUnavailable {
			return fmt.Errorf("%w: %s", ErrRuntimeStatsUnavailable, resp.Error)
		}
		return fmt.Errorf("%w: %s", ErrRuntimeStatsFailed, resp.Error)
	}
	if !resp.ResetStats {
		return fmt.Errorf("%w: empty response", ErrRuntimeStatsFailed)
	}
	return nil
}

// InvokeFunction asks the live worker at path to run the named function's
// matching event handlers synchronously against the live runtime pool, and
// returns the number of handlers executed. event must be a JSON object (the
// matcher is defined over a map); the CLI validates it before dialing and the
// worker validates it again on the wire. ctx bounds the CLI side (a cancelled
// ctx, e.g. Ctrl-C, closes the connection so the command aborts promptly rather
// than waiting out the invocation deadline).
//
// It is the CLI's running-worker path for `relay function invoke`; there is no
// offline fallback (a manual invocation must run through the live runner/runtime
// pool, which only the worker owns). A missing/unresponsive worker, or a worker
// whose runner is not wired yet, reports ErrInvokeUnavailable. The worker's
// unknown-function / function-unavailable / handler-failure answers report
// ErrInvokeFailed with the worker's message, so the CLI surfaces the real cause
// (including a failed handler's reason) rather than masking it.
func InvokeFunction(ctx context.Context, path, function string, event json.RawMessage) (int, error) {
	d := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	defer conn.Close()
	// A manual invocation can legitimately run a handler up to the rule timeout
	// (capped at function.MaxTimeout), which can exceed the short query deadline.
	// The CLI must not cut off a live handler mid-flight, so the client deadline
	// is the worker-side invocation bound plus the short exchange margin; the
	// worker enforces its own independent bound regardless.
	_ = conn.SetDeadline(time.Now().Add(invokeTimeout + runtimeStateRequestTimeout))
	// A cancelled ctx (Ctrl-C) closes the connection, unblocking the response
	// read so the command exits promptly instead of waiting out the deadline.
	// The stop clears the hook when the call finishes normally, so no callback
	// outlives this call.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := json.NewEncoder(conn).Encode(socketRequest{
		Command:  cmdInvokeFunction,
		Function: function,
		Event:    event,
	}); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return 0, fmt.Errorf("%w: %v", ErrInvokeUnavailable, ctx.Err())
		}
		return 0, fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	if resp.Error != "" {
		if resp.Error == errCodeInvokeUnavailable {
			return 0, fmt.Errorf("%w: %s", ErrInvokeUnavailable, messageOr(resp.Message, resp.Error))
		}
		return 0, fmt.Errorf("%w: %s", ErrInvokeFailed, messageOr(resp.Message, resp.Error))
	}

	if resp.InvokeResult == nil {
		return 0, fmt.Errorf("%w: empty response", ErrInvokeUnavailable)
	}

	return resp.InvokeResult.Invoked, nil
}

// ReplayDLQ asks the live worker at path to re-execute the exact function and
// handler recorded by a DLQ entry, with event as the replayed payload. It is the
// CLI's running-worker path for `relay dlq replay`; there is no offline fallback
// (a replay must run through the live runner/runtime pool, which only the worker
// owns). event must be the entry's payload bytes, replayed verbatim.
//
// ctx bounds the CLI side (a cancelled ctx, e.g. Ctrl-C, closes the connection
// so the command aborts promptly). A missing/unresponsive worker, or a worker
// whose runner is not wired yet, reports ErrInvokeUnavailable. The worker's
// unknown-function / function-unavailable / handler-not-found / handler-failure
// answers report ErrInvokeFailed with the worker's message, so the CLI surfaces
// the real cause (including a failed handler's reason) rather than masking it.
func ReplayDLQ(ctx context.Context, path, function, handler string, event []byte) error {
	d := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	defer conn.Close()
	// A replay can legitimately run a handler up to the rule timeout, which can
	// exceed the short query deadline, so the client deadline is the worker-side
	// invocation bound plus the short exchange margin. The worker enforces its
	// own bound regardless.
	_ = conn.SetDeadline(time.Now().Add(invokeTimeout + runtimeStateRequestTimeout))
	// A cancelled ctx (Ctrl-C) closes the connection, unblocking the response
	// read so the command exits promptly instead of waiting out the deadline.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := json.NewEncoder(conn).Encode(socketRequest{
		Command:  cmdReplayDLQ,
		Function: function,
		Handler:  handler,
		Event:    event,
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: %v", ErrInvokeUnavailable, ctx.Err())
		}
		return fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	if resp.Error != "" {
		if resp.Error == errCodeInvokeUnavailable {
			return fmt.Errorf("%w: %s", ErrInvokeUnavailable, messageOr(resp.Message, resp.Error))
		}
		return fmt.Errorf("%w: %s", ErrInvokeFailed, messageOr(resp.Message, resp.Error))
	}
	if !resp.Replayed {
		return fmt.Errorf("%w: empty response", ErrInvokeUnavailable)
	}
	return nil
}

// messageOr returns msg when it is non-empty, else fallback. It keeps the wire
// error code as the last-resort message when a worker (an older build, or a
// defensive path) sends no human-readable detail.
func messageOr(msg, fallback string) string {
	if msg != "" {
		return msg
	}
	return fallback
}
