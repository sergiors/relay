package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// executionContainer is one reused execution container for a function. It holds
// a long-running bootstrap process (python/node) speaking the line-JSON
// invocation protocol (see protocol.go): Start creates and starts the container
// and its output demultiplexers, Invoke performs one sequential
// request/response exchange. All protocol I/O is serialized: the owning
// Manager's per-function pool leases a container to exactly one invocation at a
// time (distinct invocations of the same function run on distinct containers),
// and ioMu below is the belt-and-braces guard inside the container itself.
//
// Discard semantics: the container is discarded (killed, removed, and poisoned
// against reuse) on timeout, process exit, protocol error, image change, or
// shutdown. A handler failure (ok:false response) is NOT a discard — the
// container stays healthy for the next invocation. On any discard the pool
// observes (or drops) the container on release and starts a fresh one on demand.
type executionContainer struct {
	cli   *client.Client
	log   *slog.Logger
	fn    string
	image string
	// meta is the creation-time RunMeta stamped as labels. Identity fields
	// (Type/Function/Hostname/Image) are set; per-invocation fields
	// (Handler/MessageID/EventID/EventName) are left EMPTY because
	// container labels are immutable at creation while this container
	// outlives individual invocations — per-invocation attribution moves to
	// the per-invocation output prefix and the request frame instead.
	meta   RunMeta
	id     string
	attach *client.ContainerAttachResult

	demux *protocolDemuxer

	// fail carries container-death events for a pending Invoke. The exit
	// monitor sends only when an invocation is pending; an idle container's
	// death is handled (and discarded) by the monitor itself.
	fail     chan failEvent
	exitInfo chan exitInfo // wait watcher → status/error, cap 1
	eof      chan struct{} // stdout EOF signal from the reader goroutine
	protoErr chan struct{} // unexpected protocol frame signal, cap 1
	closed   chan struct{} // closed exactly once when discarded

	// dying is the discard CAS: only the first discarder closes/tears down.
	dying    atomic.Bool
	reasonMu sync.Mutex
	reason   string

	ioMu       sync.Mutex // serializes request writes per container
	invocation int        // invocation ordinal within this container (ioMu-guarded)
}

type exitInfo struct {
	code int64
	err  error
}

// failEvent is a container-side failure delivered to a pending Invoke:
// reason is one of the discard reasons ("timeout", "process_exit",
// "protocol_error"); err is the invocation error to surface to the runner.
type failEvent struct {
	reason string
	err    error
}

// startExecutionContainer creates, attaches, demultiplexes, starts, and
// registers the wait watcher for one reused execution container. Create and
// start failures are plain errors (there is no container to discard); after a
// successful start the container cleans itself up via AutoRemove the moment
// its process exits, or via an explicit kill/remove on our discard paths.
func startExecutionContainer(
	ctx context.Context,
	cli *client.Client,
	log *slog.Logger,
	fn, image string,
	env []string,
	meta RunMeta,
) (*executionContainer, error) {
	createResp, err := cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config: &container.Config{
			Image: image,
			Env:   env,
			// OpenStdin stays true; StdinOnce must be FALSE — the container is
			// reused across invocations, so stdin must stay open for its
			// lifetime and must not be auto-closed after the daemon sees one
			// write+session-end.
			OpenStdin:    true,
			StdinOnce:    false,
			AttachStdin:  true,
			AttachStdout: true,
			AttachStderr: true,
			Tty:          false,
			Labels:       runLabels(meta),
		},
		// hardenedHostConfig(true) unchanged: AutoRemove still removes the
		// container the moment it exits (crash recovery is free), and during
		// its idle lifetime it simply stays running. The hardening baseline
		// (read-only rootfs, dropped caps, resource limits, bounded /tmp
		// tmpfs, non-root user baked into the image) is identical to the
		// one-shot containers.
		HostConfig: hardenedHostConfig(true),
	})
	if err != nil {
		return nil, fmt.Errorf("docker run: create container: %w", err)
	}
	id := createResp.ID

	// Attach once before start; the hijacked conn is kept for the container's
	// LIFETIME (requests are written to it per invocation). CloseWrite is
	// NEVER called while healthy: EOF on stdin is reserved for process death
	// semantics.
	attach, err := cli.ContainerAttach(ctx, id, client.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		// Created but never started: AutoRemove can never fire, so remove it
		// before returning.
		if err := removeContainer(cli, id); err != nil {
			log.Warn("Runtime container: remove container failed", "container", id, "error", err)
		}
		return nil, fmt.Errorf("docker run: attach: %w", err)
	}

	c := &executionContainer{
		cli:      cli,
		log:      log,
		fn:       fn,
		image:    image,
		meta:     meta,
		id:       id,
		attach:   &attach,
		demux:    newProtocolDemuxer(fn),
		fail:     make(chan failEvent, 4),
		exitInfo: make(chan exitInfo, 1),
		eof:      make(chan struct{}, 1),
		protoErr: make(chan struct{}, 1),
		closed:   make(chan struct{}),
	}
	// Unexpected parseable protocol responses (no matching pending id) are
	// protocol violations: signal, never silently multiplex.
	c.demux.protoFail = func() { signal(c.protoErr) }

	// Long-lived reader goroutine: demultiplex the attach stream for the
	// container's whole lifetime. Reading runs concurrently so a chatty
	// container cannot dead-lock on a full socket while a request is written.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_, _ = stdcopy.StdCopy(c.demux, &stderrSink{d: c.demux}, attach.Reader)
		// EOF on stdout: flush any trailing partial (user) line, then signal
		// EOF so the exit monitor (or a pending Invoke) reacts immediately
		// rather than waiting for the wait-result delivery.
		c.demux.flushPending()
		c.demux.flushForwarders()
		signal(c.eof)
	}()

	// Start. The not-running wait below is only meaningful once running.
	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		killContainer(cli, id)
		if err := removeContainer(cli, id); err != nil {
			log.Warn("Runtime container: remove container failed", "container", id, "error", err)
		}
		attach.Close()
		<-readerDone
		return nil, fmt.Errorf("docker run: start: %w", err)
	}

	// Exit watcher: the container is expected to keep running between
	// invocations, so the wait request lives for the container's whole
	// lifetime and is detached from any single invocation's ctx. It delivers
	// the exit status (or a wait error) once; the monitor routes it. A bounded
	// give-up guards against a broken daemon that never delivers.
	go func() {
		wait := cli.ContainerWait(context.Background(), id, client.ContainerWaitOptions{
			Condition: container.WaitConditionNotRunning,
		})
		timer := time.NewTimer(24 * time.Hour)
		defer timer.Stop()
		select {
		case res := <-wait.Result:
			info := exitInfo{code: res.StatusCode}
			if res.Error != nil {
				info.err = fmt.Errorf("%s", res.Error.Message)
			}
			signalExit(c.exitInfo, info)
		case err := <-wait.Error:
			signalExit(c.exitInfo, exitInfo{err: err})
		case <-c.closed:
			// Discarded: kill makes the daemon exit the container shortly, so
			// the wait delivery lands; drain it so the client's request
			// goroutine can exit, bounded like drainWait.
			select {
			case <-wait.Result:
			case <-wait.Error:
			case <-time.After(5 * time.Second):
			}
			return
		case <-timer.C:
			return
		}
		<-c.closed // drain: the monitor owns routing from here
		select {
		case <-wait.Result:
		case <-wait.Error:
		case <-time.After(5 * time.Second):
		}
	}()

	// Exit monitor: reacts to process exit, stdout EOF, and unexpected
	// protocol frames. An idle container death discards immediately; a death
	// while an Invoke is pending is delivered to that Invoke and discarded
	// there (single owner).
	go c.monitor()

	c.log.Debug("Runtime container: started",
		"function", c.fn, "image", c.image, "container", c.id)
	return c, nil
}

// signal delivers a single struct{} signal, non-blocking (the channel is
// buffered, cap 1; repeated signals are not meaningful).
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func signalExit(ch chan exitInfo, info exitInfo) {
	select {
	case ch <- info:
	default:
	}
}

// monitor funnels container-death events: exit status, stdout EOF, unexpected
// protocol frames. A pending invocation is notified via fail (and performs the
// discard itself); an idle container's death discards directly.
func (c *executionContainer) monitor() {
	for {
		select {
		case <-c.closed:
			return
		case info := <-c.exitInfo:
			c.onDeath("process_exit", exitEvent(info))
		case <-c.eof:
			// stdout ended: the process is gone or about to be reported. Give
			// the wait delivery a short grace period so the reason is
			// process_exit (with the exact status) rather than a vaguer
			// protocol error.
			var info exitInfo
			select {
			case info = <-c.exitInfo:
				c.onDeath("process_exit", exitEvent(info))
			case <-time.After(3 * time.Second):
				c.onDeath("protocol_error", fmt.Errorf("container stdout ended before a response"))
			}
		case <-c.protoErr:
			c.onDeath("protocol_error", fmt.Errorf("unexpected protocol response from container"))
		}
	}
}

// onDeath routes one container-death event: to the pending Invoke (which
// discards), or — when idle — to a direct discard.
func (c *executionContainer) onDeath(reason string, err error) {
	if c.demux.pending() {
		// Buffered send; if the channel is full (a burst of events) the
		// waiting Invoke still sees the discard through closed below.
		select {
		case c.fail <- failEvent{reason: reason, err: err}:
		default:
		}
		return
	}
	c.discard(reason)
}

// exitEvent wraps an exitInfo as the monitor event error.
func exitEvent(info exitInfo) error {
	if info.err != nil {
		return info.err
	}
	return fmt.Errorf("container exited with status %d", info.code)
}

// Invoke performs one invocation against the reuse container: registers the
// response channel, writes one request frame line to stdin, and blocks for the
// response, the container's death, the unexpected-protocol signal, or ctx
// cancellation. It is called by the pool on a container leased to exactly one
// invocation; ioMu additionally prevents any racing writer on this container.
//
// Timeout: ctx.Done while in flight kills + removes + discards the container
// (reason "timeout") and returns the ctx error wrapped exactly like the
// one-shot path ("docker run: <ctx err>"), preserving retry/log semantics.
func (c *executionContainer) Invoke(ctx context.Context, handler string, eventJSON []byte, env map[string]string) error {
	c.ioMu.Lock()
	defer c.ioMu.Unlock()

	c.invocation++
	first := c.invocation == 1

	if ctx.Err() != nil {
		// Pre-cancelled/deadline-expired before acquiring the serialized
		// protocol slot: this call never sent a request, so the container is
		// NOT ours to discard. Report cancellation like the one-shot path.
		return fmt.Errorf("docker run: %w", ctx.Err())
	}

	id := newRequestID()
	respCh := make(chan invokeResponse, 1)
	// The in-flight output prefix must carry THIS invocation's handler even for
	// direct callers that injected no RunMeta (the old one-shot path took the
	// prefix handler from the Execute argument). Fall back to the handler
	// argument when the ctx meta leaves it empty; the idle fallback below the
	// demuxer keeps "[fn/]" for asynchronous output between invocations.
	meta := RunMetaFrom(ctx)
	if meta.Handler == "" {
		meta.Handler = handler
	}
	c.demux.begin(meta)
	defer c.demux.end()
	c.demux.setPending(id, respCh)
	defer c.demux.clearPending()

	frame, err := json.Marshal(invokeRequest{
		ID:      id,
		Handler: handler,
		Event:   json.RawMessage(eventJSON),
		Env:     env,
	})
	if err != nil {
		// Event JSON is always marshalled by the runner before reaching here;
		// a failure is a bug in the caller's contract, not container state.
		return fmt.Errorf("docker run: marshal request frame: %w", err)
	}
	if _, err := c.attach.Conn.Write(append(frame, '\n')); err != nil {
		// The conn is broken (process died, daemon hiccup): the container is
		// unusable. Kill + remove (idempotent) and discard.
		c.discard("protocol_error")
		return fmt.Errorf("docker run: write request frame: %w", err)
	}

	select {
	case resp := <-respCh:
		if !first {
			// An actual reuse: this container already served an invocation and
			// just served another one. DEBUG only — never noisy INFO.
			c.log.Debug("Runtime container: reused",
				"function", c.fn, "image", c.image, "container", c.id)
		}
		if resp.OK {
			return nil
		}
		// Handler failure: the PROCESS is healthy; the container is kept.
		// The error string is bounded (see clampResponseError) so the frame can
		// never exceed the demuxer's line cap.
		return fmt.Errorf("handler %q failed: %s", handler, clampResponseError(resp.Error))
	case ev := <-c.fail:
		c.discard(ev.reason)
		return fmt.Errorf("docker run: %s", ev.err)
	case <-c.protoErr:
		c.discard("protocol_error")
		return fmt.Errorf("docker run: unexpected protocol response from container")
	case <-ctx.Done():
		c.discard("timeout")
		return fmt.Errorf("docker run: %w", ctx.Err())
	case <-c.closed:
		return fmt.Errorf("docker run: container discarded (%s)", c.discardReason())
	}
}

// newRequestID returns 8 random hex chars (8–16 per the protocol). random ids
// disambiguate frames from a container's earlier life even if a caller
// pipelines.
func newRequestID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure would be process-wide catastrophe; fall back to
		// a time-derived id (uniqueness still holds via the per-container
		// serialization: responses are matched 1:1);
		return fmt.Sprintf("%x", time.Now().UnixNano())[:16]
	}
	return hex.EncodeToString(b[:])
}

// discard kills, removes, and poisons the container against reuse. It is
// idempotent: only the first caller tears down (closing closed) and logs; race
// losers are no-ops. Removal is idempotent w.r.t. AutoRemove having already
// deleted the container (benign not-found/conflict). A genuine removal failure
// is logged Warn (a discard that could not remove anything is worth surfacing).
func (c *executionContainer) discard(reason string) bool {
	if !c.dying.CompareAndSwap(false, true) {
		return false
	}
	c.reasonMu.Lock()
	c.reason = reason
	c.reasonMu.Unlock()
	killContainer(c.cli, c.id)
	if err := removeContainer(c.cli, c.id); err != nil {
		c.log.Warn("Runtime container: remove container failed",
			"container", c.id, "reason", reason, "error", err)
	}
	c.attach.Close()
	close(c.closed)
	c.log.Debug("Runtime container: discarded",
		"function", c.fn, "image", c.image, "container", c.id, "reason", reason)
	return true
}

// dead reports whether the container has been discarded (poisoned).
func (c *executionContainer) dead() bool { return c.dying.Load() }

func (c *executionContainer) discardReason() string {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return c.reason
}
