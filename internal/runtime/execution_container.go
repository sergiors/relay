package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"relay/internal/app"
)

// executionEndpoints builds the Docker NetworkingConfig EndpointsConfig for an
// execution (event/schedule) container from the worker-global network set
// (WithNetworks): each named network exactly once. An empty/nil list yields
// nil, so a worker with no NETWORKS sends no NetworkingConfig (unchanged
// default bridge/network behavior). It shares NetworkSet with the service path,
// so both normalize identically.
func executionEndpoints(networks []string) map[string]*network.EndpointSettings {
	set := NetworkSet(networks)
	if len(set) == 0 {
		return nil
	}
	endpoints := make(map[string]*network.EndpointSettings, len(set))
	for _, n := range set {
		endpoints[n] = &network.EndpointSettings{}
	}
	return endpoints
}

// executionContainer is one reused execution container for an app. It holds
// a long-running bootstrap process (python/node) speaking the line-JSON
// invocation protocol (see protocol.go): Start creates and starts the container
// and its output demultiplexers, Invoke performs one sequential
// request/response exchange. All protocol I/O is serialized: the owning
// Manager's per-app pool leases a container to exactly one invocation at a
// time (distinct invocations of the same app run on distinct containers),
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
	// (Type/App/Hostname/Image) are set; per-invocation fields
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
	dying atomic.Bool
	// removed records that the physical Docker container is CONFIRMED gone
	// (removed, or already absent because AutoRemove/not-found won the race). It
	// is set at most once, only when the first discarder's remove succeeds; a
	// genuine remove failure leaves it false forever, so the container's global
	// warm-budget slot stays conservatively reserved for the Manager's lifetime
	// (see removalConfirmed and discardContainerContext).
	removed  atomic.Bool
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
//
// networks is the worker-global network set (NETWORKS): every execution
// container joins them at create time so it can reach (and be reached on) those
// Docker networks. The networks are infrastructure owned OUTSIDE Relay — Relay
// never creates them — and the worker verifies they exist at startup (see
// Manager.VerifyNetworks); a network that disappears between verification and
// create surfaces as a create error here.
func startExecutionContainer(
	ctx context.Context,
	cli *client.Client,
	log *slog.Logger,
	fn, image string,
	env []string,
	networks []string,
	limits app.ResourceLimits,
	meta RunMeta,
) (*executionContainer, error) {
	createOps := client.ContainerCreateOptions{
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
		// hardenedHostConfig(true, limits) unchanged in its SECURITY baseline:
		// AutoRemove still removes the container the moment it exits (crash
		// recovery is free), and during its idle lifetime it simply stays
		// running. The read-only rootfs, dropped caps, bounded /tmp tmpfs, and
		// non-root user baked into the image are identical to the one-shot
		// containers; only the memory/CPU/pids limits follow the app's
		// effective resource configuration.
		HostConfig: hardenedHostConfig(true, limits),
	}
	if endpoints := executionEndpoints(networks); len(endpoints) > 0 {
		createOps.NetworkingConfig = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}
	createResp, err := cli.ContainerCreate(ctx, createOps)
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
	readerDone := c.startOutputReader()

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
				info.err = errors.New(res.Error.Message)
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
		"app", c.fn, "image", c.image, "container", c.id)
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

// startOutputReader launches the container's long-lived stdout/stderr
// demultiplexer goroutine and returns a channel closed when it exits. Reading
// runs concurrently with request writes so a chatty container cannot dead-lock
// on a full socket; the goroutine exits when the attach stream reaches EOF or
// the hijacked conn is closed (the cancellation path), so it is the single
// long-lived reader for the container's whole lifetime.
func (c *executionContainer) startOutputReader() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = stdcopy.StdCopy(c.demux, &stderrSink{d: c.demux}, c.attach.Reader)
		// EOF on stdout: flush any trailing partial (user) line, then signal
		// EOF so the exit monitor (or a pending Invoke) reacts immediately
		// rather than waiting for the wait-result delivery.
		c.demux.flushPending()
		c.demux.flushForwarders()
		signal(c.eof)
	}()
	return done
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
//
// Cancellation must also release the SYNCHRONOUS request write and the
// long-lived demux reader: both block on the attach's shared hijacked
// net.Conn, which observes no context, so a ctx.Done select branch alone is
// unreachable while the write is stuck. A cancellation callback registered at
// the attach boundary closes that conn — the SDK-supported lifecycle action
// that releases both operations — but only while this invocation is
// unresolved; once it completes, the callback is suppressed (see settle).
func (c *executionContainer) Invoke(
	ctx context.Context,
	handler string, eventJSON []byte,
	env map[string]string,
) error {
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

	// The request frame carries the invocation identity, the verbatim event
	// payload, the per-invocation env, and the optional W3C trace carrier
	// (see encodeInvokeRequest) so a container-side propagator can parent its
	// spans to this invocation.
	frame, err := encodeInvokeRequest(ctx, id, handler, eventJSON, env)
	if err != nil {
		// Event JSON is always marshalled by the runner before reaching here;
		// a failure is a bug in the caller's contract, not container state.
		return fmt.Errorf("docker run: marshal request frame: %w", err)
	}

	// Cancellation unblocker (see the method comment). The callback closes the
	// shared hijacked conn FIRST so a Write blocked sending the request frame
	// and the demux reader blocked in stdcopy.StdCopy are both released
	// promptly, then poisons the container with the timeout semantics. Its own
	// attach.Close in discard is a harmless second close.
	cbDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cbDone)
		c.attach.Close()
		c.discard("timeout")
	})
	// settle unregisters the cancellation callback exactly once. It reports
	// whether cancellation won: false when the callback was suppressed (stop
	// returned true, so it never has and never will run), true when the callback
	// had already started, in which case settle waits for it to finish — no
	// untracked callback may outlive this invocation and close a conn that a
	// later invocation has been handed. The deferred call joins the callback on
	// any abnormal exit.
	settled := false
	settle := func() bool {
		if settled {
			return false
		}
		settled = true
		if stop() {
			return false
		}
		<-cbDone
		return true
	}
	defer settle()

	if _, err := c.attach.Conn.Write(append(frame, '\n')); err != nil {
		if settle() {
			// The cancellation callback closed the attach under us: report the
			// invocation as the timeout with the wrapped ctx error, never a
			// write failure — the close surfaces here as an I/O error.
			return fmt.Errorf("docker run: %w", ctx.Err())
		}
		// The conn is broken (process died, daemon hiccup): the container is
		// unusable. Kill + remove (idempotent) and discard.
		c.discard("protocol_error")
		return fmt.Errorf("docker run: write request frame: %w", err)
	}

	const (
		outcomeResponse = iota
		outcomeFail
		outcomeProtoErr
		outcomeCancel
		outcomeClosed
	)
	var (
		resp    invokeResponse
		ev      failEvent
		outcome int
	)
	select {
	case resp = <-respCh:
		outcome = outcomeResponse
	case ev = <-c.fail:
		outcome = outcomeFail
	case <-c.protoErr:
		outcome = outcomeProtoErr
	case <-ctx.Done():
		outcome = outcomeCancel
	case <-c.closed:
		outcome = outcomeClosed
	}
	if settle() {
		// The cancellation callback started before this invocation resolved:
		// cancellation wins consistently, whichever branch the select happened
		// to pick. The callback has already closed the attach and poisoned the
		// container with reason "timeout". stop() reporting false implies ctx
		// is done, so ctx.Err() is non-nil.
		return fmt.Errorf("docker run: %w", ctx.Err())
	}
	switch outcome {
	case outcomeResponse:
		if !first {
			// An actual reuse: this container already served an invocation and
			// just served another one. DEBUG only — never noisy INFO.
			c.log.Debug("Runtime container: reused",
				"app", c.fn, "image", c.image, "container", c.id)
		}
		if resp.OK {
			return nil
		}
		// Handler failure: the PROCESS is healthy; the container is kept.
		// The error string is bounded (see clampResponseError) so the frame can
		// never exceed the demuxer's line cap.
		return fmt.Errorf("handler %q failed: %s", handler, clampResponseError(resp.Error))
	case outcomeFail:
		c.discard(ev.reason)
		return fmt.Errorf("docker run: %s", ev.err)
	case outcomeProtoErr:
		c.discard("protocol_error")
		return fmt.Errorf("docker run: unexpected protocol response from container")
	case outcomeCancel:
		// Defensive: a ctx.Done selection normally means the callback already
		// ran and settle reported true above. If the callback was suppressed in
		// the settle/ctx race, the invocation is still unresolved, so discard
		// with the same timeout semantics here.
		c.discard("timeout")
		return fmt.Errorf("docker run: %w", ctx.Err())
	default: // outcomeClosed
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
// It runs on detached, bounded Docker calls.
func (c *executionContainer) discard(reason string) bool {
	return c.discardContext(context.Background(), reason)
}

// discardContext is discard with a caller-supplied bound (the pool's shutdown
// context). The kill and remove observe ctx, so a shutdown teardown stops
// waiting promptly when the shutdown step's bound expires instead of running on
// the container's own detached 5s-per-call context. The CAS/reason/closed
// bookkeeping is shared with discard and idempotent.
//
// The return value reports whether THIS call performed the teardown; race losers
// (an earlier discard already set dying) return false without touching the
// container. `removed` is set only when removeContainerContext reports success
// (or the container was already gone, see benignRemovalErr), so the pool can tell
// a torn-down-and-gone container from one whose physical removal failed and keep
// the latter's global warm-budget slot reserved.
func (c *executionContainer) discardContext(ctx context.Context, reason string) bool {
	if !c.dying.CompareAndSwap(false, true) {
		return false
	}
	c.reasonMu.Lock()
	c.reason = reason
	c.reasonMu.Unlock()
	killContainerContext(ctx, c.cli, c.id)
	if err := removeContainerContext(ctx, c.cli, c.id); err != nil {
		// A genuine removal failure: the container may survive as an orphan.
		// `removed` stays false so its warm-budget slot is never returned for the
		// Manager's lifetime (see removalConfirmed / discardContainerContext).
		c.log.Warn("Runtime container: remove container failed",
			"container", c.id, "reason", reason, "error", err)
	} else {
		c.removed.Store(true)
	}
	c.attach.Close()
	close(c.closed)
	c.log.Debug("Runtime container: discarded",
		"app", c.fn, "image", c.image, "container", c.id, "reason", reason)
	return true
}

// dead reports whether the container has been discarded (poisoned).
func (c *executionContainer) dead() bool { return c.dying.Load() }

// removalConfirmed reports whether the physical Docker container is CONFIRMED
// gone: removeContainerContext succeeded (removed, or already absent because
// AutoRemove/not-found won the race). It is false when a genuine removal failure
// left the container possibly alive, so the pool never returns that container's
// warm-budget slot and a fresh container is never admitted on phantom capacity.
func (c *executionContainer) removalConfirmed() bool { return c.removed.Load() }

// removalDone is closed once the first discarder has recorded its removal outcome
// (see discardContext). A pool reaping a container that died by its own path uses
// it to wait out a still-in-flight teardown before deciding whether the physical
// container is gone, rather than observing dead() and permanently withholding the
// warm-budget slot.
func (c *executionContainer) removalDone() <-chan struct{} { return c.closed }

func (c *executionContainer) discardReason() string {
	c.reasonMu.Lock()
	defer c.reasonMu.Unlock()
	return c.reason
}
