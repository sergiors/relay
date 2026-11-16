package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"relay/internal/app"
)

// executionEndpoints builds the Docker NetworkingConfig EndpointsConfig from
// the given network set: each named network exactly once. Production passes the
// managed internal control network plus the optional NETWORKS application set,
// so empty NETWORKS still yields the control endpoint; an empty/nil list yields
// nil only for a caller that supplies no networks at all (direct construction /
// tests), never for the production execution path. It shares NetworkSet with the
// service path, so both normalize identically.
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

// containerOps is the narrow Manager-owned operation seam threaded into an
// execution container. It carries two things:
//
//   - lifecycle, the manager shutdown context the container's detached exit wait
//     is rooted in, so Manager close cancels the wait request; and
//   - gate, the Manager's operation lifecycle, which every container teardown
//     (a pool-initiated discard, a monitor self-discard) and the exit-wait
//     watcher itself is admitted through, so no Manager-owned execution-container
//     Docker call can run outside the lifecycle CloseContext drains.
//
// A zero value (directly constructed test containers, callers without a manager)
// falls back to context.Background and no gate, preserving the historical
// detached behavior. controlNetwork is REQUIRED and names the internal network
// the execution channel uses for its bootstrap address; it is never substituted
// with an application network.
type containerOps struct {
	lifecycle      context.Context
	gate           *managerOps
	controlNetwork string
}

// opCtx returns the container's operation lifecycle, or context.Background when
// no manager-owned lifecycle was supplied.
func (o containerOps) opCtx() context.Context {
	if o.lifecycle != nil {
		return o.lifecycle
	}
	return context.Background()
}

// admitNormal admits one NORMAL Manager operation for a container's ordinary
// Docker work (a pool- or monitor-initiated teardown, or the exit-wait watcher).
// ok is false when Manager shutdown has closed normal admission: the caller must
// then issue no Docker call and leave the container to the shutdown-owned cache
// teardown phase. With no gate (a directly constructed test container) it
// returns ctx and a no-op release.
func (o containerOps) admitNormal(ctx context.Context) (context.Context, func(), bool) {
	if o.gate == nil {
		return ctx, func() {}, true
	}
	opCtx, release, err := o.gate.begin(ctx)
	if err != nil {
		return nil, nil, false
	}
	return opCtx, release, true
}

// executionContainer is one reused execution container for an app. It holds a
// long-running bootstrap process (python/node) serving the persistent
// execution-channel protocol (see protocol.go): Start creates and starts the
// container, establishes and authenticates ONE TCP connection to its bootstrap,
// and Invoke performs one sequential request/response exchange over that
// connection. All protocol I/O is serialized: the owning Manager's per-app pool
// leases a container to exactly one invocation at a time (distinct invocations of
// the same app run on distinct containers), and ioMu below is the belt-and-braces
// guard inside the container itself.
//
// Discard semantics: the container is discarded (killed, removed, its channel
// closed, and poisoned against reuse) on timeout, process exit, transport loss,
// protocol error, image change, or shutdown. A handler failure (ok:false
// response) is NOT a discard — the container stays healthy for the next
// invocation. On any discard the pool observes (or drops) the container on
// release and starts a fresh one on demand.
type executionContainer struct {
	cli   *client.Client
	log   *slog.Logger
	fn    string
	image string
	// ops is the Manager-owned operation lifecycle this container's detached
	// Docker work (the exit wait request and a self-initiated discard) is rooted
	// in. It replaces a detached context.Background so neither outlives the
	// Manager's Docker client. A zero value is the detached historical behavior.
	ops containerOps
	// meta is the creation-time RunMeta stamped as labels. Identity fields
	// (Type/App/NodeID/Image) are set; per-invocation fields
	// (Handler/MessageID/EventID/EventName) are left EMPTY because container
	// labels are immutable at creation while this container outlives individual
	// invocations — per-invocation attribution moves to the request frame.
	meta RunMeta
	id   string
	conn net.Conn

	// fail carries container-death events for a pending Invoke. The monitor
	// sends only when an invocation is pending; an idle container's death is
	// handled (and discarded) by the monitor itself.
	fail     chan failEvent
	exitInfo chan exitInfo // wait watcher → status/error, cap 1
	connErr  chan error    // channel transport failure from the reader, cap 1
	protoErr chan error    // protocol-violation signal from the reader, cap 1
	closed   chan struct{} // closed exactly once when discarded

	// readerDone is closed when the connection reader goroutine exits. Discard
	// joins it after closing the connection, so a container teardown leaves no
	// live protocol reader behind.
	readerDone chan struct{}

	// dying is the discard CAS: only the first discarder closes/tears down.
	dying atomic.Bool
	// fatal records that the reader or monitor observed a
	// container-death/channel-failure event. An invocation that resolves from an
	// already-buffered result while a failure is being delivered must still
	// report that result (it is unambiguous) but must never return the container
	// to the pool for reuse.
	fatal atomic.Bool
	// removed records that the physical Docker container is CONFIRMED gone
	// (removed, or already absent because AutoRemove/not-found won the race). It
	// is set at most once, only when the first discarder's remove succeeds; a
	// genuine remove failure leaves it false forever, so the container's global
	// warm-budget slot stays conservatively reserved for the Manager's lifetime
	// (see removalConfirmed and discardContainerContext).
	removed  atomic.Bool
	reasonMu sync.Mutex
	reason   string

	ioMu       sync.Mutex // serializes request/response exchanges per container
	invocation int        // invocation ordinal within this container (ioMu-guarded)

	// pendingMu guards the result correlation state below. The single reader
	// goroutine is the only sender; Invoke registers and clears.
	pendingMu sync.Mutex
	pendingID string
	pendingCh chan invokeResponse
	hasPend   bool
	// pendingDelivered records that the first matching result was already
	// accepted for the current pending invocation. It is reset with the pending
	// registration so a later result with the same id is always a protocol
	// violation, even after Invoke consumed the first one from the channel.
	pendingDelivered bool
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

// startExecutionContainer creates, starts, connects, and registers the wait
// watcher for one reused execution container. Create and start failures are plain
// errors (there is no container to discard); after a successful start the
// container cleans itself up via AutoRemove the moment its process exits, or via
// an explicit kill/remove on our discard paths.
//
// ops is the Manager-owned operation lifecycle the container's detached Docker
// work is rooted in: the exit wait request and a self-initiated discard derive
// from it instead of context.Background, so Manager close aborts them rather
// than letting them outlive the Docker client. A zero value preserves the
// detached behavior for callers (tests) without a manager.
//
// networks is the full network set the container joins at create time: the
// internal control network first, then the optional worker-global NETWORKS set
// (application connectivity). ops.controlNetwork is REQUIRED and identifies the
// network the execution channel dials; NETWORKS is never used as an IPC
// fallback. NETWORKS is infrastructure owned OUTSIDE Relay and is validated
// separately by the worker. A network that disappears between verification and
// create surfaces as a create error here.
//
// The execution channel requires a usable address on the INTERNAL CONTROL
// network in the just-started container; an application network is never used
// as an IPC fallback. That is checked from the daemon's inspect data and the
// start FAILS CLOSED when no such address exists. Docker membership alone is not
// proof that this Relay process can route to the address: a containerized Relay
// attaches itself to the control network at startup, while a direct host
// process must have native bridge routing (verified at startup). A dial failure
// also fails the start; Relay never falls back to published ports or
// stdin/stdout.
func startExecutionContainer(
	ctx context.Context,
	ops containerOps,
	cli *client.Client,
	log *slog.Logger,
	fn, image string,
	env []string,
	networks []string,
	limits app.ResourceLimits,
	meta RunMeta,
	workDir string,
	sourceRoot string,
	mounts ...mount.Mount,
) (*executionContainer, error) {
	if ops.controlNetwork == "" {
		return nil, errors.New(
			"docker run: internal control network is not configured")
	}
	hostConfig := hardenedHostConfig(true, limits)
	// A HOT_RELOAD app mounts its live source read-only at the engine workdir
	// (managed by the caller). An empty list is the historical no-mount shape.
	if len(mounts) > 0 {
		hostConfig.Mounts = mounts
	}
	// An invocation container declares no service port, so it can never carry
	// the low-port capability.
	if err := validateContainerHostConfig(
		hostConfig, mounts, 0, sourceRoot,
	); err != nil {
		return nil, fmt.Errorf("docker run: unsafe container configuration: %w", err)
	}
	// Docker materializes an image-declared VOLUME as an anonymous writable
	// mount even under ReadonlyRootfs, so refuse it before create just like an
	// unsafe HostConfig. An image without declared volumes is unchanged.
	createImage, err := validateImageDeclaredVolumes(ctx, cli, image)
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	if err := validateRecursiveReadOnlyMountSupport(cli, mounts); err != nil {
		return nil, fmt.Errorf("docker run: unsafe container configuration: %w", err)
	}
	// The credential is fresh for every container create, so a replacement
	// container can never be authenticated with a previous container's secret.
	credential, err := newExecutionCredential()
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	createOps := client.ContainerCreateOptions{
		Config: &container.Config{
			Image: createImage,
			// The app plan env plus Relay's channel port/credential. The
			// credential is a per-container channel secret, not an invocation
			// secret; it is never logged or framed.
			Env: executionBootstrapEnv(env, credential),
			// workDir is the HOT_RELOAD app root (Node's /app/src) so
			// process.cwd() and relative filesystem operations see the app's
			// source; empty preserves the image WORKDIR (/app) exactly.
			WorkingDir: workDir,
			// The bootstrap communicates over TCP only. stdin/stdout are not
			// attached and carry no protocol traffic; app output is not
			// forwarded by Relay.
			Tty:    false,
			Labels: runLabels(meta),
		},
		// hardenedHostConfig(true, limits) unchanged in its SECURITY baseline:
		// AutoRemove still removes the container the moment it exits (crash
		// recovery is free), and during its idle lifetime it simply stays
		// running. The read-only rootfs, dropped caps, bounded /tmp tmpfs, and
		// non-root user baked into the image are identical to the one-shot
		// containers; only the memory/CPU/pids limits follow the app's
		// effective resource configuration. The optional HOT_RELOAD bind is
		// read-only and adds no writable surface.
		HostConfig: hostConfig,
	}
	if endpoints := executionEndpoints(networks); len(endpoints) > 0 {
		createOps.NetworkingConfig = &network.NetworkingConfig{EndpointsConfig: endpoints}
	}
	createResp, err := cli.ContainerCreate(ctx, createOps)
	if err != nil {
		return nil, fmt.Errorf("docker run: create container: %w", err)
	}
	id := createResp.ID

	removeFailedContainer := func() {
		killContainer(cli, id)
		if err := removeContainer(cli, id); err != nil {
			log.Warn("Runtime container: remove container failed", "container", id, "error", err)
		}
	}

	if _, err := cli.ContainerStart(ctx, id, client.ContainerStartOptions{}); err != nil {
		removeFailedContainer()
		return nil, fmt.Errorf("docker run: start: %w", err)
	}

	c, err := connectExecutionContainer(
		ctx, ops, cli, log, fn, image, id, credential, meta,
	)
	if err != nil {
		removeFailedContainer()
		return nil, err
	}

	c.log.Debug("Runtime container: started",
		"app", c.fn, "image", c.image, "container", c.id)
	return c, nil
}

// connectExecutionContainer discovers the container's control-network address
// candidates, dials and authenticates the first reachable bootstrap, and starts
// the long-lived connection reader, exit watcher, and monitor. Only the
// explicitly configured internal control network is considered; an application
// network is never used as the execution IPC path. Any failure leaves the
// caller to remove the started container; a connection established before a
// later failure is closed here so no resource escapes.
func connectExecutionContainer(
	ctx context.Context,
	ops containerOps,
	cli *client.Client,
	log *slog.Logger,
	fn, image, id, credential string,
	meta RunMeta,
) (*executionContainer, error) {
	if ops.controlNetwork == "" {
		return nil, errors.New(
			"docker run: internal control network is not configured")
	}
	insp, err := cli.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("docker run: inspect execution container: %w", err)
	}
	if insp.Container.State == nil || !insp.Container.State.Running {
		return nil, errors.New("docker run: execution container is not running")
	}
	var settings map[string]*network.EndpointSettings
	if insp.Container.NetworkSettings != nil {
		settings = insp.Container.NetworkSettings.Networks
	}
	addresses, err := executionAddresses(
		[]string{ops.controlNetwork}, settings, executionPort,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"docker run: execution control network %q: %w",
			ops.controlNetwork, err,
		)
	}
	conn, err := connectExecutionChannel(ctx, addresses, credential)
	if err != nil {
		return nil, fmt.Errorf(
			"docker run: execution control network %q is unreachable from this Relay process "+
				"(a containerized Relay must be attached to it; a host process needs native bridge routing): %w",
			ops.controlNetwork, err,
		)
	}
	c := &executionContainer{
		cli:        cli,
		log:        log,
		fn:         fn,
		image:      image,
		ops:        ops,
		meta:       meta,
		id:         id,
		conn:       conn,
		fail:       make(chan failEvent, 4),
		exitInfo:   make(chan exitInfo, 1),
		connErr:    make(chan error, 1),
		protoErr:   make(chan error, 1),
		closed:     make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go c.readResponses()
	// Exit watcher: the container is expected to keep running between
	// invocations, so the wait request lives for the container's whole
	// lifetime and is detached from any single invocation's ctx.
	go c.watchExit()
	// Exit monitor: reacts to process exit and execution-channel failure. An
	// idle container death discards immediately; a death while an Invoke is
	// pending is delivered to that Invoke and discarded there (single owner).
	go c.monitor()
	return c, nil
}

func signalExit(ch chan exitInfo, info exitInfo) {
	select {
	case ch <- info:
	default:
	}
}

// readResponses is the container's single long-lived protocol reader. It reads
// result frames for the whole connection lifetime, delivering each to the
// registered invocation and treating any malformed, unexpected, duplicate, or
// uncorrelated frame as a fatal channel failure. It exits when the connection is
// closed (discard) or the peer closes it, and discard joins it.
func (c *executionContainer) readResponses() {
	defer close(c.readerDone)
	for {
		raw, err := readFrame(c.conn, maxResponseFrame)
		if err != nil {
			c.signalConnErr(err)
			return
		}
		var resp invokeResponse
		if err := json.Unmarshal(raw, &resp); err != nil {
			c.signalProtocolErr(fmt.Errorf("malformed response frame: %w", err))
			return
		}
		if err := c.deliverResponse(resp); err != nil {
			c.signalProtocolErr(err)
			return
		}
	}
}

// signalConnErr reports one channel transport failure to the monitor. The
// channel is buffered; the first failure is the only useful one.
func (c *executionContainer) signalConnErr(err error) {
	c.fatal.Store(true)
	select {
	case c.connErr <- err:
	default:
	}
}

// signalProtocolErr reports a protocol violation (malformed, unexpected,
// duplicate, or uncorrelated frame) to the monitor. It is distinct from a
// transport failure because it needs no process-exit grace period.
func (c *executionContainer) signalProtocolErr(err error) {
	c.fatal.Store(true)
	select {
	case c.protoErr <- err:
	default:
	}
}

// setPending registers the in-flight invocation's result channel. It must be
// called before the request frame is written.
func (c *executionContainer) setPending(id string, ch chan invokeResponse) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pendingID = id
	c.pendingCh = ch
	c.hasPend = true
	c.pendingDelivered = false
}

// clearPending unregisters the in-flight invocation. A result arriving after
// this point is uncorrelated and therefore a protocol error.
func (c *executionContainer) clearPending() {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	c.pendingID = ""
	c.pendingCh = nil
	c.hasPend = false
	c.pendingDelivered = false
}

// pending reports whether an invocation is currently registered. The monitor
// uses pendingAwaitingResult, not this method, to decide whether a failure must
// also be queued for the invocation.
func (c *executionContainer) pending() bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return c.hasPend
}

// pendingAwaitingResult reports whether an invocation is registered and no
// matching result has been delivered yet. When true, a death/failure must wake
// the invocation; when false, an already-buffered result (or the closed channel)
// already resolves it, so queuing a fail would only strand a monitor event.
func (c *executionContainer) pendingAwaitingResult() bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return c.hasPend && !c.pendingDelivered
}

// deliverResponse correlates one result frame with the pending invocation. The
// first matching result is accepted exactly once: pendingDelivered rejects every
// later result for that same pending id even after Invoke consumed the first
// value from the channel, so a duplicate cannot be silently stranded. A
// duplicate or an uncorrelated/wrong-id result is a protocol error, never
// silently dropped.
func (c *executionContainer) deliverResponse(resp invokeResponse) error {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if !c.hasPend {
		return errors.New("result frame with no pending invocation")
	}
	if resp.ID != c.pendingID {
		return errors.New("result frame id does not match the pending invocation")
	}
	if c.pendingDelivered {
		return errors.New("duplicate result frame for the pending invocation")
	}
	c.pendingDelivered = true
	select {
	case c.pendingCh <- resp:
		return nil
	default:
		return errors.New("result frame channel is unexpectedly full")
	}
}

// watchExit runs the container's exit wait request for its whole lifetime. The
// watcher's LIFETIME is itself a NORMAL Manager operation (ops.admitNormal), so
// it is counted and Manager shutdown cancels and joins it (drainNormal) before
// cache teardown begins. If normal admission has already closed, the watcher is
// NOT admitted late and issues no wait request: the container is being torn down
// by cache teardown. The request context is the admitted normal context, so
// Manager close both aborts the request and (via the operation release below) is
// joined by the drain. It delivers the exit status (or a wait error) once; the
// monitor routes it. A bounded give-up guards against a broken daemon that never
// delivers.
func (c *executionContainer) watchExit() {
	waitCtx, release, ok := c.ops.admitNormal(c.ops.opCtx())
	if !ok {
		// Normal admission closed: never admit a watcher late after close. The
		// container's teardown is owned by the cache teardown phase.
		return
	}
	defer release()
	waitCtx, waitCancel := context.WithCancel(waitCtx)
	defer waitCancel()
	wait := c.cli.ContainerWait(waitCtx, c.id, client.ContainerWaitOptions{
		Condition: container.WaitConditionNotRunning,
	})
	// drainWait releases the wait request's reader goroutine: the SDK delivers
	// at most one result/error, and a cancelled request reports through the
	// buffered Error channel, so a bounded drain lets the goroutine exit without
	// blocking on an unread Result. It runs before the operation release, so the
	// drain barrier covers the SDK reader too.
	drainWait := func() {
		select {
		case <-wait.Result:
		case <-wait.Error:
		case <-time.After(5 * time.Second):
		}
	}
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
		// Discarded: cancel so the wait request aborts, then drain it so the
		// client's reader goroutine can exit.
		waitCancel()
		drainWait()
		return
	case <-waitCtx.Done():
		// Manager close (or discard) cancelled the wait request: drain the
		// aborting request so its reader goroutine exits, then return.
		drainWait()
		return
	case <-timer.C:
		return
	}
	// The single wait delivery landed (the SDK reader goroutine has exited); the
	// monitor owns routing from here until the container is discarded OR the
	// manager closes (which cancels waitCtx), whichever comes first. Returning on
	// the cancellation is what lets drainNormal complete before cache teardown.
	select {
	case <-c.closed:
	case <-waitCtx.Done():
	}
}

// monitor funnels container-death events: exit status and execution-channel
// failure. A pending invocation is notified via fail (and performs the discard
// itself); an idle container's death discards directly.
func (c *executionContainer) monitor() {
	for {
		select {
		case <-c.closed:
			return
		case info := <-c.exitInfo:
			c.onDeath("process_exit", exitEvent(info))
		case err := <-c.protoErr:
			c.onDeath("protocol_error", err)
		case err := <-c.connErr:
			// A transport loss can leave the process alive, so it must poison
			// immediately rather than wait out a process-exit grace period. Take
			// an ALREADY-DELIVERED exit status non-blockingly so the discard
			// reason stays process_exit when possible; otherwise the transport
			// failure is the reason.
			select {
			case info := <-c.exitInfo:
				c.onDeath("process_exit", exitEvent(info))
			default:
				c.onDeath("protocol_error", fmt.Errorf("execution channel failed: %w", err))
			}
		}
	}
}

// onDeath routes one container-death event. Teardown is ALWAYS performed here:
// a protocol violation, transport loss, or process exit must physically discard
// the container even when an invocation has just consumed a valid result, or the
// container could remain alive with a queued failure and be reused. A death that
// arrives before any result was delivered also queues a fail for the pending
// invocation; once a result was delivered, respCh (or closed) resolves the
// invocation, so no fail is queued.
func (c *executionContainer) onDeath(reason string, err error) {
	c.fatal.Store(true)
	if c.pendingAwaitingResult() {
		// Buffered send; if the channel is full (a burst of events) the
		// waiting Invoke still sees the discard through closed below.
		select {
		case c.fail <- failEvent{reason: reason, err: err}:
		default:
		}
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
// result channel, writes one request frame to the persistent connection, and
// blocks for the result, the container's death, a channel failure, or ctx
// cancellation. It is called by the pool on a container leased to exactly one
// invocation; ioMu additionally prevents any racing exchange on this container.
//
// Timeout: ctx.Done while in flight closes the connection, kills + removes +
// discards the container (reason "timeout"), and returns the ctx error wrapped
// exactly like the one-shot path ("docker run: <ctx err>"), preserving retry/log
// semantics.
//
// Cancellation must also release the synchronous request write and the long-lived
// reader: both block on the connection, which observes no context. A cancellation
// callback registered at the channel boundary claims the timeout discard and
// closes the conn while this invocation is unresolved; once it completes, the
// callback is suppressed (see settle).
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
	c.setPending(id, respCh)
	defer c.clearPending()

	// The request frame carries the invocation identity, the verbatim event
	// payload, the per-invocation env, the optional W3C trace carrier, and the
	// per-invocation HandlerContext metadata (see encodeInvokeRequest) so a
	// container-side propagator can parent its spans to this invocation and the
	// handler sees the execution identity.
	frame, err := encodeInvokeRequest(ctx, id, handler, eventJSON, env)
	if err != nil {
		// Event JSON is always marshalled by the runner before reaching here;
		// a failure is a bug in the caller's contract, not container state.
		return fmt.Errorf("docker run: marshal request frame: %w", err)
	}
	if len(frame) > maxRequestFrame {
		// Caller-contract failure, not container state: report it without
		// poisoning a healthy container.
		return fmt.Errorf("docker run: request frame of %d bytes exceeds the %d-byte limit",
			len(frame), maxRequestFrame)
	}

	// Cancellation unblocker (see the method comment). The callback claims the
	// timeout teardown BEFORE closing the connection, then closes it to release
	// both a Write blocked sending the request frame and the reader blocked in
	// readFrame, without waiting for the Docker teardown.
	cbDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cbDone)
		c.discardCancelled()
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

	if err := writeFrame(c.conn, frame, maxRequestFrame); err != nil {
		if settle() {
			// The cancellation callback closed the channel under us: report the
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
	case <-ctx.Done():
		outcome = outcomeCancel
	case <-c.closed:
		outcome = outcomeClosed
	}
	if settle() {
		// The cancellation callback started before this invocation resolved:
		// cancellation wins consistently, whichever branch the select happened
		// to pick. The callback has already closed the channel and poisoned the
		// container with reason "timeout". stop() reporting false implies ctx
		// is done, so ctx.Err() is non-nil.
		return fmt.Errorf("docker run: %w", ctx.Err())
	}
	// A valid result may have been delivered just before the reader observed
	// EOF and queued a failure, making both respCh and c.fail ready; Go's
	// select may then have picked the failure. Prefer an ALREADY-BUFFERED
	// result over a failure/closed branch so a confirmed handler outcome is not
	// converted into a retry that repeats side effects. Cancellation remains
	// authoritative (settle above and outcomeCancel below), and a response that
	// is not already buffered at this point remains ambiguous and fails.
	if outcome != outcomeResponse && outcome != outcomeCancel {
		select {
		case resp = <-respCh:
			outcome = outcomeResponse
		default:
		}
	}
	// Unregister the result channel before acting on the outcome. An
	// already-delivered result may be reported unambiguously, but a death or
	// channel failure that raced the select must not be left queued against a
	// still-"pending" invocation; clearing here also lets a monitor that has not
	// yet inspected pending state take the idle discard path itself. The
	// deferred clear remains for every earlier return.
	c.clearPending()
	switch outcome {
	case outcomeResponse:
		if c.fatal.Load() {
			// A death/channel failure was observed while this result was in
			// flight. The result is unambiguous, so report it, but the container
			// itself is not reusable.
			c.discard("protocol_error")
		} else if !first {
			// An actual reuse: this container already served an invocation and
			// just served another one. DEBUG only — never noisy INFO.
			c.log.Debug("Runtime container: reused",
				"app", c.fn, "image", c.image, "container", c.id)
		}
		if resp.OK {
			return nil
		}
		// Handler failure: the PROCESS is healthy; the container is kept.
		// The error string is bounded (see clampResponseError).
		return fmt.Errorf("handler %q failed: %s", handler, clampResponseError(resp.Error))
	case outcomeFail:
		c.discard(ev.reason)
		return fmt.Errorf("docker run: %s", ev.err)
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

// discard kills, removes, closes the execution channel, and poisons the
// container against reuse. It is idempotent: only the first caller tears down
// (closing closed) and logs; race losers are no-ops. Removal is idempotent w.r.t.
// AutoRemove having already deleted the container (benign not-found/conflict). A
// genuine removal failure is logged Warn (a discard that could not remove
// anything is worth surfacing).
//
// It admits the teardown as a NORMAL Manager operation first, so a
// self-initiated teardown (a death observed while idle, a protocol error) is
// ordinary work. If normal admission has already closed, it issues NO Docker
// call and reports false: the container is still owned by its pool, so the
// shutdown-owned cache teardown phase cleans it. The per-call containerOpTimeout
// still applies on top.
func (c *executionContainer) discard(reason string) bool {
	opCtx, release, ok := c.ops.admitNormal(c.ops.opCtx())
	if !ok {
		return false
	}
	defer release()
	return c.discardContext(opCtx, reason)
}

// discardCancelled is the invocation-cancellation teardown. It CLAIMS the
// timeout teardown before closing the connection: closing the connection wakes
// the protocol reader, whose transport failure the monitor reports as
// protocol_error, so a claim taken after the close could lose the discard race
// and label a known timeout as a protocol error. The claim is authoritative for
// the reason (see claimDiscard), and the close right after it releases the
// blocked protocol write/read ahead of the Docker teardown.
//
// It admits the teardown as a NORMAL Manager operation, exactly like discard. If
// normal admission has already closed, no Docker call is issued and the blocked
// I/O is simply released: the container stays owned by its pool for the
// shutdown-owned cache teardown phase.
func (c *executionContainer) discardCancelled() {
	opCtx, release, ok := c.ops.admitNormal(c.ops.opCtx())
	if !ok {
		_ = c.conn.Close()
		return
	}
	defer release()
	claimed := c.claimDiscard(reasonTimeout)
	// Closing is safe either way: a container whose teardown is claimed (by this
	// callback or by a genuine death path) is never reused.
	_ = c.conn.Close()
	if claimed {
		c.teardown(opCtx, reasonTimeout)
	}
}

// discardContext runs the teardown on ctx. It performs NO admission: the caller
// owns that. A pool-initiated discard passes the Manager operation it reserved
// BEFORE removing the container from the pool (or the cache-teardown phase's
// context for shutdown teardown); the monitor's discard passes the normal
// operation it admitted in discard. The kill and remove observe ctx, so a
// shutdown teardown stops waiting promptly when its bound expires instead of
// running on the container's own detached 5s-per-call context. The
// CAS/reason/closed bookkeeping is idempotent.
//
// The return value reports whether THIS call performed the teardown; a race
// loser (an earlier discard already set dying) returns false without touching
// the container. `removed` is set only when removeContainerContext reports
// success (or the container was already gone, see benignRemovalErr), so the pool
// can tell a torn-down-and-gone container from one whose physical removal failed
// and keep the latter's global warm-budget slot reserved.
func (c *executionContainer) discardContext(ctx context.Context, reason string) bool {
	if !c.claimDiscard(reason) {
		return false
	}
	return c.teardown(ctx, reason)
}

// claimDiscard atomically claims this container's single teardown and records
// its reason. Only the CAS winner owns the teardown (see teardown); it records
// the reason BEFORE any connection close, so a teardown that will wake the
// protocol reader (and thus the monitor) cannot have its reason overwritten by
// the resulting transport failure. Race losers change nothing, keeping the
// first cause authoritative for the pool's discard metric.
func (c *executionContainer) claimDiscard(reason string) bool {
	if !c.dying.CompareAndSwap(false, true) {
		return false
	}
	c.reasonMu.Lock()
	c.reason = reason
	c.reasonMu.Unlock()
	return true
}

// teardown performs an already-claimed teardown: kill, remove, close the
// execution channel, publish the discard, and join the protocol reader. Only a
// claimDiscard winner may call it.
func (c *executionContainer) teardown(ctx context.Context, reason string) bool {
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
	if c.conn != nil {
		_ = c.conn.Close()
	}
	close(c.closed)
	if c.readerDone != nil {
		<-c.readerDone
	}
	c.log.Debug("Runtime container: discarded",
		"app", c.fn, "image", c.image, "container", c.id, "reason", reason)
	return true
}

// dead reports whether the container has been discarded or poisoned by a
// death/channel-failure event. A fatal event is recorded by the reader/monitor
// before teardown completes, so the pool can never lease a container whose
// connection has failed even if the invocation is still unwinding.
func (c *executionContainer) dead() bool { return c.dying.Load() || c.fatal.Load() }

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
