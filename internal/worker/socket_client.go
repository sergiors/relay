package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"syscall"
	"time"
)

// This file is the CLI client side of the worker control socket: it dials the
// worker, performs one bounded request/response exchange per operation, and maps
// wire error codes back onto the shared sentinel errors.

const (
	// runtimeStateDialTimeout bounds the CLI's dial to the worker socket, so a
	// missing/unresponsive worker cannot stall `relay app inspect`.
	runtimeStateDialTimeout = 1 * time.Second
)

// QueryRuntimeState dials the live worker query socket at path and asks for
// app's live pool gauges. It is the CLI's whole client surface: callers
// treat ErrRuntimeStateUnavailable and ErrUnknownApp (detected with
// errors.Is) as "live fields unknown" and render the persisted counters.
//
// It is deliberately in internal/worker (which the CLI already imports for
// `relay start`) and returns only the wire value type, so there is no package
// cycle and the CLI never holds a worker or manager.
func QueryRuntimeState(path, name string) (RuntimeState, error) {
	dialer := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := dialer.Dial("unix", path)
	if err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(runtimeStateRequestTimeout))

	if err := json.NewEncoder(conn).Encode(socketRequest{Command: cmdRuntimeState, App: name}); err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		return RuntimeState{}, fmt.Errorf("%w: %v", ErrRuntimeStateUnavailable, err)
	}
	if resp.Error != "" {
		if resp.Error == errCodeUnknownApp {
			return RuntimeState{}, fmt.Errorf("%w: %s", ErrUnknownApp, name)
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
// zero without losing the flush race. It is the CLI's running-worker path.
//
// Only a POSITIVELY absent worker permits the CLI's direct state-DB fallback:
// a refused dial (no socket / nothing listening) reports ErrRuntimeStatsNoWorker.
// Every ambiguous outcome — a dial timeout, a write failure, a missing or
// malformed response — reports ErrRuntimeStatsUnavailable, because a live
// worker may have reset or may do so after the CLI gives up, and the live
// worker must stay the authority; the CLI does not fall back on this error. A
// worker that answered but failed the reset reports ErrRuntimeStatsFailed so the
// CLI surfaces it. The worker's Prometheus counters are deliberately left
// monotonic (the worker resets its Relay-side baseline only).
func ResetRuntimeStats(path string) error {
	dialer := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := dialer.Dial("unix", path)
	if err != nil {
		if socketPositivelyAbsent(err) {
			return fmt.Errorf("%w: %v", ErrRuntimeStatsNoWorker, err)
		}
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

// socketPositivelyAbsent reports whether a dial failed because there is
// definitively no worker listening, as opposed to an ambiguous failure.
// The unambiguous cases are:
//
//   - the socket file does not exist (a stopped worker removed it on shutdown),
//     reported as fs.ErrNotExist; or
//   - nothing is listening on an existing socket, reported as
//     syscall.ECONNREFUSED (a stale file left by a SIGKILLed worker).
//
// A timeout or any other error is NOT positively absent: a live worker may be
// slow to accept, so the caller must treat the outcome as unknown.
func socketPositivelyAbsent(dialErr error) bool {
	return errors.Is(dialErr, fs.ErrNotExist) || errors.Is(dialErr, syscall.ECONNREFUSED)
}

// InvokeApp asks the live worker at path to run the named app's
// matching event handlers synchronously against the live runtime pool, and
// returns the number of handlers executed. event must be a JSON object (the
// matcher is defined over a map); the CLI validates it before dialing and the
// worker validates it again on the wire. ctx bounds the CLI side (a cancelled
// ctx, e.g. Ctrl-C, closes the connection so the command aborts promptly rather
// than waiting out the invocation deadline).
//
// It is the CLI's running-worker path for `relay app invoke`; there is no
// offline fallback (a manual invocation must run through the live runner/runtime
// pool, which only the worker owns). A missing/unresponsive worker, or a worker
// whose runner is not wired yet, reports ErrInvokeUnavailable. The worker's
// unknown-app / app-unavailable / handler-failure answers report
// ErrInvokeFailed with the worker's message, so the CLI surfaces the real cause
// (including a failed handler's reason) rather than masking it.
func InvokeApp(ctx context.Context, path, name string, event json.RawMessage) (int, error) {
	dialer := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvokeUnavailable, err)
	}
	defer conn.Close()
	// A manual invocation can legitimately run a handler up to the rule timeout
	// (capped at app.MaxTimeout), which can exceed the short query deadline.
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
		Command: cmdInvokeApp,
		App:     name,
		Event:   event,
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

// ReplayDLQ asks the live worker at path to re-execute the exact app and
// handler recorded by a DLQ entry, with event as the replayed payload. It is the
// CLI's running-worker path for `relay dlq replay`; there is no offline fallback
// (a replay must run through the live runner/runtime pool, which only the worker
// owns). event must be the entry's payload bytes, replayed verbatim. trace is the
// entry's optional compact lineage, carried as control-path metadata (a sibling
// of event) so the worker's replay operation can link back to the original failed
// invocation; it is never part of the event payload.
//
// ctx bounds the CLI side (a cancelled ctx, e.g. Ctrl-C, closes the connection
// so the command aborts promptly). A missing/unresponsive worker, or a worker
// whose runner is not wired yet, reports ErrInvokeUnavailable. The worker's
// unknown-app / app-unavailable / handler-not-found / handler-failure
// answers report ErrInvokeFailed with the worker's message, so the CLI surfaces
// the real cause (including a failed handler's reason) rather than masking it.
func ReplayDLQ(ctx context.Context, path, name, handler string, event []byte, trace string) error {
	dialer := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := dialer.DialContext(ctx, "unix", path)
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
		Command: cmdReplayDLQ,
		App:     name,
		Handler: handler,
		Event:   event,
		Trace:   trace,
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

// CheckReady dials the live worker at path and asks whether it is ready to
// consume. It is the CLI's `relay health` client surface. It returns
// (true, "", nil) for a ready worker; for a worker that answered not-ready
// (still starting, shutting down, or its live dependencies are currently
// failing) it returns (false, reason, err) with err wrapping ErrNotReady; and
// for a socket that cannot be reached or an exchange that fails it returns
// (false, "", err) with err wrapping ErrReadyUnavailable. The two error classes
// are distinct so a caller can tell "no worker answered" from "worker answered
// not-ready".
//
// ctx bounds the CLI side (a cancelled ctx, e.g. Ctrl-C, closes the connection
// so the command aborts promptly). The worker's own readiness probe is bounded
// independently, so a hung dependency can never hold the query open.
func CheckReady(ctx context.Context, path string) (bool, string, error) {
	dialer := net.Dialer{Timeout: runtimeStateDialTimeout}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err != nil {
		return false, "", fmt.Errorf("%w: %v", ErrReadyUnavailable, err)
	}
	defer conn.Close()
	// The worker's readiness probe is bounded by its own timeout; the client
	// deadline adds a short exchange margin so a wedged peer still cannot hold
	// the command open indefinitely.
	_ = conn.SetDeadline(time.Now().Add(readinessQueryTimeout()))
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	if err := json.NewEncoder(conn).Encode(socketRequest{Command: cmdReady}); err != nil {
		return false, "", fmt.Errorf("%w: %v", ErrReadyUnavailable, err)
	}
	var resp socketResponse
	if err := json.NewDecoder(io.LimitReader(conn, runtimeStateMaxResponse)).Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return false, "", fmt.Errorf("%w: %v", ErrReadyUnavailable, ctx.Err())
		}
		return false, "", fmt.Errorf("%w: %v", ErrReadyUnavailable, err)
	}
	if resp.ReadyResult == nil {
		return false, "", fmt.Errorf("%w: empty response", ErrReadyUnavailable)
	}
	if !resp.ReadyResult.Ready {
		return false, resp.ReadyResult.Reason, fmt.Errorf("%w: %s", ErrNotReady, messageOr(resp.ReadyResult.Reason, "not ready"))
	}
	return true, "", nil
}

// readinessQueryTimeout is the client-side bound for one readiness exchange: the
// worker's own probe bound plus the short request margin, so the client always
// outlasts a slow-but-bounded worker probe rather than cutting it off.
func readinessQueryTimeout() time.Duration {
	return readinessProbeTimeout + runtimeStateRequestTimeout
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
