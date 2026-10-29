package worker

import (
	"encoding/json"
	"errors"
	"time"

	"relay/internal/app"
	"relay/internal/processlock"
)

// This file declares the worker control-socket wire protocol shared by the
// serving worker and the CLI client: the socket path, exchange timeouts, wire
// commands and error codes, request/response frames, result types, and the
// sentinel errors both sides classify on.

// SocketPath is the fixed live query socket `relay app inspect` dials for
// live runtime-pool gauges, `relay stats reset` dials to reset a running
// worker's statistics, `relay app invoke` dials to run an app's
// matching handlers synchronously against the live runtime, `relay dlq
// replay` dials to re-execute one dead-lettered handler, and `relay health`
// dials for worker health (readiness plus live dependency health). It lives
// beside the
// process lock in the ephemeral runtime directory
// (internal/processlock.DefaultDir), NOT under the persisted /var/lib/relay
// state volume: a socket is process state that can neither outlive the worker
// nor be meaningfully persisted.
const SocketPath = processlock.DefaultDir + "/relay.sock"

const (
	// runtimeStateRequestTimeout bounds one query end to end: it is set as the
	// connection deadline on BOTH sides (the CLI read and the worker read/write),
	// so a wedged peer can never hold a handler open indefinitely.
	runtimeStateRequestTimeout = 2 * time.Second
	// runtimeStateMaxRequest / runtimeStateMaxResponse bound the bytes a single
	// local query may carry, so a misbehaving peer cannot make either side
	// allocate unboundedly.
	runtimeStateMaxRequest  = 64 << 10
	runtimeStateMaxResponse = 64 << 10
	// invokeTimeout bounds one manual invocation end to end on the WORKER side:
	// it is the context deadline the runner's executor runs under, so a wedged
	// container cannot hold the socket handler (and the CLI's dial) open
	// indefinitely. It deliberately exceeds the largest allowed rule timeout
	// (app.MaxTimeout, 5m) plus a margin for container start/model load, so
	// a legitimate long-running handler is never cut short by the socket itself;
	// the per-rule timeout still bounds the actual execution.
	invokeTimeout = app.MaxTimeout + 2*time.Minute
)

// Wire error codes. They are stable strings (not prose) so the CLI can
// distinguish "the worker has no pool for this app" from any other
// failure without parsing a message.
const (
	errCodeMalformedRequest = "malformed_request"
	errCodeUnknownApp       = "unknown_app"
	errCodeStatsUnavailable = "stats_unavailable"
	errCodeStatsFailed      = "stats_reset_failed"
	// Manual-invocation wire codes. errCodeInvokeUnavailable reports that no
	// runner is wired (the worker is not serving invocations);
	// errCodeAppUnavailable reports a registered app whose image could
	// not be built; errCodeInvokeFailed reports that every matching handler was
	// attempted but at least one failed (or the invocation could not start).
	errCodeInvokeUnavailable = "invoke_unavailable"
	errCodeAppUnavailable    = "app_unavailable"
	errCodeInvokeFailed      = "invoke_failed"
	// DLQ-replay wire codes. errCodeHandlerNotFound reports that the app is
	// currently configured but the exact handler recorded in the DLQ entry is no
	// longer in its template (an intentional configuration change);
	// errCodeReplayFailed reports that the single replay attempt executed and
	// failed. The CLI keeps the DLQ entry in both cases.
	errCodeHandlerNotFound = "handler_not_found"
	errCodeReplayFailed    = "replay_failed"
)

// Wire commands. Every request frame MUST name its command explicitly:
// cmdRuntimeState requests app's live pool gauges, cmdResetStats asks the
// worker to reset its Relay statistics (the command name is the operator-facing
// semantic, not a DB operation — the socket never exposes storage details), and
// cmdInvokeApp asks the worker to run app's matching event handlers
// synchronously against the live runner. An absent or unknown command is
// malformed.
const (
	cmdRuntimeState = "runtime_state"
	cmdResetStats   = "reset_stats"
	cmdInvokeApp    = "invoke_app"
	cmdReplayDLQ    = "replay_dlq"
	cmdReady        = "ready"
)

// ErrRuntimeStateUnavailable reports that the live worker query socket could not
// be reached or did not answer — no worker is running, the socket is stale, or
// the exchange failed. The CLI treats it as "render the live fields unknown".
var ErrRuntimeStateUnavailable = errors.New("runtime state unavailable")

// ErrUnknownApp reports that the worker answered but has no live pool for
// the requested app (it was never warmed, or was already removed).
var ErrUnknownApp = errors.New("unknown app")

// ErrRuntimeStatsNoWorker reports that the worker socket positively does not
// exist or is refusing connections, so no worker can have received the reset
// request. It is the ONLY reset outcome that permits the CLI's direct state-DB
// fallback: the absence is unambiguous (connection refused / no such file), so
// there is no live worker whose authority could be violated. A transient dial
// timeout is deliberately NOT this error (see ErrRuntimeStatsUnavailable).
var ErrRuntimeStatsNoWorker = errors.New("runtime stats: no running worker")

// ErrRuntimeStatsUnavailable reports that the worker socket exchange could not
// be completed and it is UNKNOWN whether a live worker received (or is still
// processing) the reset — a dial timeout, a request write failure, or a
// missing/malformed response. Because a live worker may have reset or may reset
// after the CLI gives up, the CLI must NOT fall back to a direct state-DB reset
// on this error; it surfaces it instead, preserving the live worker as the
// authority. It is also reported when a live worker answers that it has no
// stats source (stats_unavailable): the worker is authoritative and explicitly
// cannot reset, so a silent direct write would mask that.
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
var ErrInvokeUnavailable = errors.New("app invocation unavailable")

// ErrInvokeNotStarted reports that a control-socket invocation or DLQ replay
// definitively did not begin executing a handler: the dial failed (no
// connection exists, so no request was sent), or the worker explicitly answered
// that it has no invoker. It wraps ErrInvokeUnavailable, so existing callers keep
// their classification. A DLQ replay uses it to release its admission claim
// because no handler can have run.
//
// A request write/encode failure is deliberately NOT classified here. A net.Conn
// write can return an error after delivering some or even all request bytes, so
// the worker may have received a complete frame and started the handler; that
// outcome is ambiguous. The same applies to a response read/decode failure or
// client cancellation, so all of those report only ErrInvokeUnavailable and the
// replay claim must be retained.
var ErrInvokeNotStarted = errors.New("app invocation did not start")

// ErrInvokeFailed reports that the worker attempted the manual invocation but at
// least one matching handler failed, or the invocation could not start (an
// unknown/unavailable app, or a concurrency slot timeout). The worker's
// error text is preserved (the CLI prints it) and no handler count is reported.
var ErrInvokeFailed = errors.New("app invocation failed")

// ErrNotReady reports that the worker answered the readiness query and is not
// ready to consume: it is still starting, is shutting down, or its live
// dependencies (Redis consumer, Docker daemon, or a configured NETWORKS network)
// are currently failing. `relay health` maps it to a non-zero exit. It is
// deliberately distinct from ErrRuntimeStateUnavailable: the worker IS running
// and answered; it is simply not ready.
var ErrNotReady = errors.New("worker not ready")

// ErrReadyUnavailable reports that the readiness query could not be completed
// because the worker socket could not be reached — no worker is running, the
// socket is stale, or the exchange failed. `relay health` treats it as not
// healthy (a running worker is required), but classifies it separately so an
// operator can tell "no worker" from "worker answered not-ready".
var ErrReadyUnavailable = errors.New("readiness unavailable")

// RuntimeState is the live, worker-local runtime-pool gauge view returned by the
// query socket. Every field is a valid zero for an app with an existing but
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

// ReadyResult is the readiness answer returned by the query socket. Ready is
// always present for a readiness response (no omitempty) so a NOT-ready
// steady-state answer is explicitly representable; Reason carries a short fixed
// explanation when Ready is false and is empty when it is true. It never carries
// a dependency's raw error text.
type ReadyResult struct {
	Ready  bool   `json:"ready"`
	Reason string `json:"reason,omitempty"`
}

// socketRequest is the newline-JSON request frame. Command selects the
// operation: cmdRuntimeState requests app's live pool gauges (App is
// required); cmdResetStats asks the worker to reset its Relay statistics;
// cmdInvokeApp asks the worker to run app's matching event handlers
// synchronously (App is required and Event carries the operator-supplied
// JSON event object); cmdReplayDLQ re-executes one DLQ entry's exact
// app/handler (App and Handler are required, Event carries the
// replayed payload, and Trace optionally carries the entry's compact lineage so
// the replay can link back to the original failed invocation); cmdReady asks
// whether the worker is ready to consume. A frame carries
// exactly one command, and an absent or unknown command is malformed.
//
// Trace is deliberately a SIBLING of Event, not part of it: the trace lineage is
// control-path metadata, so it never reaches the handler's event map and the
// replay's own worker-side operation span stays independent of it.
type socketRequest struct {
	Command string          `json:"command,omitempty"`
	App     string          `json:"app,omitempty"`
	Handler string          `json:"handler,omitempty"`
	Event   json.RawMessage `json:"event,omitempty"`
	Trace   string          `json:"trace,omitempty"`
}

// socketResponse is the newline-JSON response frame. Exactly one of the
// embedded RuntimeState (success), Error (failure), ResetStats=true
// (reset success), *InvokeResult (manual-invocation success), or *ReadyResult
// (readiness answer, ready or not) is present. Message optionally carries
// human-readable detail for an Error, so the CLI can surface a clear cause (e.g.
// a handler failure reason) while Error remains the stable machine code it
// classifies on.
type socketResponse struct {
	*RuntimeState
	*InvokeResult
	*ReadyResult
	ResetStats bool   `json:"reset_stats,omitempty"`
	Replayed   bool   `json:"replayed,omitempty"`
	Error      string `json:"error,omitempty"`
	Message    string `json:"message,omitempty"`
}
