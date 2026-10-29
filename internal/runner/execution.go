package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// warmAdmitterResolver returns the runner's resolved warm-budget admitter, or
// nil when the executor does not implement it (fake executors in tests, or an
// unbounded runtime). Resolution scans the registry snapshot once and caches,
// mirroring invalidatorResolver.
func (r *Runner) warmAdmitterResolver() runtime.WarmAdmitter {
	r.warmAdmitterOnce.Do(func() {
		for _, pf := range r.reg.snapshot() {
			if adm, ok := pf.executor.(runtime.WarmAdmitter); ok {
				r.warmAdmitter = adm
				return
			}
		}
	})
	return r.warmAdmitter
}

// warmSaturated reports whether err is the runtime's create-time warm-budget
// saturation sentinel (a narrow race between admission and container create). It
// is BACKPRESSURE, not a handler failure: the caller must leave the work pending
// without charging a retry or a DLQ entry.
func warmSaturated(err error) bool {
	return errors.Is(err, runtime.ErrWarmBudgetSaturated)
}

// lifecycleCancelled reports whether the DELIVERY context (the worker lifecycle
// context the stream layer hands the runner) is already canceled, i.e. the
// worker is shutting down. A cancellation must not change the PERSISTED
// retry/exhaustion decision: if it were treated as a failed attempt, a shutdown
// could schedule a retry backoff or write a terminal exhaustion marker (and thus
// a DLQ entry) for work the handler never had a chance to finish, while the
// stream layer leaves the message pending anyway (see processMessage). The
// runner therefore refuses to claim an attempt once the lifecycle is canceled
// (pre-claim check on each path) and refuses to issue a retry transition or an
// exhausted marker for a canceled delivery (recordFailure's guard). This is
// deliberately narrower than "a cancellation charges nothing": the execution
// failure telemetry (handler_failure_total, the labeled invocation-outcome and
// per-app failure counters, and last_failure_at) is incremented as soon as a
// post-claim executor reports an error, BEFORE recordFailure's guard is
// consulted, so a post-claim cancellation is still counted as a failure in
// telemetry while the persisted invocation state is left untouched. The check is
// on the PARENT context, never the invocation's own timeout context: a genuine
// handler timeout (context.DeadlineExceeded on a live delivery) still records
// normally.
func lifecycleCancelled(ctx context.Context) bool {
	return ctx.Err() != nil
}

// admitWarm pre-admits ctx against the worker-global warm-container budget
// (MAX_WARM_CONTAINERS) BEFORE a handler attempt is claimed, returning a context
// carrying the permit and whether admission succeeded. ok=false means every warm
// execution container is busy and none can be evicted within warmAdmitWait
// (backpressure): the caller MUST leave the work pending WITHOUT claiming a
// handler attempt, so no retry budget is charged. When the executor exposes no
// warm admitter (fake/in-process executors, an unbounded runtime) admission is
// unconditional and the context is returned unchanged.
//
// The permit is released by Manager.Execute when it is not consumed (an idle
// container was leased) or by the owning container on discard when it is; a
// caller that admits and then does not execute must release it, which the
// consume-aware WarmPermit.Release makes safe. The wait is bounded by
// warmAdmitWait (not r.slotWait); it is added AFTER the concurrency-slot wait, so
// the total wait may exceed the stream reclaim threshold, which is safe because
// it happens before the claim (see slotWaitTimeout). Tests may shorten it via
// r.warmWait.
func (r *Runner) admitWarm(ctx context.Context) (context.Context, bool) {
	adm := r.warmAdmitterResolver()
	if adm == nil {
		return ctx, true
	}
	wait := r.warmWait
	if wait <= 0 {
		wait = warmAdmitWait
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	permit, err := adm.AcquireWarmPermit(waitCtx)
	if err != nil {
		return ctx, false
	}
	return runtime.WithWarmPermit(ctx, permit), true
}

// executeWithRefs runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, so a concurrent
// RetireImage cannot remove the image an in-flight execution still needs
// (at-least-once safety). The release is deferred so it runs even if the
// executor panics; the helper is called per rule so the defer scope is
// per-invocation rather than accumulating across a long rule loop. extraEnv are
// the per-invocation env vars (template env values + resolved secrets).
//
// lease is the snapshot's admitted publication lease pinning the image; it is
// attached to the execution context so Manager.Execute executes under that
// admitted authority rather than acquiring a fresh (possibly rejected) lease. A
// nil lease (fake executors, no-runtime apps, test values) leaves the
// context unchanged and lets Execute acquire its own.
func (r *Runner) executeWithRefs(
	pf *PreparedApp,
	invokeCtx context.Context,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
) error {
	image := toImage(pf)
	r.refs.acquire(image)
	defer r.refs.release(image)
	if lease != nil {
		invokeCtx = runtime.WithSnapshotLease(invokeCtx, lease)
	}
	return pf.executor.Execute(invokeCtx, pf.prepared, handler, eventJSON, extraEnv)
}

// invokeOnce is the smallest reusable single-handler execution core shared by
// InvokeHandler (with or without invocation state): it resolves the per-invocation
// template env/secrets, runs the handler inside the panic boundary and the
// caller-provided capped timeout, and emits the handler success/failure metrics.
// A create-time warm-budget saturation is the one executor error that is NOT a
// failed execution: it is backpressure, so no failure metric is emitted and the
// caller maps the sentinel to its pending (or synchronous error) contract.
// It assumes the caller has already reserved the concurrency slots; on success,
// when invState is non-nil it marks the invocation complete BEFORE the success
// metrics so a crash between the side effect and MarkComplete re-runs the
// handler (at-least-once; the handler must remain idempotent), mirroring
// Handle's per-rule ordering. It returns the plain execution error (nil on
// success) that the caller routes through the invocation-state lifecycle.
func (r *Runner) invokeOnce(
	ctx context.Context,
	pf *PreparedApp,
	handler string,
	payload []byte,
	timeout time.Duration,
	lease *runtime.ImageLease,
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	msgID string,
) error {
	// The claimed attempt has begun: the caller claimed it via TryStart (or is a
	// direct no-state caller) and holds the concurrency slots. The persisted
	// attempt advances at that claim; execution normally follows, though a
	// post-claim crash can leave it spent unexecuted. This is the schedule path's
	// last_execution_at attribution point — once per claimed attempt, retries
	// included.
	r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())

	// Resolve the template's env values and secret references immediately before
	// container creation, mirroring Handle's rule path.
	extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
	if err != nil {
		// Counted as a handler failure with a zero duration (no execution
		// happened), mirroring how Handle attributes a resolution failure.
		r.recordHandlerFailure(pf.fn.Name, handler, 0)
		return fmt.Errorf("schedule invocation: app %q handler %q: %w", pf.fn.Name, handler, err)
	}

	r.log.Debug("Schedule: invoking handler",
		"app", pf.fn.Name,
		"handler", handler,
	)

	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	// Stamp the invocation's diagnostic metadata. MessageID is the schedule
	// occurrence's real Redis stream message ID, stamped on relay.message_id;
	// EventID/EventName are empty. Type: containerTypeSchedule is the strict
	// relay.type marker that classifies this one-shot invocation container.
	invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
		Type:      runtime.ContainerTypeSchedule,
		App:       pf.fn.Name,
		Handler:   handler,
		MessageID: msgID,
		NodeID:    r.nodeID,
		Image:     toImage(pf),
	})
	start := time.Now()
	panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, handler, payload, extraEnv, lease,
		invocationTrace{state: invState, invocation: invocation, attempt: claim.Attempt})
	elapsed := time.Since(start)
	if panicked {
		r.log.Error("App handler: PANICKED for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"panic_value", fmt.Sprintf("%v", panicValue),
			"stack", string(debug.Stack()),
		)
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		return err
	}
	if err != nil {
		// A create-time warm-budget saturation is BACKPRESSURE, not a failed
		// execution: the caller leaves the occurrence pending without a retry,
		// exhaustion, or DLQ entry (see schedules.go). It must therefore not be
		// attributed as a handler failure either. The event and manual paths
		// guard the same sentinel before recordHandlerFailure; this shared
		// schedule core does the same so the telemetry contract is identical.
		if warmSaturated(err) {
			return err
		}
		r.recordHandlerFailure(pf.fn.Name, handler, elapsed)
		r.log.Warn("App handler: execution failed for schedule",
			"app", pf.fn.Name,
			"handler", handler,
			"duration", elapsed,
			"reason", err,
		)
		return err
	}
	// Mark the invocation complete on success BEFORE the success metrics so a
	// crash between the side effect and MarkComplete re-runs the handler
	// (at-least-once; the handler must remain idempotent). The completion is
	// CASed on this attempt's claim; a refusal means the marker was re-claimed by
	// a newer claim or is already terminal exhausted, so this delivery does not
	// own the resolution: return the not-eligible sentinel so the stream leaves
	// the message pending rather than ACK a superseded claim's outcome.
	if invState != nil {
		if !invState.MarkComplete(invocation, claim) {
			return fmt.Errorf("%w: completion superseded for %v", stream.ErrInvocationNotEligible, invocation)
		}
	}
	r.recordHandlerSuccess(pf.fn.Name, handler, elapsed)
	r.log.Info("App handler: executed for schedule",
		"app", pf.fn.Name,
		"handler", handler,
		"duration", elapsed,
	)
	return nil
}

// handlerLogFields builds the common structured-log fields for a handler
// execution log line: app, handler, message_id, and the attempt
// attribution. With invocation state the attempt attribution is the
// Redis-backed handler attempt (the count that drives retry/exhaustion); without
// it there is no handler attempt to attribute, so the message delivery attempt is
// reported instead. It never presents a delivery count as a handler attempt.
func handlerLogFields(hasState bool, fnName, handler, msgID string, handlerAttempt int, deliveryAttempt int64) []any {
	fields := []any{
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
	}
	if hasState {
		return append(fields, "handler_attempt", handlerAttempt)
	}
	return append(fields, "delivery_attempt", int(deliveryAttempt))
}

// recordHandlerSuccess increments the success metrics shared by Handle's
// success branch, InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app success attribution, the
// last_success_at timestamp, and the duration histogram. It deliberately does
// NOT touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so an execution
// must not be double-attributed here.
func (r *Runner) recordHandlerSuccess(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "success"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	// Unlabeled total for the PostgreSQL snapshot; the labeled counter above stays
	// for Prometheus.
	r.metrics.Inc(metrics.MetricHandlerSuccess)
	// Per-app success attribution.
	r.metrics.IncLabels(metrics.MetricFunctionHandlerSuccess,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampSuccess, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordHandlerFailure increments the failure metrics shared by Handle's
// execution-error branches and its post-claim marshal/env/secret failures,
// InvokeHandler, and manual invocation: the labeled invocation
// outcome counter, the unlabeled total, per-app failure attribution, and
// the duration histogram. A failed attempt that will retry still counts as a
// failure here (it sets last_failure_at); only the DLQ-routed exhaustion
// additionally sets last_dlq_at (see recordFailure). It deliberately does NOT
// touch the event classification counters or app_events_matched_total —
// Handle owns those and counts them once per logical event, so a failure must
// not be double-attributed here.
func (r *Runner) recordHandlerFailure(fnName, handler string, duration time.Duration) {
	r.metrics.IncLabels(metrics.MetricHandlerInvocations,
		[]metrics.Label{
			{Name: "outcome", Value: "failure"},
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		})
	r.metrics.Inc(metrics.MetricHandlerFailure)
	r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampFailure, time.Now().Unix())
	r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
		[]metrics.Label{
			{Name: "app", Value: fnName},
			{Name: "handler", Value: handler},
		}, duration)
}

// recordFailure handles a failed invocation attempt: it decides whether the
// handler attempt is retryable or exhausted, updates the invocation state and
// metrics accordingly, and returns the outcome plus the plain error Handle
// should aggregate. It is used for execution, marshal, secret-resolution, and
// timeout failures (all are failed attempts).
//
// A retryable handler attempt (attempt < 1+retries) schedules a retry backoff
// via RecordFailure, counts function_retries_total, and returns
// (outcomeRetryable, err). An exhausted handler attempt (attempt >= 1+retries)
// marks the invocation terminal via MarkExhausted, counts function_dlq_total, and
// returns (outcomeExhausted, err). Both transitions are CASed on the claim
// (attempt + token): a refusal (the claim is stale, or the marker is already
// terminal) returns (outcomePendingSkip, nil) so the message stays pending and a
// superseded claim never ACKs or dead-letters a newer claim's message. The
// message-level DLQ decision (whether EVERY matched invocation is terminal) is
// NOT made here; it is deferred to Handle's end-of-loop aggregation, so an
// invocation can exhaust while others still run without short-circuiting them.
//
// The retryable error is returned plain. The exhausted error is a
// *stream.HandlerExhaustedError carrying the exhausted handler attempt, so the
// stream layer can attribute the DLQ entry's handler_attempts from the handler
// retry state rather than the message delivery count. Its message names the
// same attempt count, keeping the DLQ `reason` consistent with
// `handler_attempts`.
//
// Lifecycle cancellation: a failure observed after the delivery context was
// canceled (worker shutdown) must not change the PERSISTED retry/exhaustion
// decision, regardless of the persisted attempt count. recordFailure returns
// (outcomePendingSkip, nil) without issuing either transition, so a shutdown can
// never schedule a retry backoff or write an exhaustion marker (and therefore
// never a DLQ entry); the claim, if one was confirmed, is left in place and its
// running deadline elapses so a later delivery claims the next attempt. The
// stream layer leaves the message pending either way. This guard covers only the
// persisted invocation state: the caller has already incremented the failure
// telemetry (handler_failure_total etc.) for the post-claim execution error that
// brought it here, so a shutdown-canceled execution is still visible in those
// counters even though no retry/exhaustion transition is issued.
func (r *Runner) recordFailure(
	ctx context.Context,
	invState stream.InvocationState,
	invocation string,
	claim stream.InvocationClaim,
	retries int,
	fnName, handler, msgID string,
	origErr error,
) (invocationOutcome, error) {
	// A lifecycle cancellation must not produce a persisted failed-attempt
	// transition. The delivery is being abandoned by a shutdown and the stream
	// layer will leave the message pending regardless, so scheduling a retry
	// backoff or writing a terminal exhaustion marker here would let a shutdown
	// consume the retry budget or dead-letter work the handler never had a chance
	// to run. (The caller has already counted the failure telemetry for any
	// post-claim execution error; only this persisted transition is suppressed.)
	// Leave the claim (if any) in place: its running deadline elapses and a later
	// delivery claims the NEXT attempt. Reported as a pending skip (no retry/DLQ
	// transition) exactly like a protected skip. This is evaluated BEFORE either
	// transition, so even a script that would otherwise commit is not issued for
	// a canceled delivery.
	if lifecycleCancelled(ctx) {
		r.log.Debug("App handler: attempt canceled during shutdown; leaving pending without charging retry/exhaustion",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", claim.Attempt,
			"reason", origErr,
		)
		return outcomePendingSkip, nil
	}
	handlerAttempt := claim.Attempt
	maxAttempts := 1 + retries
	if handlerAttempt >= maxAttempts {
		// Exhausted: mark the invocation terminal, CASed on this attempt's claim
		// (attempt + token). If the store refuses (the claim is stale — a newer
		// token now owns the marker — or the marker is already terminal), this
		// delivery does not own the invocation's resolution: do NOT report
		// exhaustion (which would dead-letter a message whose newer claim may
		// still resolve), leave the message pending, and let the newer claim or a
		// later terminal-skip delivery drive the DLQ decision. This is the
		// exhaustion-commit attribution point: the invocation exhausted its
		// retries and its terminal exhausted marker is now persisted, so
		// function_dlq_total and last_dlq_at are stamped HERE — not on every
		// failure, not again on the later terminal-skip redeliveries of the same
		// invocation, and not at the message-level PostgreSQL DLQ insert (which
		// happens later in the stream layer, only after all exhausted siblings
		// resolve, and only if the row commit succeeds).
		if !invState.MarkExhausted(invocation, claim) {
			r.log.Warn("App handler: exhaustion superseded; leaving pending",
				"app", fnName,
				"handler", handler,
				"message_id", msgID,
				"handler_attempt", handlerAttempt,
			)
			return outcomePendingSkip, nil
		}
		r.metrics.IncLabels(metrics.MetricFunctionDLQ,
			[]metrics.Label{{Name: "app", Value: fnName}})
		r.metrics.SetAppTimestamp(fnName, metrics.AppTimestampDLQ, time.Now().Unix())
		r.log.Error("App handler: exhausted; invocation terminal",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
			"handler_attempts_total", maxAttempts,
		)
		return outcomeExhausted, &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: handlerAttempt,
				Err:      origErr,
			}},
		}
	}
	// Retryable: schedule a retry backoff and count the retry. The claim
	// (attempt + token) is passed as a compare-and-set guard so a stale owner
	// (whose running deadline elapsed and whose invocation was re-claimed by a
	// newer claim) cannot overwrite the newer claim's marker. If the store
	// refuses (stale claim or terminal marker), this delivery does not own the
	// invocation: leave the message pending rather than schedule a retry on a
	// superseded claim's behalf.
	backoff := retryBackoff(handlerAttempt)
	if !invState.RecordFailure(invocation, claim, backoff) {
		r.log.Warn("App handler: retry superseded; leaving pending",
			"app", fnName,
			"handler", handler,
			"message_id", msgID,
			"handler_attempt", handlerAttempt,
		)
		return outcomePendingSkip, nil
	}
	r.metrics.IncLabels(metrics.MetricFunctionRetries,
		[]metrics.Label{{Name: "app", Value: fnName}})
	r.log.Warn("App handler: failed attempt; retrying later",
		"app", fnName,
		"handler", handler,
		"message_id", msgID,
		"handler_attempt", handlerAttempt,
		"handler_attempts_total", maxAttempts,
		"retry_backoff", backoff,
		"reason", origErr,
	)
	return outcomeRetryable, fmt.Errorf(
		"app %q handler %q: handler attempt %d failed: %w",
		fnName, handler, handlerAttempt, origErr,
	)
}

// retryBackoff returns the retry delay to apply after a failed attempt with the
// given completed attempt number. The schedule is fixed and non-configurable:
// attempt 1 → 1m, 2 → 2m, 3 → 5m, 4+ → 10m (capped). The runner owns rule
// semantics, so the schedule lives here, not in the stream store.
func retryBackoff(completedAttempts int) time.Duration {
	switch {
	case completedAttempts <= 1:
		return time.Minute
	case completedAttempts == 2:
		return 2 * time.Minute
	case completedAttempts == 3:
		return 5 * time.Minute
	default:
		return 10 * time.Minute
	}
}

// eventFields extracts the low-cardinality, label-safe event_id and event_name
// from the event for the invocation's runtime RunMeta (container labels and the
// stream output prefix). These are never used as metric labels or structured
// log fields.
func eventFields(event map[string]any) (eventID, eventName string) {
	if v, ok := event["event_id"]; ok {
		eventID = stringify(v)
	}
	if v, ok := event["event_name"]; ok {
		eventName = stringify(v)
	}
	return eventID, eventName
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// resolveExtraEnv builds the per-invocation env for a template: the literal env
// values followed by the resolved secret values, both name-ordered. Secret
// values are resolved here, immediately before execution, so they are never
// cached on Prepared or baked into the fingerprint. A template that references a
// secret but has no provider configured fails with a clear error naming the
// reference. The returned slice is the ONLY place a resolved secret value lives
// before it is handed to the executor; the executor carries it in the request
// frame's env map to the reused bootstrap process. It is never logged,
// persisted, or written into an execution container's Docker Config.Env.
func (r *Runner) resolveExtraEnv(ctx context.Context, tmpl *app.Template) ([]string, error) {
	var extra []string
	for _, ev := range tmpl.EnvList() {
		extra = append(extra, ev.Name+"="+ev.Value)
	}
	for _, sb := range tmpl.SecretList() {
		if r.secrets == nil {
			return nil, fmt.Errorf("app references secret %q but no secret provider is configured", sb.Ref)
		}
		val, err := r.secrets.Resolve(ctx, sb.Ref.String())
		if err != nil {
			// The provider's error carries the reference name only, never a value.
			return nil, fmt.Errorf("resolve secret %q: %w", sb.Ref, err)
		}
		extra = append(extra, sb.Name+"="+val)
	}
	return extra, nil
}
