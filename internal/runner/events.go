package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/stream"
)

// invocationOutcome classifies what one delivery round did for a single matched
// invocation. Handle's rule loop returns one of these per invocation so it can
// aggregate the per-invocation outcomes AFTER the whole loop (see Handle's doc
// comment), rather than fail-fast on the first failure. outcomeRetryable and
// outcomeExhausted are produced by recordFailure; the skip outcomes are produced
// by the TryStart/slot paths.
type invocationOutcome int

const (
	// outcomeExecuted means the handler ran and returned success (the invocation
	// was marked complete).
	outcomeExecuted invocationOutcome = iota
	// outcomeTerminalSkip means the invocation was skipped because it is already
	// terminal: complete or exhausted (never eligible again). Exhausted skips are
	// distinguishable from complete skips by the existing marker's attempt
	// count (>0).
	outcomeTerminalSkip
	// outcomePendingSkip means the invocation was skipped because it is protected
	// (running or waiting out a retry backoff) or its concurrency slot timed out:
	// it is unresolved, so the message must stay pending (never ACKed).
	outcomePendingSkip
	// outcomeRetryable means the invocation failed a retryable attempt: a retry
	// backoff was scheduled and the message must stay pending for a later retry.
	outcomeRetryable
	// outcomeExhausted means the invocation failed its last attempt: it was
	// marked exhausted (terminal) and, if every other matched invocation is also
	// terminal, the message routes to the DLQ.
	outcomeExhausted
)

// runInvocation runs one rule's handler while holding a reference to the
// app's image for the duration of the invocation, converting an
// executor/runtime panic into a failed-attempt error instead of letting it
// escape Handle and kill the worker. This is the single panic boundary for
// message processing: panics inside executor/runtime code are a misbehaving
// execution (isolated, retried via the normal pending/reclaim flow), while
// panics elsewhere (startup, reconciler, Redis client) stay fatal and visible.
//
// The invocation context's cancel is deferred here so it always runs, even when
// the executor panics: without this, a panic that unwinds past the call site
// would skip the explicit cancel() and leak the context's timer until it fired
// on its own. executeWithRefs's own defers (image refcount release) run during
// panic unwinding BEFORE the recover here, so the image reference is never
// leaked; the returned panic value is logged by the caller.
func (r *Runner) runInvocation(
	pf *PreparedApp,
	invokeCtx context.Context,
	cancel context.CancelFunc,
	handler string,
	eventJSON []byte,
	extraEnv []string,
	lease *runtime.ImageLease,
	trace invocationTrace,
) (panicked bool, panicValue any, err error) {
	// The end-to-end invocation span, shared by the event-rule, schedule, and
	// manual paths (all funnel through here). It is a child of the delivery
	// (or replay/manual operation) context, and its span context is propagated
	// into the executor so the runtime's runtime.execute/acquire/invoke children
	// nest beneath it. RunMeta and the timeout already stamped on invokeCtx are
	// inherited.
	//
	// Retry lineage: a new attempt links to the PREVIOUS attempt's stored span
	// reference (when one was persisted) and then records its own span context
	// so a later retry, even after a worker restart, can link back to it. The
	// link is not a parent: every attempt is a distinct function.invoke span
	// (usually sharing the original upstream trace through the delivery context)
	// while the link records the causal retry chain across attempts. Recording
	// only ever persists the compact traceparent/tracestate lineage, never
	// baggage, and a state-free caller (manual/replay) records nothing.
	var runtimeName string
	if pf != nil && pf.fn.Template != nil {
		runtimeName = pf.fn.Template.Runtime
	}
	var fnName string
	if pf != nil {
		fnName = pf.fn.Name
	}
	spanCtx, span := startInvocationSpan(invokeCtx, fnName, handler, runtimeName, trace.spanOpts()...)
	trace.record(span.SpanContext())
	defer func() {
		if panicked {
			finishInvocationSpan(span, fmt.Errorf("executor panic: %v", panicValue))
			return
		}
		finishInvocationSpan(span, err)
	}()
	defer cancel()
	defer func() {
		if pv := recover(); pv != nil {
			panicked = true
			panicValue = pv
			err = fmt.Errorf("executor panic: %v", pv)
		}
	}()
	return false, nil, r.executeWithRefs(pf, spanCtx, handler, eventJSON, extraEnv, lease)
}

// Handle evaluates the event against all loaded apps and executes every
// matching rule's handler. It returns nil only when every invocation succeeded
// (or nothing matched); otherwise it returns an error so the stream layer does
// not acknowledge the message.
//
// At-least-once semantics: Handle returns an error after partial successes, so
// a retried message re-runs the handlers that already succeeded. The event
// classification counters (events_received/matched/unmatched_total) are NOT
// delivery-attempt counters: each logical event is classified exactly once
// across redeliveries/retries via an atomic claim in the message's
// invocation-state hash (see stream.InvocationState.ClaimClassification). A
// handler failure stays in the matched class. Handle does NOT claim
// exactly-once execution: redeliveries re-run whatever is not yet recorded as
// complete, and the handlers must stay idempotent.
//
// Aggregate, per-invocation semantics: a single Redis message can match multiple
// "<app>/<handler>" invocations, and each is tracked independently in the
// per-message invocation-state hash. Every eligible matching invocation gets its
// own attempt on each delivery regardless of the others' outcomes: a failure in one
// handler does NOT prevent later matching handlers from running. The rule loop is
// strictly sequential (never parallel), and after iterating every matching rule
// Handle aggregates the per-invocation outcomes into the message-level return
// contract below.
//
// Invocation state: when the stream layer injects an InvocationState into ctx
// (see stream.WithInvocationState), Handle skips any matching invocation whose
// "<app>/<handler>" ID is already recorded as completed on a previous
// delivery, is protected by an active attempt deadline or a retry backoff (a
// running or next_attempt_at marker whose persisted deadline has not yet passed
// — this or another replica may be executing it, or it is waiting out its
// backoff), or is exhausted (terminal). Skipped invocations are not executions:
// they do not touch the handler_* or function_* metrics.
// app_events_matched_total still counts the app as engaged (it
// matched), which is attribution, not execution counting. When no invocation
// state is present (direct Handle callers/tests, or invocation tracking
// disabled) Handle behaves exactly as before: it runs every matching handler
// and returns the first failure's plain error immediately, with no invocation
// wrapping or DLQ attribution.
//
// The per-invocation attempt is claimed (TryStart) only AFTER the concurrency
// slots and the warm-container budget are admitted, so a capacity rejection
// spends no attempt. The claim is the persisted attempt boundary: a crash after
// a confirmed claim but before the handler runs spends that attempt when its
// running deadline later elapses. The configured retry budget bounds normal
// failing executions but does not cap admitted claims across repeated crashes, so
// the persisted count can exceed 1+retries before any real failure (see
// internal/stream/doc.go). TryStart itself never exhausts, though: exhaustion is
// recorded here only when a later attempt actually runs and fails with the
// persisted count already at or above 1+retries, so a crash alone cannot DLQ a
// message.
//
// Pending desired generations: the registry can hold, beside the active set, a
// PENDING desired template for a valid generation that is being prepared but is
// not yet runnable (see Registry.SetPending). Pending rules are MATCHED — they
// engage their app and keep the message pending as unavailable — but they never
// execute, claim a TryStart, exhaust, ACK, or DLQ. This closes the event-loss
// window during preparation: a delivery that matches ONLY a pending rule (a
// brand-new app before its first image, or a new rule not yet in the active
// generation) is matched-but-unavailable rather than unmatched, so it is never
// ACKed away. A pending rule whose invocation is ALSO matched by the active
// generation is deduped by "<app>/<handler>": the active generation executes it
// and it is not separately held. When the new generation is installed (or the
// app is removed/invalidated) the pending entry is cleared atomically with the
// registry mutation.
//
// Return contract (with invocation state, aggregated after the full rule loop):
//   - a plain (retryable) error when any matched invocation had a retryable
//     failure this delivery — regardless of other invocations' outcomes — so
//     the message stays pending and is retried. The first such error is returned.
//   - a wrapped runner.ErrAppUnavailable when at least one MATCHED
//     invocation belongs to a configured app whose image is not currently
//     built and that invocation has not already completed. Such an event is
//     MATCHED, never unmatched, but the invocation cannot run this delivery: the
//     message stays pending (not ACKed, and not DLQ'd solely for
//     unavailability). No handler attempt is claimed and no handler counter is
//     touched, because no handler ran. A later delivery — once the app is
//     rebuilt and available again — completes the outstanding invocation.
//   - a wrapped stream.ErrInvocationExhausted when every invocation in the
//     matched set is terminal (complete or exhausted), at least one of them is
//     exhausted, and no retryable failure or unavailable match occurred this
//     delivery. The whole message is terminal, so the stream layer routes it to
//     the DLQ. This also fires on a redelivery where the exhausting invocation
//     was already marked exhausted by an earlier delivery: the message may still
//     be pending because its DLQ write (or the post-DLQ XACK) failed, so it must
//     be re-routed rather than returned nil and ACKed without a DLQ entry.
//   - a wrapped stream.ErrInvocationNotEligible when no retryable failure
//     occurred and the message is not all-terminal, but at least one matched
//     invocation was skipped because it is protected (running or waiting out a
//     retry backoff) or its concurrency slot timed out. This fires even when
//     other invocations executed successfully in this same call: a protected
//     or slot-timeout invocation is unresolved, so the message must stay pending
//     and NOT be ACKed (this is what fixes the cross-replica ACK hazard — a
//     replica that reclaims a message whose invocation is still in flight on
//     another replica must not ACK it).
//   - nil when every matched invocation is complete (or nothing matched), so the
//     stream layer ACKs the message.
//
// Panic boundary: each invocation's execution runs inside runInvocation, which
// recovers an executor/runtime panic and converts it into a normal failed
// attempt (see runInvocation). A panicking execution is therefore isolated and
// retried via the same failure/backoff/exhaustion machinery as any other
// failure — it never escapes Handle to kill the worker. The image refcount is
// still released (executeWithRefs's defer runs during unwinding) and the
// invocation's context cancel is deferred so no timer leaks. Panics elsewhere
// (startup, reconciler, Redis client) are not recovered here and stay fatal.
func (r *Runner) Handle(ctx context.Context, msgID string, event map[string]any) error {
	// Best-effort delivery attempt, defaulting to 1 when the stream did not set
	// it (e.g. when the runner is driven directly in tests). It is the delivery
	// attempt used for logging; without invocation state the handler attempt is
	// unknown, so the delivery attempt is logged instead.
	deliveryAttempt := stream.DeliveryAttemptFrom(ctx)
	// Best-effort invocation state, absent when the stream did not inject it
	// (direct Handle callers/tests, or invocation tracking disabled).
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Trackers for the message-level return contract, aggregated only after the
	// whole rule loop has run (see the Handle doc comment). skippedPending records
	// whether any matched invocation was skipped because it is protected (running
	// or waiting out a retry backoff) or its concurrency slot timed out — an
	// unresolved invocation that must keep the message pending even when other
	// invocations succeeded this call. firstErr holds the first plain retryable
	// failure; exhaustedInvocations collects EVERY exhausted matched invocation —
	// the typed metadata of one that exhausted this delivery, or one synthesized
	// from a terminal-skip of an invocation already marked exhausted on a previous
	// delivery (so a redelivery after a failed DLQ write re-routes instead of
	// ACKing). The aggregate returns them all on a *stream.HandlerExhaustedError,
	// so each exhausted app/handler gets its own DLQ entry with its own
	// attempt count. anyExhausted records whether any matched invocation is
	// exhausted (this delivery or a previous one). These are only meaningful when
	// hasState is true.
	skippedPending := false
	var firstErr error
	// claimErr records the first ambiguous-claim failure (a TryStart store
	// error) seen this delivery. It is kept separate from firstErr because a
	// claim failure is NOT a failed attempt (no handler ran, no retry/exhaustion
	// accounting); it must stay pending, and it is surfaced (wrapping
	// ErrInvocationNotEligible) so the cause is distinguishable from an ordinary
	// protected skip.
	var claimErr error
	var exhaustedInvocations []stream.ExhaustedInvocation
	anyExhausted := false
	// executed tracks whether any invocation actually executed, for the
	// no-state (direct caller/test) path, which preserves the old fail-fast
	// tail exactly. With invocation state it is unused (the aggregate uses the
	// per-invocation outcomes instead).
	executed := false

	// Take one consistent snapshot for the whole call so a concurrent registry
	// swap mid-execution cannot reorder or drop apps under us, and PIN each
	// published app's image while the registry lock still protects its
	// entry. The pins are held until Handle returns, so an image published at
	// snapshot time cannot be removed out from under this delivery even if the
	// entry is superseded concurrently; each matching execution carries its
	// app's pin into Manager.Execute. The pins are shares of the published
	// lease, so a retirement in progress never blocks an already-admitted
	// delivery.
	snap := r.reg.snapshotPinned()
	defer snap.release()
	snapshot := snap.fns

	// Pre-pass: collect every matched invocation ID so an exhausted attempt can
	// decide whether the whole message is terminal (all matched invocations
	// complete or exhausted), and collect the engaged app names. The
	// registry snapshot holds one entry per app, so an app with several
	// matching rules is appended once here (deduped by construction). This must
	// be complete before any execution, because a rule that exhausts early must
	// still see the full set of matched invocations (including ones that sort
	// later). Matching is pure; a panic here is a programming error that escapes
	// and no counter has been touched yet.
	//
	// Matching includes configured apps whose image is not currently built
	// (available == false): an event matching ONLY such an app is MATCHED,
	// not unmatched, so it is classified matched and the app is counted as
	// engaged. Skipping unavailable apps here (the previous behavior) both
	// mis-classified those events as unmatched and let a mixed message ACK
	// without running the unavailable app's share. unavailableMatched
	// records the invocations that matched but cannot run this delivery; an
	// unresolved one keeps the message pending rather than ACKed or DLQ'd for
	// unavailability alone (see the aggregate below). A nil Template (only
	// reachable from a hand-built unavailable entry, never from the loader)
	// cannot match and is skipped.
	var matched []string
	var matchedFns []string
	var unavailableMatched []string
	for _, pf := range snapshot {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		matchedFns = append(matchedFns, pf.fn.Name)
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			matched = append(matched, invocation)
			if !pf.available {
				unavailableMatched = append(unavailableMatched, invocation)
			}
		}
	}

	// Pending desired matches: a valid generation being PREPARED but not yet
	// runnable contributes its desired invocations as matched-but-unavailable,
	// so a delivery arriving during preparation stays pending instead of being
	// acknowledged as unmatched (the new-app / new-rule event-loss gap this
	// closes). Pending entries are non-runnable placeholders (no image, no
	// executor): they are matched here for CLASSIFICATION and holding only and
	// are deliberately absent from the execution loop below. Dedupe by
	// invocation identity against the active matches: a rule that is active in
	// v1 and also present in pending v2 is executed by the active path and must
	// not be separately held. A pending-only invocation still counts as matched
	// and engages its app, but its non-terminal (unresolved) state keeps the
	// message pending — it never executes, claims a TryStart, exhausts, ACKs, or
	// DLQs.
	matchedSet := make(map[string]struct{}, len(matched))
	for _, inv := range matched {
		matchedSet[inv] = struct{}{}
	}
	matchedFnSet := make(map[string]struct{}, len(matchedFns))
	for _, fnName := range matchedFns {
		matchedFnSet[fnName] = struct{}{}
	}
	for _, pf := range snap.pending {
		if pf.fn.Template == nil {
			continue
		}
		rules := snap.matchingPendingRules(pf, event)
		if len(rules) == 0 {
			continue
		}
		engaged := false
		for _, rule := range rules {
			invocation := pf.fn.Name + "/" + rule.Handler
			if _, dup := matchedSet[invocation]; dup {
				// Already matched (and executable) through the active
				// generation: do not separately hold the same invocation.
				continue
			}
			matchedSet[invocation] = struct{}{}
			matched = append(matched, invocation)
			unavailableMatched = append(unavailableMatched, invocation)
			engaged = true
		}
		if engaged {
			if _, dup := matchedFnSet[pf.fn.Name]; !dup {
				matchedFnSet[pf.fn.Name] = struct{}{}
				matchedFns = append(matchedFns, pf.fn.Name)
			}
		}
	}

	// An unavailable matched invocation is UNRESOLVED unless it already COMPLETED
	// for this message: a complete marker means the invocation's work here is
	// settled (it ran before this worker lost/never had the app), so it does
	// not hold the message pending. Anything else — never ran, or exhausted
	// awaiting a DLQ re-route whose attempt count only the available path can read
	// back — is unresolved, and the message must stay pending: it must neither be
	// ACKed (which could drop an outstanding execution or an unpersisted DLQ
	// entry) nor be dead-lettered solely for unavailability. The exhausted case is
	// deliberately included: this worker cannot attribute the exhausted attempt
	// count from an unavailable entry, and ACKing would race another replica's
	// DLQ write, so the safe choice is to hold pending until the app is
	// available again and the normal path surfaces the exhaustion with correct
	// metadata. With no invocation state there is no marker to consult, so any
	// unavailable match is treated as unresolved (the fail-safe direction). This
	// is read-only: no attempt is claimed and no counter is touched for an
	// unavailable app.
	var unresolvedUnavailable []string
	if len(unavailableMatched) > 0 {
		if hasState {
			for _, invocation := range unavailableMatched {
				if !invState.IsComplete(invocation) {
					unresolvedUnavailable = append(unresolvedUnavailable, invocation)
				}
			}
		} else {
			unresolvedUnavailable = unavailableMatched
		}
	}

	// Classify the logical event exactly once (received == matched +
	// unmatched), regardless of delivery attempt. With invocation state the
	// stream-injected handle claims the classification atomically in the
	// message's invocation-state hash: only the first delivery across
	// redeliveries, reclaims, and replicas wins, so retries never double-count.
	// A claim error counts nothing (classification is a partition, so a missed
	// count is preferable to a double count). Without invocation state (direct
	// Handle callers/tests) there is no dedup channel, so each call is treated
	// as a distinct logical event.
	//
	// A handler failure does NOT move the event out of the matched class: the
	// classification is decided from matching alone, before any execution.
	// Unmatched events are acknowledged and never retried.
	classify := true
	if hasState {
		claimed, err := invState.ClaimClassification()
		if err != nil {
			classify = false
		} else {
			classify = claimed
		}
	}
	if classify {
		r.metrics.Inc(metrics.MetricEventsReceived)
		if len(matchedFns) > 0 {
			r.metrics.Inc(metrics.MetricEventsMatched)
			// An app is "engaged" by an event when at least one of its
			// rules matches, regardless of whether the execution later fails:
			// an event matching two apps counts once per app here,
			// while MetricEventsMatched counts it once globally.
			for _, fnName := range matchedFns {
				r.metrics.IncLabels(metrics.MetricAppEventsMatched,
					[]metrics.Label{{Name: "app", Value: fnName}})
			}
		} else {
			r.metrics.Inc(metrics.MetricEventsUnmatched)
		}
	}

	for _, pf := range snapshot {
		if !pf.available {
			continue
		}
		rules := snap.matchingRules(pf, event)
		for _, rule := range rules {
			// The invocation identity is stable across restarts and config
			// reloads as long as the rule still exists: the app name and the
			// rule handler string. Renaming either invalidates old invocation
			// state — old entries simply never match, and the msg-level set of
			// required invocations is recomputed each delivery from current
			// templates, so a rule removed from the template no longer gates the
			// ACK.
			invocation := pf.fn.Name + "/" + rule.Handler
			if hasState && invState.IsComplete(invocation) {
				r.log.Debug("App handler: already succeeded for event; skipping",
					"app", pf.fn.Name,
					"handler", rule.Handler,
					"message_id", msgID,
					"delivery_attempt", deliveryAttempt,
				)
				continue
			}
			// Cap the rule timeout at the configured maximum (defense in depth;
			// template validation enforces the cap at load). This guarantees a
			// handler can never run longer than the stream layer's MaxRuleTimeout,
			// and the same capped value is what TryStart persists as the running
			// deadline, so the persisted deadline matches the local timer by
			// construction.
			timeout := rule.Timeout
			if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && timeout > cap {
				timeout = cap
			}
			// executeRule runs this single rule's invocation while holding the
			// worker-global and per-app concurrency slots (whose defer scope
			// is THIS call, so a multi-rule message never accumulates slots across
			// rules). It classifies this invocation's outcome for this delivery and
			// returns it plus the plain error (for a failed attempt) so the outer
			// loop can aggregate AFTER iterating every matching rule — a failure in
			// one handler never prevents later handlers from running.
			executeRule := func() (invocationOutcome, error) {
				// The per-invocation handler attempt/claim. With invocation
				// state it comes from TryStart (Redis-backed: the persisted
				// attempt advances at the confirmed claim, with a fresh opaque
				// token; execution and its metrics normally follow, but a
				// post-claim crash can spend a claim unexecuted). Without
				// invocation state there is no persisted handler attempt, so it
				// stays the zero claim (explicitly not attributed): the delivery
				// count is NOT reused as a handler attempt count, because
				// deliveries count redeliveries, not executions. The no-state path
				// never calls recordFailure, so it never fabricates an
				// exhaustion/DLQ attribution either.
				var claim stream.InvocationClaim
				// Reserve the worker-global and per-app concurrency slots
				// BEFORE TryStart, so a blocked invocation is never counted as an
				// attempt and does not persist state. reserveSlots acquires the
				// two semaphores sequentially, each with its own up-to-slotWait
				// wait, and warm admission can then add warmAdmitWait, so the
				// total wait may approach 60s + the warm wait — enough to overlap
				// MinPendingIdle and let a reclaim redeliver this still-waiting
				// message. That overlap is safe: TryStart is the only admission
				// point and runs AFTER capacity is reserved, and invocation state
				// prevents a concurrent second execution, so a reclaim cannot run
				// the invocation while this delivery still waits. On a timeout
				// this path claims nothing (pre-claim): it records no retry, ACKs
				// nothing, and leaves the message pending in the PEL for reclaim.
				// The slots are released via defer as soon as THIS invocation
				// finishes.
				releaseSlots, waited := r.reserveSlots(ctx, pf.fn.Name, pf.fn.Template.Concurrency)
				if releaseSlots == nil {
					// No slot freed in time: skip this invocation without marking
					// it failed, leave the message pending (reclaim replays it
					// later).
					skippedPending = true
					r.log.Debug("App handler: concurrency slot wait timed out; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				if waited {
					r.log.Debug("App handler: waiting for concurrency slot",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
				}
				defer releaseSlots()
				// Pre-admit the worker-global warm-container budget
				// (MAX_WARM_CONTAINERS) BEFORE TryStart, so a saturated warm
				// budget leaves the invocation pending (backpressure) without
				// claiming a handler attempt or charging a retry. The permit is
				// consumed by the container Execute creates (released on its
				// discard) or released unused here when Execute reuses an idle
				// container; the deferred Release below is the safety net for the
				// paths that do not reach Execute.
				warmCtx, warmOK := r.admitWarm(ctx)
				if !warmOK {
					skippedPending = true
					r.log.Debug("App handler: warm-container budget saturated; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				warmPermit := runtime.WarmPermitFrom(warmCtx)
				defer warmPermit.Release()
				// A lifecycle cancellation that landed while this invocation
				// waited for capacity must not claim an attempt: with no claim
				// there is no retry budget or exhaustion to charge, and the
				// message is left pending for a live consumer after restart
				// (satisfying "shutdown leaves messages pending, not counted as
				// attempts"). The concurrency slots and warm permit acquired
				// above are released by the deferred releases on this return
				// path. This check is deliberately BEFORE TryStart: a
				// cancellation that races an already-confirmed claim is
				// PRESERVED (the claim is spent, never rolled back) and handled
				// by recordFailure's cancellation gate below. Gated on hasState
				// because only the state-carrying path claims a persisted
				// attempt.
				if hasState && lifecycleCancelled(ctx) {
					skippedPending = true
					r.log.Debug("App handler: lifecycle canceled before claim; leaving pending without claiming an attempt",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
						"delivery_attempt", int(deliveryAttempt),
					)
					return outcomePendingSkip, nil
				}
				// Claim the invocation for this execution before running it.
				// TryStart atomically persists an absolute running deadline
				// (now + timeout), the attempt, and a fresh claim token, and
				// returns started=false when the invocation is already complete
				// (handled above), exhausted, or protected by an active attempt
				// deadline or a retry backoff — this or another replica may be
				// executing it, or it is waiting out its backoff, so we must not
				// run it concurrently. The IsComplete check above is the fast path
				// that avoids a script call on completed invocations; TryStart's
				// own read also covers "ok" and the same case, so the two are
				// consistent.
				if hasState {
					started, startClaim, wait, startErr := invState.TryStart(invocation, timeout)
					if startErr != nil {
						// The claim outcome is unknown (Redis/transport or
						// token-generation error): do NOT run the handler (an
						// ambiguous claim could race a replica that won the same
						// claim) and leave the message pending so a later delivery
						// retries the claim. No handler attempt was confirmed, so
						// the claim stays zero and no retry/exhaustion accounting
						// happens here. The returned error wraps
						// ErrInvocationNotEligible (the stream's pending/no-ACK
						// contract) plus the distinct ErrInvocationClaimUnconfirmed
						// sentinel so the aggregate can surface a claim failure
						// separately from an ordinary protected skip.
						skippedPending = true
						r.log.Warn("App handler: claim failed (outcome unknown); leaving pending without executing",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"error", startErr,
						)
						return outcomePendingSkip, fmt.Errorf("%w: %v: %w: %w",
							stream.ErrInvocationNotEligible, invocation,
							stream.ErrInvocationClaimUnconfirmed, startErr)
					}
					if !started {
						// The slot is released by the deferred releaseSlots before
						// the next rule acquires.
						if wait > 0 {
							// Protected by an active running deadline or a retry
							// backoff. The message must stay pending (the protected
							// invocation may still complete or fail on its own), so
							// this is a "not eligible" skip, not a completion.
							skippedPending = true
							r.log.Debug("App handler: not eligible for event (running or waiting for retry); leaving pending",
								"app", pf.fn.Name,
								"handler", rule.Handler,
								"message_id", msgID,
								"handler_attempt", startClaim.Attempt,
								"next_attempt_in", wait,
							)
							return outcomePendingSkip, nil
						}
						// Terminal (complete or exhausted): never execute again. An exhausted
						// invocation must still drive the message-level DLQ decision, so a redelivery
						// after a failed DLQ write or XACK routes to the DLQ again instead of being
						// acknowledged as complete.
						if startClaim.Attempt > 0 {
							anyExhausted = true
							exhaustedInvocations = append(exhaustedInvocations, stream.ExhaustedInvocation{
								App:      pf.fn.Name,
								Handler:  rule.Handler,
								Attempts: startClaim.Attempt,
							})
						}
						r.log.Debug("App handler: terminal for event; skipping",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", startClaim.Attempt,
						)
						return outcomeTerminalSkip, nil
					}
					claim = startClaim
				}
				handlerAttempt := claim.Attempt
				// The claimed attempt has begun: the TryStart above (or the
				// absence of invocation state, for direct callers) claimed it and
				// the slots are held. The persisted attempt advances at that
				// claim; execution normally follows immediately, but a crash (or
				// the create-time budget race) after the claim can leave it spent
				// unexecuted. This is where last_execution_at is stamped — once
				// per claimed attempt, retries included — and the skip branches
				// returned above never reach it.
				r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampExecution, time.Now().Unix())
				r.log.Debug("App rule: matched event",
					handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt)...,
				)
				eventJSON, err := json.Marshal(event)
				if err != nil {
					// The invocation was already claimed (deferred-released) but
					// will not execute: this is a failed attempt, so treat it as
					// such — schedule a retry (or exhaust), then CONTINUE to the
					// next matching rule so each independent invocation gets its own
					// failed attempt.
					if hasState {
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: marshal event: %w", pf.fn.Name, rule.Handler, err)
				}
				// Resolve the template's env values and secret references into the
				// per-invocation extra env, immediately before container creation.
				// Secret values are resolved per execution (never cached on
				// Prepared, never in the fingerprint), so rotating a secret value
				// never requires a rebuild. A resolution failure is a failed
				// attempt (the invocation was already claimed by TryStart), so it
				// flows through the same retry/exhaustion machinery as an
				// execution failure and, like marshal failures, does not
				// short-circuit the remaining rules. The error names the secret
				// REFERENCE only — never any value.
				extraEnv, err := r.resolveExtraEnv(ctx, pf.fn.Template)
				if err != nil {
					if hasState {
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					return outcomeRetryable, fmt.Errorf("app %q handler %q: %w", pf.fn.Name, rule.Handler, err)
				}
				invokeCtx, cancel := context.WithTimeout(ctx, timeout)
				// Carry the pre-admitted warm-budget permit into Execute so a
				// newly created container owns the counted slot. The permit is
				// released by Execute (unused, on a warm lease) or by the
				// container on discard; the deferred release above is the safety
				// net for paths that never reach Execute.
				if warmPermit != nil {
					invokeCtx = runtime.WithWarmPermit(invokeCtx, warmPermit)
				}
				// Stamp the invocation's diagnostic metadata into the context so
				// the executor can attach it as container labels. This keeps the
				// Executor interface (and every test fake) unchanged.
				eventID, eventName := eventFields(event)
				invokeCtx = runtime.WithRunMeta(invokeCtx, runtime.RunMeta{
					Type:      runtime.ContainerTypeEvent,
					App:       pf.fn.Name,
					Handler:   rule.Handler,
					MessageID: msgID,
					EventID:   eventID,
					EventName: eventName,
					Hostname:  r.hostname,
					Image:     toImage(pf),
				})
				start := time.Now()
				panicked, panicValue, err := r.runInvocation(pf, invokeCtx, cancel, rule.Handler, eventJSON, extraEnv, snap.pinFor(pf), invocationTrace{state: invState, invocation: invocation, attempt: handlerAttempt})
				elapsed := time.Since(start)
				if panicked {
					// A panicking execution is a misbehaving handler, not a
					// healthy failure: log the panic value and the full stack so
					// the bug is visible and attributable, then funnel it through
					// the SAME failure branch below (metrics + recordFailure) so
					// retry and exhaustion accounting stay per-invocation.
					r.log.Error("App handler: PANICKED for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"panic_value", fmt.Sprintf("%v", panicValue),
							"stack", string(debug.Stack()),
						)...,
					)
				}
				if warmSaturated(err) {
					// The container could not be created because the global warm
					// budget saturated at create time (a race between admission
					// and the actual create). This is BACKPRESSURE, not a handler
					// failure: no failure, retry, or DLQ accounting is charged and
					// the message stays pending. The attempt claimed by TryStart
					// above IS nonetheless spent — this is the accepted
					// claim-before-execute window (the same one a crash between the
					// claim and the handler start falls in): the claimed running
					// marker's deadline simply elapses, and a later delivery claims
					// the NEXT attempt. Pre-claim backpressure (a slot timeout or a
					// saturated warm-budget admission above) never reaches a claim
					// and so spends nothing. Nothing here exhausts: the eventual
					// DLQ still needs a later attempt to actually run and fail.
					skippedPending = true
					r.log.Debug("App handler: warm-container budget saturated at create; leaving pending",
						"app", pf.fn.Name,
						"handler", rule.Handler,
						"message_id", msgID,
					)
					return outcomePendingSkip, nil
				}
				if err != nil {
					r.metrics.IncLabels(metrics.MetricHandlerInvocations,
						[]metrics.Label{
							{Name: "outcome", Value: "failure"},
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						})
					// Unlabeled total for the PostgreSQL snapshot; the labeled counter
					// above stays for Prometheus.
					r.metrics.Inc(metrics.MetricHandlerFailure)
					// Per-app failure attribution (per rule execution). A
					// failed attempt that will retry still counts as a failure
					// (and as an execution above); only a DLQ-routed exhaustion
					// additionally sets last_dlq_at (see recordFailure).
					r.metrics.IncLabels(metrics.MetricFunctionHandlerFailure,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					r.metrics.SetAppTimestamp(pf.fn.Name, metrics.AppTimestampFailure, time.Now().Unix())
					r.metrics.ObserveDurationLabels(metrics.MetricHandlerDuration,
						[]metrics.Label{
							{Name: "app", Value: pf.fn.Name},
							{Name: "handler", Value: rule.Handler},
						}, elapsed)
					r.log.Warn("App handler: execution failed for event",
						append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
							"duration", elapsed,
							"reason", err,
						)...,
					)
					// Record the failure and decide retry vs exhaustion. This is
					// the per-invocation retry driver: a retryable failure
					// schedules a backoff and counts function_retries_total; an
					// exhausted attempt marks the invocation terminal and counts
					// function_dlq_total. The message-level DLQ decision (whether
					// EVERY matched invocation is terminal) is left to Handle's
					// end-of-loop aggregate, so an invocation may exhaust while
					// others still run.
					if hasState {
						return r.recordFailure(ctx, invState, invocation, claim, rule.Retries, pf.fn.Name, rule.Handler, msgID, err)
					}
					// No invocation state (direct callers/tests): every failure
					// counts as a retry driver, but there is no Redis-backed
					// attempt count to decide exhaustion — so no DLQ attribution
					// here either. The DLQ metric is only meaningful with
					// invocation state, where exhaustion is actually persisted
					// and observable.
					r.metrics.IncLabels(metrics.MetricFunctionRetries,
						[]metrics.Label{{Name: "app", Value: pf.fn.Name}})
					return outcomeRetryable, err
				}
				// Record the invocation as completed so a redelivery skips it.
				// This happens BEFORE the success metrics so a crash between the
				// side effect and MarkComplete re-runs the handler (at-least-once;
				// the handler must remain idempotent). The completion is CASed on
				// this attempt's claim; a refusal (the marker was re-claimed by a
				// newer token, or is already terminal exhausted) means this
				// delivery does not own the invocation's resolution, so treat it
				// as unresolved: leave the message pending (never ACK a
				// superseded claim's outcome) and let the newer claim or terminal
				// state drive the decision on a later delivery.
				if hasState {
					if !invState.MarkComplete(invocation, claim) {
						skippedPending = true
						r.log.Warn("App handler: completion superseded; leaving pending",
							"app", pf.fn.Name,
							"handler", rule.Handler,
							"message_id", msgID,
							"handler_attempt", handlerAttempt,
						)
						return outcomePendingSkip, nil
					}
				}
				r.recordHandlerSuccess(pf.fn.Name, rule.Handler, elapsed)
				r.log.Info("App handler: executed for event",
					append(handlerLogFields(hasState, pf.fn.Name, rule.Handler, msgID, handlerAttempt, deliveryAttempt),
						"duration", elapsed,
					)...,
				)
				return outcomeExecuted, nil
			}
			outcome, err := executeRule()
			if !hasState {
				// No invocation state (direct callers/tests): preserve the old
				// fail-fast behavior exactly — run every matching handler and
				// return the first failure's plain error immediately; the tail
				// handles the nothing-executed/not-eligible case. No invocation
				// wrapping or DLQ attribution when matched.
				if outcome == outcomeRetryable {
					return err
				}
				if outcome == outcomeExecuted {
					executed = true
				}
				continue
			}
			// Aggregate the per-invocation outcome for the message-level contract,
			// decided only after the whole loop (see Handle's doc comment). Keep
			// iterating regardless: each matching invocation gets its own attempt.
			switch outcome {
			case outcomePendingSkip:
				// Every pending skip (protected invocation, slot timeout, claim
				// failure, or a superseded stale transition) leaves the message
				// unresolved: it must never be ACKed. A claim failure is
				// additionally remembered via its distinguishable error so the
				// aggregate can surface its cause.
				skippedPending = true
				if err != nil && errors.Is(err, stream.ErrInvocationClaimUnconfirmed) && claimErr == nil {
					claimErr = err
				}
			case outcomeRetryable:
				if firstErr == nil {
					firstErr = err
				}
			case outcomeExhausted:
				anyExhausted = true
				// recordFailure returns a *stream.HandlerExhaustedError carrying
				// exactly one exhausted invocation; collect it so the aggregate
				// reports every exhausted app/handler with its own attempt
				// count. A non-typed error is ignored here (it cannot happen in
				// production) and the terminal-skip metadata still drives the
				// aggregate.
				var typed *stream.HandlerExhaustedError
				if errors.As(err, &typed) {
					exhaustedInvocations = append(exhaustedInvocations, typed.Invocations...)
				}
			}
		}
	}
	// Aggregate: decide the single message-level error from the per-invocation
	// outcomes collected across the whole rule loop.
	if hasState {
		// 0. An ambiguous claim failure (a TryStart store error) is surfaced
		//    first: the claim outcome is unknown, so the message stays pending
		//    with no handler execution and no retry accounting, and the distinct
		//    sentinel keeps the cause visible even when another invocation also
		//    failed this delivery.
		if claimErr != nil {
			return claimErr
		}
		// 1. Any retryable failure this delivery → the message stays pending
		//    (retryable): return the first such failure, even if other
		//    invocations succeeded or are otherwise still running.
		if firstErr != nil {
			return firstErr
		}
		// 2. A matched-but-unavailable invocation is unresolved (it matched and
		//    has not already completed) → the message stays pending and is NOT
		//    ACKed, and it is NOT dead-lettered solely for unavailability. No
		//    handler attempt was claimed and no handler counter was touched for
		//    it (no handler ran). Returning the retryable ErrAppUnavailable
		//    makes the stream leave the message pending (an ordinary retryable
		//    error), exactly like the schedule path's unavailable handling, so a
		//    later delivery — after the app is rebuilt and available again —
		//    completes the outstanding work. This is checked BEFORE the DLQ
		//    decision so an exhausted sibling can never dead-letter a message
		//    whose unavailable invocation is still outstanding.
		if len(unresolvedUnavailable) > 0 {
			return unavailableMatchError(unresolvedUnavailable)
		}
		// 3. Every matched invocation is terminal (complete or exhausted) AND at
		//    least one exhausted → the message is terminal; route it to the DLQ.
		//    allMatchedTerminal fails open to false on a read error, keeping the
		//    message pending rather than DLQ'ing it. The aggregate error carries
		//    EVERY exhausted invocation's exact app/handler/attempt metadata
		//    and wraps stream.ErrInvocationExhausted, so the stream layer writes
		//    one correctly-attributed DLQ entry per exhausted invocation.
		if anyExhausted && allMatchedTerminal(invState, matched) {
			return &stream.HandlerExhaustedError{Invocations: dedupeExhausted(exhaustedInvocations)}
		}
		// 4. Any matched invocation was protected- or slot-timeout-skipped
		//    (unresolved) → the message stays pending with NO retry accounting.
		//    This fires even when other invocations executed successfully this
		//    call: an unresolved invocation must not be ACKed away. A claim
		//    failure was already surfaced at step 0.
		if skippedPending {
			return stream.ErrInvocationNotEligible
		}
		// 5. Every matched invocation is complete (or nothing matched) → ACK.
		return nil
	}
	// No invocation state: preserve the old fail-fast tail — nil when something
	// executed successfully (or nothing matched); ErrInvocationNotEligible when
	// nothing executed and at least one invocation was protected- or
	// slot-timeout-skipped (so the stream leaves the message pending). A
	// matched-but-unavailable invocation is likewise unresolved: it is reported
	// (never silently treated as unmatched/complete) so the caller leaves the
	// message pending. With no state there is no terminal marker to consult, so
	// any unavailable match counts as unresolved.
	if len(unresolvedUnavailable) > 0 {
		return unavailableMatchError(unresolvedUnavailable)
	}
	if executed {
		return nil
	}
	if skippedPending {
		return stream.ErrInvocationNotEligible
	}
	return nil
}

// unavailableMatchError reports the matched-but-unavailable invocations of a
// message as a retryable error wrapping ErrAppUnavailable. An unavailable
// app's event is MATCHED (not unmatched) and must keep the message pending
// without being dead-lettered for unavailability alone, so the stream layer
// treats this exactly like any other retryable failure. The invocation IDs are
// included so the log line names the outstanding app/handler.
func unavailableMatchError(invocations []string) error {
	return fmt.Errorf("%w: matched but unavailable: %s", ErrAppUnavailable, strings.Join(invocations, ", "))
}

// dedupeExhausted returns the exhausted invocations deduplicated by
// "<app>/<handler>", preserving first-seen order. This is a defensive
// backstop: a parsed template rejects duplicate event handlers, so the same
// invocation should not appear twice in the aggregate, but hand-built
// templates and legacy aggregates may still carry duplicate metadata; the DLQ
// contract is one entry per exhausted invocation, so it is collapsed here.
func dedupeExhausted(invocations []stream.ExhaustedInvocation) []stream.ExhaustedInvocation {
	if len(invocations) < 2 {
		return invocations
	}
	seen := make(map[string]bool, len(invocations))
	out := make([]stream.ExhaustedInvocation, 0, len(invocations))
	for _, iv := range invocations {
		inv := iv.Invocation()
		if seen[inv] {
			continue
		}
		seen[inv] = true
		out = append(out, iv)
	}
	return out
}

// allMatchedTerminal reports whether every matched invocation is terminal
// (complete or exhausted). It is used to decide whether a message whose last
// failing invocation just exhausted has any runnable invocation left: if none,
// the message is terminal and must be routed to the DLQ. A read error fails
// open to false (not terminal), so the message is conservatively left pending.
func allMatchedTerminal(invState stream.InvocationState, matched []string) bool {
	for _, inv := range matched {
		if !invState.IsTerminal(inv) {
			return false
		}
	}
	return true
}
