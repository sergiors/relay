package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"relay/internal/app"
	"relay/internal/runtime"
	"relay/internal/schedule"
	"relay/internal/stream"
)

// obsoleteOccurrence is the terminal error returned when a schedule occurrence
// names an app or schedule that is no longer in the configuration. The
// stream layer recognizes ErrInvocationObsolete and ACKs the message: an
// obsolete occurrence is never retried or dead-lettered.
func obsoleteOccurrence(fnName, scheduleName string) error {
	return fmt.Errorf(
		"%w: schedule app %q schedule %q no longer in configuration",
		stream.ErrInvocationObsolete, fnName, scheduleName,
	)
}

// rejectAppAbsent disposes of a schedule occurrence whose app is ABSENT from
// THIS replica's registry. A local absence is NOT proof that the occurrence is
// obsolete: another replica may already have ADMITTED it (pinning a descriptor
// and claiming the invocation), and an app-absent replica cannot execute an
// adopted descriptor — there is no image or executor. The terminal obsolete
// rejection is therefore RESERVED atomically (stream.InvocationState
// ReserveScheduleRejection), which refuses it when anything was already admitted:
//
//   - reserved / already reserved: the reserved kind is authoritative. An
//     obsolete reservation is ACKed (never retried or dead-lettered); an invalid
//     one — reserved earlier, when the app was still present here — is
//     dead-lettered.
//   - an ACTIVE marker (a pinned descriptor whose invocation is running/backoff,
//     or a legacy no-descriptor active marker): the message stays PENDING
//     (ErrInvocationNotEligible) so the attempt is not cancelled. It is never
//     ACKed and never DLQ'd.
//   - an exhausted marker (pinned descriptor OR legacy no-descriptor): the
//     admitted invocation(s) already exhausted and still owe a per-invocation DLQ
//     entry, so a typed HandlerExhaustedError is returned — the stream persists
//     the missing entries (idempotently; an already-":dlq" marker is skipped) and
//     then ACKs.
//   - a pinned descriptor with no terminal marker, terminal-retained, or a "ok"
//     marker: a pinned no-marker admission stays PENDING; a completed invocation
//     (or a message already off the PEL) returns nil so the stream ACKs and
//     terminal-retains idempotently.
//
// A reservation error means the outcome is unknown: the message is left pending,
// exactly like an unconfirmed claim, so no rejection is dispositioned on an
// ambiguous result.
func (r *Runner) rejectAppAbsent(invState stream.InvocationState, fnName, scheduleName string) error {
	admission, err := invState.ReserveScheduleRejection(stream.ScheduleRejectionObsolete)
	if err != nil {
		r.log.Warn("Schedule: app absent; rejection reservation failed; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
			"error", err,
		)
		return fmt.Errorf("%w: %v: %w: %w",
			stream.ErrInvocationNotEligible, fnName,
			stream.ErrInvocationClaimUnconfirmed, err)
	}
	if admission.InFlight {
		// An ACTIVE admission already exists (a pinned descriptor admitted
		// elsewhere, or a legacy active marker with no readable descriptor). This
		// replica cannot resolve it, so keep the message pending: never reject,
		// never ACK/DLQ.
		r.log.Debug("Schedule: app absent here but the occurrence is admitted; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
		)
		return stream.ErrInvocationNotEligible
	}
	if len(admission.ExhaustedInvocations) > 0 {
		// An exhausted marker owns the message (a pinned descriptor whose
		// invocation exhausted before its DLQ write, or a legacy no-descriptor
		// exhausted marker): the admitted invocation(s) already exhausted and still
		// owe a per-invocation DLQ entry. Return the typed exhaustion error so the
		// stream persists the missing entries (idempotently — an already-":dlq"
		// marker is skipped) and then ACKs. This is never a rejection and never
		// leaves the disposition stuck.
		r.log.Warn("Schedule: app absent here; exhausted invocation owed a DLQ entry; routing to DLQ",
			"app", fnName,
			"schedule", scheduleName,
			"invocations", len(admission.ExhaustedInvocations),
		)
		return &stream.HandlerExhaustedError{Invocations: admission.ExhaustedInvocations}
	}
	if admission.Rejected {
		if admission.Rejection == stream.ScheduleRejectionInvalid {
			// Reserved invalid by an earlier delivery (when the app was still
			// present): terminal and non-retryable, so dead-letter it.
			r.log.Warn("Schedule: occurrence previously rejected as invalid; routing to DLQ",
				"app", fnName,
				"schedule", scheduleName,
			)
			return fmt.Errorf("%w: schedule app %q schedule %q was rejected as invalid before admission",
				stream.ErrScheduleInvalid, fnName, scheduleName)
		}
		r.log.Warn("Schedule: occurrence obsolete; app removed; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}
	// A zero admission is either terminal-retained (the message already left the
	// PEL) or a legacy no-descriptor "ok" marker (the admitted invocation already
	// succeeded). Return nil so the stream ACKs and terminal-retains idempotently.
	r.log.Debug("Schedule: app removed and occurrence already dispositioned; acknowledging",
		"app", fnName,
		"schedule", scheduleName,
	)
	return nil
}

// InvokeHandler executes a single schedule-occurrence invocation routed through
// the stream. Its admission boundary is deliberate and documented here:
//
// BEFORE ADMISSION (no descriptor pinned for the message): the occurrence
// resolves the CURRENT template by its STABLE schedule NAME on every delivery, so
// a handler/timeout/retries change under the same name takes effect and the
// name's current handler is what runs. Because an occurrence may block for a long
// time waiting for a concurrency slot, this config snapshot is refreshed from the
// registry AFTER the slot is admitted and immediately before the atomic first
// claim, so a reload that completed during the wait is observed. A missing NAME
// means the schedule was removed while the occurrence was pending; if nothing was
// ever pinned the occurrence is OBSOLETE, and — with invocation state — the
// obsolete rejection is RESERVED atomically (never a descriptor or attempt) and
// the stream ACKs it (never retried, never dead-lettered). A locally ABSENT app
// takes the same reserve-only path: the reservation is refused when another
// replica already admitted the occurrence, and the message then either stays
// PENDING (an active no-descriptor attempt) or completes the admitted
// invocation's real terminal disposition — a legacy "ok" marker is ACKed, and a
// legacy exhausted marker is dead-lettered (idempotently) then ACKed.
//
// AT ADMISSION (the first successful claim): the resolved descriptor — schedule
// name, handler, the CAPPED timeout, and the retry budget — is pinned ATOMICALLY
// with the claim (stream.InvocationState.TryStartScheduled), so a concurrent
// replica with a different template, or a reload racing the post-slot refresh,
// can never diverge: exactly one descriptor wins and every other delivery adopts
// it. Redis is therefore the global first-writer: a reload concurrent with that
// atomic first claim is treated as a competing proposal, and whichever descriptor
// the script persists is authoritative.
//
// AFTER ADMISSION (a descriptor is pinned): the descriptor is the single source
// of truth. The pinned handler, timeout, and retries are used EVEN IF the current
// schedule was renamed, retimed, or REMOVED, so an admitted invocation completes
// its retry/DLQ lifecycle instead of being cancelled by a config change. The
// schedule name remains the occurrence/dedup identity, but a pinned message no
// longer consults the current template for execution.
//
// The handler timeout is capped at the configured maximum exactly like Handle
// caps rule timeouts, and the CAPPED value is passed BOTH to admission (persisted
// as the running deadline) and to context.WithTimeout so the persisted deadline
// matches the local kill timer. The capped value is what the descriptor pins, so
// it stays stable across attempts.
//
// It reuses the exact event execution path: registry snapshot lookup, the global
// + per-app concurrency slots, per-invocation secret resolution, the panic
// boundary, and the same handler metrics. Invocation/DLQ attribution stays
// handler-based ("<app>/<handler>"); the descriptor adds schedule provenance
// only.
//
// When scheduleName is empty (the state-free DLQ-replay path, which knows only a
// recorded app/handler), resolution falls back to the first schedule entry
// matching the handler, then to that exact event rule (never event matching), and
// the no-state path executes a single attempt exactly as before.
//
// InvokeHandler takes only the app/schedule/handler strings; it does not carry
// the occurrence's scheduled instant, so it performs no semantic validation.
// The production stream entry point is InvokeOccurrence, which additionally
// validates the occurrence's scheduled_at against the current schedule before
// admission.
func (r *Runner) InvokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, fnName, scheduleName, handler, payload, nil)
}

// InvokeOccurrence executes one schedule occurrence routed through the stream.
// It is InvokeHandler's production entry point: the decoded Occurrence (app,
// stable schedule name, envelope handler, scheduled instant, and derived
// identity) is passed so that, BEFORE admission, the occurrence is semantically
// validated against the CURRENT template — the named app must be configured and
// available, the stable schedule name must exist, and scheduled_at must be a
// real firing of that schedule's current cron/timezone (see validateOccurrence).
//
// The envelope handler is NEVER the execution target: only the app/schedule
// name select the invocation, and the current template's handler is what runs.
// The envelope handler is carried for provenance/logging only, so a handler
// change under the same name remains a valid live change.
//
// Validation applies only to a not-yet-admitted occurrence. Once a descriptor
// is pinned (first successful admission), it is authoritative and the
// occurrence completes its lifecycle even if the schedule was since changed or
// removed (see InvokeHandler). The state-free path (no invocation state) keeps
// its legacy single-attempt behavior and does not validate.
func (r *Runner) InvokeOccurrence(
	ctx context.Context, msgID string, occ schedule.Occurrence, payload []byte,
) error {
	return r.invokeHandler(ctx, msgID, occ.App, occ.Schedule, occ.Handler, payload,
		func(tmpl *app.Template) error { return validateOccurrence(tmpl, occ) })
}

// invokeHandler is the shared implementation of InvokeHandler and
// InvokeOccurrence. validate, when non-nil, is the occurrence's pre-admission
// semantic validation, called with the CURRENT template once the app and
// schedule name have resolved and the app is available. It runs only for an
// unadmitted occurrence (no pinned descriptor) whose schedule name still exists;
// a missing name is left to the existing obsolete path, and an admitted
// occurrence is authoritative and never re-validated.
func (r *Runner) invokeHandler(
	ctx context.Context, msgID, fnName, scheduleName, handler string, payload []byte,
	validate func(tmpl *app.Template) error,
) error {
	// Invocation state (when present) distinguishes the production stream path
	// from direct callers/tests: obsolete-removal and descriptor pinning only
	// apply on the production path. See the availability checks below.
	invState, hasState := stream.InvocationStateFrom(ctx)

	// Find the app in the current registry. GetByName returns nil only when
	// the app is ABSENT (removed) — a present but unavailable app
	// returns a non-nil entry whose Prepared() is nil. These two cases must be
	// handled differently:
	//   - absent (removed): an occurrence for it is OBSOLETE and terminal (ACKed,
	//     never retried/DLQ'd) — but only on the production state-carrying path;
	//     direct callers/tests keep the legacy plain error. Because a local
	//     absence is NOT proof of removal (another replica may have ADMITTED the
	//     occurrence), the obsolete rejection is RESERVED atomically first
	//     (rejectAppAbsent), and an already-admitted invocation is completed
	//     instead of rejected: a pinned descriptor admitted elsewhere or a legacy
	//     ACTIVE no-descriptor marker leaves the message PENDING, a legacy "ok"
	//     marker is ACKed, and a legacy exhausted marker is dead-lettered
	//     (idempotently) then ACKed — this replica has no image or executor, so it
	//     never executes the adopted descriptor. (The schedule NAME, not the
	//     app, is what admission pins; a still-present app whose schedule was
	//     removed is NOT obsolete once admitted — see the descriptor path below.)
	//   - present but unavailable: temporary (build failed at startup/reconcile),
	//     retryable exactly as today.
	//
	// The lookup PINS the app's published image (a share of its publication
	// lease, acquired under the registry lock), held until this invocation
	// returns, so a concurrent retirement cannot remove the image between this
	// lookup and the handler's execution. An UNADMITTED schedule occurrence
	// re-reads the current entry after the slot wait below and swaps in that
	// entry's pin (see the refresh there); the closure releases whichever lease
	// `pin` holds at return, so the stale lookup's lease is not leaked.
	pf, pin := r.reg.getByNamePinned(fnName)
	defer func() { pin.Release() }()
	if pf == nil {
		if hasState {
			// The app is absent from THIS replica. A local absence is NOT proof
			// that the occurrence is obsolete: another replica may already have
			// ADMITTED it, so the terminal obsolete rejection must be RESERVED
			// atomically, and only when nothing was admitted. See rejectAppAbsent.
			return r.rejectAppAbsent(invState, fnName, scheduleName)
		}
		r.log.Warn("Schedule: app is not available", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the app
		// is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Reserve the worker-global and per-app concurrency slots BEFORE the
	// admission claim so a blocked invocation is never counted as an attempt and
	// does not persist state (same ordering as Handle's event path). A slot
	// timeout means the invocation is unresolved: with invocation state it
	// returns a "not eligible" skip (the stream leaves the message pending with no
	// retry accounting); without state it preserves the legacy plain error. The
	// reservation is made for both the state-free and state-carrying paths, so a
	// held app slot also bounds a direct/no-state caller.
	releaseSlots, _ := r.reserveSlots(ctx, fnName, pf.fn.Template.Concurrency)
	if releaseSlots == nil {
		r.log.Warn("Schedule: concurrency slot wait timed out", "app", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: concurrency slot wait timed out")
	}
	defer releaseSlots()

	// Pre-admit the worker-global warm-container budget (MAX_WARM_CONTAINERS)
	// before any handler attempt is claimed, so a saturated warm budget leaves
	// the occurrence pending (backpressure) instead of charging a retry. The
	// permit rides the invocation context into Execute.
	schedCtx, warmOK := r.admitWarm(ctx)
	if !warmOK {
		r.log.Debug("Schedule: warm-container budget saturated; leaving pending",
			"app", fnName, "handler", handler)
		if hasState {
			return stream.ErrInvocationNotEligible
		}
		return fmt.Errorf("schedule invocation: warm-container budget saturated")
	}
	defer runtime.WarmPermitFrom(schedCtx).Release()

	// The state-free path (direct callers/tests, DLQ replay) preserves the legacy
	// single-attempt behavior exactly: resolve the current schedule, cap the
	// timeout, and execute once. No descriptor, no broker lifecycle.
	if !hasState {
		desc := resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
			desc.Timeout = cap
		}
		return r.invokeOnce(schedCtx, pf, desc.Handler, payload, desc.Timeout, pin, nil, "", stream.InvocationClaim{}, msgID)
	}

	// Read the pinned schedule descriptor ONCE (a Redis read, and the
	// authoritative source for an already-admitted occurrence). A descriptor
	// pinned by an earlier first admission is immutable while the message is
	// recoverable, so this read is stable for the rest of the delivery.
	desc, known := invState.ScheduleDescriptor()
	// admitted records whether a descriptor was already pinned at the START of
	// this delivery. `known` is later reassigned when the current template can
	// propose a descriptor, so this is the stable "is this occurrence
	// already-admitted?" signal the pre-admission validation keys on.
	admitted := known

	// An UNADMITTED schedule occurrence (scheduleName != "" and no pinned
	// descriptor) resolves its template by the stable schedule NAME, and it may
	// have waited arbitrarily long for a concurrency slot while a reload
	// hot-swapped or removed the app. The lookup above is stale for that
	// occurrence: refresh it from the registry under the lock and pin the NEWEST
	// entry, dropping the stale pin, so the descriptor proposed for the atomic
	// first admission comes from the CURRENT config. A reload racing AFTER this
	// fresh snapshot is a concurrent proposal: the atomic descriptor write in
	// TryStartScheduled remains the global first-winner boundary. A pinned
	// descriptor is authoritative and is NEVER re-resolved (its admitted schedule
	// may already be gone), but the app's availability is still rechecked.
	if scheduleName != "" && !known {
		// getByNamePinned returns a FRESH share of the current entry's
		// publication lease (always a distinct lease object, or nil when the
		// entry has none), so release the stale lookup's pin and adopt the fresh
		// one unconditionally — even when fresh is nil, so an app removed
		// while the occurrence waited drops the stale pin with it.
		fresh, freshPin := r.reg.getByNamePinned(fnName)
		pin.Release()
		pf, pin = fresh, freshPin
	}
	if pf == nil {
		// The app was present at the stale lookup but is gone from the CURRENT
		// registry now, after the slot wait. Use the SAME absent-app disposition as
		// the initial lookup: reserve the obsolete rejection atomically, and only
		// when nothing was admitted (a replica with the app may have admitted it
		// while this one waited).
		return r.rejectAppAbsent(invState, fnName, scheduleName)
	}
	if pf.Prepared() == nil {
		// The app exists but its image could not be built yet: temporarily
		// unavailable, so the occurrence is retryable (not obsolete — the
		// app is still configured).
		r.log.Warn("Schedule: app is temporarily unavailable", "app", fnName)
		return fmt.Errorf("schedule invocation: app %q is not available", fnName)
	}

	// Resolve the descriptor that owns this message. A pinned descriptor (read
	// above) is authoritative and survives a later template change or the
	// schedule's removal; otherwise resolve the CURRENT template by schedule NAME
	// (the production path) or by handler (the state-carrying fallback).
	if !known {
		desc = resolveScheduleDescriptor(pf.fn.Template, scheduleName, handler)
		// The production path carries a schedule NAME. When the app is
		// present but that NAME is gone, the occurrence may have been admitted by
		// ANOTHER replica whose template still had it, so we must NOT pre-empt the
		// atomic admission: pass no descriptor (known=false) and let the script
		// hand back the pinned one to adopt, or report it obsolete.
		if scheduleName != "" && !scheduleNameExists(pf.fn.Template, scheduleName) {
			known = false
		} else {
			// A resolved name, or the state-carrying handler fallback: a descriptor
			// is known (a fallback with no matching entry keeps the defaults).
			known = true
		}
	}
	// Pre-admission semantic validation (schedule-occurrence entry point only):
	// the app is present and available, so validate the occurrence against the
	// CURRENT template — the stable name must resolve to a schedule and
	// scheduled_at must be a real firing of that schedule's current
	// cron/timezone. A MISSING name is deliberately a no-op here: the atomic
	// admission below already decides between adopting a descriptor pinned by
	// another replica and reporting the never-admitted occurrence obsolete. A
	// pinned (already-admitted) occurrence is authoritative and never
	// re-validated, so a schedule edited or removed after admission still
	// completes under its pinned contract.
	//
	// On failure the disposition is deliberately NOT decided here: the caller
	// proposes no descriptor (known=false) and the atomic admission below still
	// observes any descriptor a concurrent replica has pinned, so an admitted
	// occurrence is adopted rather than raced into an ACK (no ACK hazard). Only
	// an admission that finds nothing pinned AND no proposal is then reported as
	// invalid, exactly where a never-admitted occurrence is reported obsolete.
	var invalidErr error
	if validate != nil && !admitted {
		if invalidErr = validate(pf.fn.Template); invalidErr != nil {
			r.log.Warn("Schedule: invalid occurrence claim; deferring to admission",
				"app", fnName,
				"schedule", scheduleName,
				"reason", invalidErr,
			)
			known = false
		}
	}

	// The admission input is either a PROPOSAL (a resolved descriptor) or a
	// terminal REJECTION. When the occurrence is not admitted and the current
	// template lets us propose nothing, it is terminally rejected: invalid when
	// the schedule NAME exists but the occurrence can never be a firing of it,
	// obsolete when the NAME (or the app) is gone. The rejection is RESERVED
	// atomically at the admission boundary (stream.InvocationState.TryStartScheduled),
	// which also refuses it if a concurrent replica admitted the occurrence first —
	// so a rejection can never race a live admission into an ACK/DLQ (F-030). A
	// pinned descriptor (admitted==true) always proposes, so an admitted
	// occurrence is never rejected.
	rejection := stream.ScheduleRejectionNone
	if !known {
		if invalidErr != nil {
			rejection = stream.ScheduleRejectionInvalid
		} else {
			rejection = stream.ScheduleRejectionObsolete
		}
	}

	// Cap the timeout exactly like Handle caps each rule's timeout (defense in
	// depth; template validation enforces the cap at load). When the descriptor is
	// pinned, its timeout was already capped at admission, so re-capping is a
	// no-op that keeps a hand-built/oversized pinned value safe.
	if cap := time.Duration(r.maxHandlerTimeout.Load()); cap > 0 && desc.Timeout > cap {
		desc.Timeout = cap
	}

	// A lifecycle cancellation that landed while the occurrence waited for
	// capacity must not claim an attempt (same contract as Handle's event
	// path). The concurrency slots and warm permit acquired above are released
	// by the deferred releases on this return path. This is BEFORE the atomic
	// admission: a cancellation racing an already-confirmed claim is PRESERVED
	// (spent, never rolled back) and handled by recordFailure's cancellation
	// gate below. This branch is reached only on the state-carrying path, which
	// is the only path that claims a persisted attempt.
	if lifecycleCancelled(ctx) {
		r.log.Debug("Schedule: lifecycle canceled before claim; leaving pending without claiming an attempt",
			"app", fnName, "handler", desc.Handler)
		return stream.ErrInvocationNotEligible
	}

	// Atomically pin the descriptor (only when none is pinned) and claim the
	// invocation. A lost descriptor race is resolved inside the handle: the
	// pinned descriptor is adopted and the admission retried, so the winning
	// handler/timeout/retries is what executes and no second invocation field is
	// ever claimed.
	admission, err := invState.TryStartScheduled(desc, rejection, func(d stream.ScheduleDescriptor) string {
		return fnName + "/" + d.Handler
	})
	if err != nil {
		// The claim outcome is unknown (Redis/transport or token-generation
		// error): do NOT run the handler and leave the message pending so a later
		// delivery retries the claim. No handler attempt was confirmed. The error
		// wraps ErrInvocationNotEligible (the stream's pending/no-ACK contract)
		// plus ErrInvocationClaimUnconfirmed so the cause is distinguishable.
		r.log.Warn("Schedule: claim failed (outcome unknown); leaving pending without executing",
			"app", fnName,
			"handler", desc.Handler,
			"error", err,
		)
		return fmt.Errorf("%w: %v: %w: %w",
			stream.ErrInvocationNotEligible, fnName+"/"+desc.Handler,
			stream.ErrInvocationClaimUnconfirmed, err)
	}
	if admission.Rejected {
		// Nothing was ever pinned and none can be: the occurrence was terminally
		// rejected before admission. For the schedule-occurrence entry point that
		// can mean two distinct things, told apart by the RESERVED kind:
		//
		//   - invalid: the schedule NAME exists but the occurrence can never be a
		//     firing of it (a non-firing scheduled_at, or an unparseable current
		//     cron). The claim can never succeed, so it is dead-lettered.
		//   - obsolete: the NAME (or the app) is gone from the current template —
		//     an intentional removal, so the occurrence is ACKed and never retried
		//     or dead-lettered. Removal semantics are preserved exactly.
		//
		// The RESERVED kind is authoritative: a redelivery (or a delivery whose
		// current template has since changed) dispositions the occurrence exactly
		// as the delivery that reserved it decided. invalidErr, when present, is
		// the current delivery's detailed rejection message and is preferred for
		// the DLQ reason; on a redelivery it may be nil because the template
		// changed (or the occurrence was already reserved), so a generic invalid
		// error is returned instead.
		if admission.Rejection == stream.ScheduleRejectionInvalid {
			if invalidErr != nil {
				r.log.Warn("Schedule: occurrence invalid; routing to DLQ",
					"app", fnName,
					"schedule", scheduleName,
					"reason", invalidErr,
				)
				return invalidErr
			}
			r.log.Warn("Schedule: occurrence previously rejected as invalid; routing to DLQ",
				"app", fnName,
				"schedule", scheduleName,
			)
			return fmt.Errorf("%w: schedule app %q schedule %q was rejected as invalid before admission",
				stream.ErrScheduleInvalid, fnName, scheduleName)
		}
		r.log.Warn("Schedule: occurrence obsolete; schedule no longer in template; acknowledging",
			"app", fnName,
			"schedule", scheduleName,
		)
		return obsoleteOccurrence(fnName, scheduleName)
	}
	if admission.InFlight {
		// An ACTIVE admission already exists but this caller must not act on it (a
		// no-descriptor running/backoff marker, or a pinned descriptor with no
		// terminal marker, detected by the rejection boundary). Keep the message
		// pending so the attempt is not cancelled: never ACK, never DLQ.
		r.log.Debug("Schedule: admission in flight without an adoptable descriptor; leaving pending",
			"app", fnName,
			"schedule", scheduleName,
		)
		return stream.ErrInvocationNotEligible
	}
	if len(admission.ExhaustedInvocations) > 0 {
		// An exhausted marker owns the message (the schedule NAME was removed but
		// the occurrence was admitted — under a pinned descriptor or the legacy
		// pre-pinning path). The admitted invocation(s) still owe a per-invocation
		// DLQ entry, so return the typed exhaustion error: the stream persists the
		// missing entries (idempotently) and then ACKs.
		r.log.Warn("Schedule: exhausted invocation owed a DLQ entry; routing to DLQ",
			"app", fnName,
			"schedule", scheduleName,
			"invocations", len(admission.ExhaustedInvocations),
		)
		return &stream.HandlerExhaustedError{Invocations: admission.ExhaustedInvocations}
	}

	// The descriptor that owns the message (adopted on a lost race, else our own).
	ed := admission.Descriptor
	handler = ed.Handler
	timeout := ed.Timeout
	retries := ed.Retries
	invocation := fnName + "/" + handler

	if !admission.Started {
		if admission.Wait > 0 {
			// Protected by an active running deadline or a retry backoff. The
			// message must stay pending: the protected invocation may still
			// complete or fail on its own, so this is a "not eligible" skip, never
			// an ACK.
			r.log.Debug("Schedule: invocation not eligible (running or waiting for retry); leaving pending",
				"app", fnName,
				"handler", handler,
				"handler_attempt", admission.Claim.Attempt,
				"next_attempt_in", admission.Wait,
			)
			return stream.ErrInvocationNotEligible
		}
		// Terminal. Attempt 0 is a complete (or terminal-retained) invocation, so
		// the message is ACKed. A positive attempt is an exhausted invocation: a
		// schedule has exactly ONE invocation, so the message is terminal and must
		// route to the DLQ, with the persisted exhausted attempt carried on the
		// typed error for DLQ attribution.
		if admission.Claim.Attempt == 0 {
			r.log.Debug("Schedule: invocation already succeeded; skipping (stream ACKs)",
				"app", fnName,
				"handler", handler,
			)
			return nil
		}
		r.log.Debug("Schedule: invocation terminal (exhausted); routing to DLQ",
			"app", fnName,
			"handler", handler,
			"handler_attempt", admission.Claim.Attempt,
		)
		return &stream.HandlerExhaustedError{
			Invocations: []stream.ExhaustedInvocation{{
				App:      fnName,
				Handler:  handler,
				Attempts: admission.Claim.Attempt,
			}},
		}
	}

	err = r.invokeOnce(schedCtx, pf, handler, payload, timeout, pin, invState, invocation, admission.Claim, msgID)
	if err != nil {
		// A stale completion (the claim was superseded or the marker is already
		// terminal) is not a handler failure: do NOT ACK a superseded claim's
		// outcome — leave the message pending so a later delivery resolves it.
		if errors.Is(err, stream.ErrInvocationNotEligible) {
			return err
		}
		// A create-time warm-budget saturation is BACKPRESSURE, not a handler
		// failure: leave the occurrence pending (a later delivery retries the
		// claim) without charging a retry, a DLQ entry, or handler-failure
		// telemetry. As on the event path, this
		// race happens after the confirmed admission, so the attempt it claimed
		// is spent even though nothing is charged (see Handle's create-time
		// saturation branch).
		if warmSaturated(err) {
			r.log.Debug("Schedule: warm-container budget saturated at create; leaving pending",
				"app", fnName,
				"handler", handler,
			)
			return stream.ErrInvocationNotEligible
		}
		// A failed attempt — resolve extra env, marshal, execution, or timeout
		// failures all land here. recordFailure decides retry vs exhaustion using
		// the ADMITTED descriptor's retry budget: a retryable failure schedules a
		// backoff and returns a plain error (the stream leaves the message pending,
		// gated by next_attempt_at); an exhausted attempt marks the invocation
		// terminal and returns a typed *stream.HandlerExhaustedError (which already
		// wraps stream.ErrInvocationExhausted and carries the exhausted handler
		// attempt). A schedule has exactly ONE invocation (this one), so the message
		// is terminal and the stream routes it to the DLQ.
		outcome, retErr := r.recordFailure(ctx, invState, invocation, admission.Claim, retries, fnName, handler, msgID, err)
		if outcome == outcomePendingSkip {
			// A stale transition (the claim was superseded, or the marker is
			// already terminal): do NOT ACK a superseded claim's outcome — leave
			// the message pending so a later delivery resolves it.
			return stream.ErrInvocationNotEligible
		}
		return retErr
	}
	return nil
}

// resolveScheduleDescriptor resolves a schedule occurrence's descriptor from the
// app's CURRENT template. When scheduleName is non-empty the schedule is
// resolved by its STABLE NAME (the production identity); a handler change under
// the same name is picked up, and a missing NAME leaves the envelope handler with
// the defaults (the caller decides whether that is obsolete). When scheduleName
// is empty (the state-free DLQ-replay path) it resolves by exact handler: first
// among the current schedules, then among event rules. This is never event
// matching — it selects by handler string alone, so no other rule can run. The
// returned descriptor's Timeout is NOT capped here; the caller caps it (and the
// capped value is what gets pinned).
func resolveScheduleDescriptor(tmpl *app.Template, scheduleName, handler string) stream.ScheduleDescriptor {
	desc := stream.ScheduleDescriptor{
		Handler: handler,
		Timeout: app.DefaultTimeout,
		Retries: app.DefaultRetries,
	}
	found := false
	if scheduleName != "" {
		for _, sch := range tmpl.Schedules {
			if sch.Name == scheduleName {
				desc.Schedule = scheduleName
				desc.Handler = sch.Handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, sch := range tmpl.Schedules {
			if sch.Handler == handler {
				desc.Handler = handler
				desc.Timeout = sch.Timeout
				desc.Retries = sch.Retries
				found = true
				break
			}
		}
	}
	if !found {
		for _, rule := range tmpl.Events {
			if rule.Handler == handler {
				if rule.Timeout > 0 {
					desc.Timeout = rule.Timeout
				}
				desc.Retries = rule.Retries
				break
			}
		}
	}
	return desc
}

// scheduleNameExists reports whether the template still declares a schedule with
// the given stable name. The production path uses it only to decide whether a
// first-time (unpinned) occurrence can be admitted locally, never to cancel an
// already-admitted one.
func scheduleNameExists(tmpl *app.Template, scheduleName string) bool {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return true
		}
	}
	return false
}

// scheduleByName returns the template's schedule with the given stable NAME, and
// whether it exists.
func scheduleByName(tmpl *app.Template, scheduleName string) (app.Schedule, bool) {
	for _, sch := range tmpl.Schedules {
		if sch.Name == scheduleName {
			return sch, true
		}
	}
	return app.Schedule{}, false
}

// validateOccurrence is the schedule occurrence's pre-admission semantic
// validation against the CURRENT template. It is called only once the app is
// present and available and no descriptor has been pinned (an admitted
// occurrence is authoritative and never re-validated).
//
// It enforces two invariants, using the schedule's stable NAME and the shared
// firing primitive:
//
//   - The name must resolve to a currently-configured schedule. A name that is
//     gone is NOT rejected here: returning nil lets the existing admission path
//     decide between adopting a descriptor pinned by another replica and
//     reporting the never-admitted occurrence OBSOLETE (ACKed, never retried or
//     dead-lettered). This keeps removal semantics exactly as they were.
//   - scheduled_at must be a real firing of that schedule's current cron in its
//     effective timezone (see schedule.IsFiring), so timezone/DST handling is
//     the parser's, not a re-implementation. A schedule whose own cron cannot be
//     parsed is a corrupt configuration: it is surfaced as invalid (a bounded,
//     non-retryable DLQ) rather than silently accepted, because no instant can
//     be a firing of an unparseable schedule.
//
// The envelope handler plays no part: the schedule is selected by name and its
// current handler is the execution target, so a handler change under the same
// name stays valid.
func validateOccurrence(tmpl *app.Template, occ schedule.Occurrence) error {
	sch, ok := scheduleByName(tmpl, occ.Schedule)
	if !ok {
		// Missing name: left to the admission path's obsolete decision.
		return nil
	}
	if sch.Location == nil {
		return fmt.Errorf("%w: app %q schedule %q has no timezone",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule)
	}
	firing, err := schedule.IsFiring(sch.Cron, sch.Location, occ.ScheduledAt)
	if err != nil {
		return fmt.Errorf("%w: app %q schedule %q has an unparseable cron %q: %v",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule, sch.Cron, err)
	}
	if !firing {
		return fmt.Errorf("%w: app %q schedule %q scheduled_at %s is not a firing of cron %q in %s",
			stream.ErrScheduleInvalid, occ.App, occ.Schedule,
			occ.ScheduledAt.UTC().Format(time.RFC3339), sch.Cron, sch.Location)
	}
	return nil
}
