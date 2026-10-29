package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// InvocationState is the read/write view of a single message's invocation
// state, carried in the delivery context so the runner can decide whether to
// execute an invocation. The invocation argument is the full
// "<app>/<handler>" ID; the stream layer never parses it.
//
// The lifecycle of a single invocation's field value (deadlines are integer
// Unix milliseconds; tokens are opaque claim identities):
//
//	attempt starts  → "running:<deadline_ms>:<n>:<token>"   (TryStart, atomic claim)
//	completes       → "ok"                                  (MarkComplete, CASed on the claim)
//	fails (retry)   → "next_attempt_at:<deadline_ms>:<n>:<token>" (RecordFailure, same claim)
//	exhausted       → "exhausted:<n>:<token>"               (MarkExhausted, CASed on the claim; the stream layer validates it before the PostgreSQL DLQ write)
//
// A crash mid-attempt leaves "running:<deadline_ms>:<n>:<token>", whose deadline
// simply elapses (the persistent hash keeps the marker; nothing expires out of
// Redis); recovery waits it out (bounded staleness of at most one timeout) and
// then reclaims it as attempt n+1 with a FRESH token. A failed attempt's
// "next_attempt_at" marker behaves the same way: if the worker crashes before
// the message is reclaimed, the marker's deadline elapses and the invocation
// becomes eligible again. Terminal "ok" and exhausted markers are never
// re-opened, and every transition that originates from an active claim CASes
// BOTH the attempt and the token, so a stale owner can never overwrite a newer
// claim's marker even if the attempt count happens to match.
//
// Lifetime: none of these writes sets a TTL. While the message is recoverable
// (still in the PEL) the hash is PERSISTENT, so a redelivery always observes the
// marker regardless of how long the message sat pending. Only after the message
// leaves the PEL does the stream layer switch the hash to terminal retention
// (the configured invocation retention) via the reserved terminal marker.
type InvocationState interface {
	IsComplete(invocation string) bool
	// MarkComplete records the invocation complete, CASed on the claim returned
	// by the successful TryStart. It returns true when the invocation is
	// complete (written, or already "ok"), false when the claim is stale (a newer
	// claim owns the marker) or the marker is terminal exhausted (success must
	// never downgrade an exhaustion). A false result is NOT an error: the caller
	// treats the invocation as unresolved (leave the message pending) rather than
	// resolve it on a superseded claim's behalf.
	MarkComplete(invocation string, claim InvocationClaim) bool
	// TryStart claims the invocation for a new execution. It returns started
	// true (with the confirmed InvocationClaim) when the caller should execute,
	// false otherwise. When not started, claim.Attempt is the existing marker's
	// attempt (0 for complete, the exhausted attempt for exhausted) and wait is
	// the duration until the invocation becomes eligible again: wait > 0 means it
	// is protected by an active running deadline or a retry backoff (this or
	// another replica), and wait == 0 means it is terminal (complete or
	// exhausted) and will never be eligible again.
	//
	// It is the persisted handler-attempt boundary: the runner calls it only after
	// the concurrency and warm-budget capacity gates, so a pre-claim capacity
	// rejection spends no attempt. A crash after a confirmed claim but before the
	// caller executes leaves the running marker, whose deadline elapses and yields
	// the NEXT attempt on a later delivery — so the claimed attempt is spent even
	// though the handler never ran (the accepted at-least-once window). TryStart
	// CLAIMS but does not EXHAUST: it never writes a terminal marker and has no
	// retry budget, so a crash alone cannot dead-letter; the exhausted marker is
	// written only by MarkExhausted after a later real failed attempt.
	//
	// An error means the claim outcome is UNKNOWN (a Redis/transport error, or a
	// failure to generate the claim token): the caller MUST leave the message
	// pending and MUST NOT execute the handler on this delivery. It is
	// deliberately not failed open, because an ambiguous claim could run a
	// duplicate of an invocation another replica just claimed.
	TryStart(invocation string, timeout time.Duration) (started bool, claim InvocationClaim, wait time.Duration, err error)
	// ScheduleDescriptor returns the immutable admission descriptor pinned for
	// this message's schedule occurrence, or ok=false when none is pinned yet.
	// While no descriptor is pinned the occurrence resolves the CURRENT template
	// by schedule NAME on every delivery; once pinned the descriptor is the
	// single source of truth (handler, capped timeout, retries) and survives the
	// schedule's removal. A read error fails open to (zero, false): the caller
	// then resolves the current template, and the atomic admission below still
	// guarantees a single winning descriptor.
	ScheduleDescriptor() (ScheduleDescriptor, bool)
	// TryStartScheduled is the schedule-occurrence admission boundary. It
	// atomically pins the caller's proposed descriptor (only when none is pinned)
	// and claims the invocation, so the descriptor, the attempt, and the claim
	// token become visible together and exactly one of two racing proposals wins.
	//
	// rejection==ScheduleRejectionNone means the caller has a PROPOSAL (desc
	// resolved from the CURRENT template by schedule NAME). A non-none rejection
	// (obsolete or invalid) means the caller can PROPOSE NOTHING and the
	// occurrence is terminally rejected: desc is ignored, and the rejection is
	// atomically RESERVED in the invocation-state hash before it is reported — so
	// a rejection can never race a concurrent admission (F-030). If the occurrence
	// was already ADMITTED (another replica pinned a descriptor first), the
	// rejection is refused and the pinned descriptor adopted, so an active
	// admission always beats a rejection.
	//
	// invocationFor maps a descriptor to its "<app>/<handler>" invocation ID: on a
	// lost race the winning descriptor owns the message and its handler (not the
	// caller's) is invoked, so the callback is re-evaluated for the adopted
	// descriptor. The loop is bounded and terminates because a pinned descriptor is
	// immutable while the message is recoverable.
	//
	// It returns a ScheduleAdmission. Rejected is true only when the occurrence was
	// terminally rejected before admission (nothing was ever pinned and nothing
	// ever will be); Rejection distinguishes obsolete (ACK) from invalid (DLQ).
	// InFlight is true when an ACTIVE no-descriptor admission exists but this
	// caller must not act on it (keep the message pending), and
	// ExhaustedInvocations is set when a legacy no-descriptor exhausted marker
	// must be dead-lettered (the caller routes the missing DLQ entries then the
	// stream ACKs). An error is a Redis/transport failure (or token-generation
	// failure): the claim outcome is unknown, so the caller leaves the message
	// pending and does not execute.
	TryStartScheduled(desc ScheduleDescriptor, rejection ScheduleRejection, invocationFor func(ScheduleDescriptor) string) (ScheduleAdmission, error)
	// ReserveScheduleRejection is the RESERVE-ONLY pre-admission rejection
	// boundary for a replica that can neither propose a descriptor nor execute an
	// adopted one (its app is absent from the registry). Unlike
	// TryStartScheduled, it NEVER claims or starts an invocation: it only reserves
	// the caller's terminal rejection, atomically and exactly once, and refuses it
	// when the occurrence was already admitted.
	//
	// It returns a ScheduleAdmission with Rejected true (Rejection is the
	// authoritative reserved kind: the caller's, or the one an earlier delivery
	// stored) when the reservation is in force; a zero admission when the hash is
	// terminal-retained OR a terminal "ok" marker is present (the admitted
	// invocation already succeeded: the caller ACKs idempotently); InFlight true
	// when an ACTIVE marker exists, or when a descriptor is pinned with no
	// terminal marker (another replica admitted and may still be running it), so
	// the caller keeps the message PENDING; and ExhaustedInvocations set when an
	// exhausted marker owns the message (pinned descriptor or legacy no-descriptor
	// alike), so the caller routes each listed invocation's missing DLQ entry
	// (idempotently) and the message is then ACKed. An error means the reservation
	// outcome is UNKNOWN: the caller leaves the message pending and dispositions
	// nothing.
	ReserveScheduleRejection(kind ScheduleRejection) (ScheduleAdmission, error)
	// RecordFailure persists a failed attempt's retry backoff so the invocation
	// is gated until now+backoff. The claim is the one this caller confirmed via
	// TryStart; the store CASes attempt+token against the active marker so a
	// stale owner cannot overwrite a newer claim's marker. It returns true when
	// the backoff marker was written; a false result means the claim is stale or
	// the marker is terminal (an explicit result, NOT an error), in which case the
	// message still stays pending for a later delivery to resolve. A write error
	// is logged only: if the marker is lost, the invocation becomes eligible
	// immediately (at-least-once).
	RecordFailure(invocation string, claim InvocationClaim, backoff time.Duration) bool
	// MarkExhausted records that the invocation's attempts are exhausted, making
	// it terminal (skipped like complete on redelivery), CASed on the claim
	// confirmed by the successful TryStart. It returns true when the terminal
	// marker was written; false when the claim is stale or the marker is already
	// terminal (an explicit result, NOT an error), so the caller can refuse a
	// superseded exhaustion and wait for a later delivery instead of
	// dead-lettering on a stale claim's behalf.
	MarkExhausted(invocation string, claim InvocationClaim) bool
	// IsTerminal reports whether the invocation is terminal: complete or
	// exhausted (never eligible again). It is a read-only check the runner uses
	// to decide whether a message whose last failing invocation just exhausted
	// has any runnable invocation left (if none, the message is routed to the
	// DLQ). A Redis read error fails open to false (not terminal), so the
	// message is conservatively left pending rather than DLQ'd.
	IsTerminal(invocation string) bool
	// ClaimClassification atomically claims this message's one-time logical
	// event classification. It returns true only for the first delivery
	// (across redeliveries and replicas) of the message; every later delivery
	// returns false. A store error returns (false, err) so the caller counts
	// nothing rather than risk a double count.
	ClaimClassification() (bool, error)
	// TraceReference returns the compact trace lineage (traceparent[|tracestate])
	// recorded for the invocation by its most recent attempt, or "" when none
	// was ever recorded. It is a best-effort durable reference used to link a
	// retry's function.invoke span back to the previous attempt; a read error
	// or an unknown/malformed value degrades to no reference, never an error.
	TraceReference(invocation string) string
	// RecordTrace persists the compact trace lineage of the invocation's current
	// attempt so a later retry (even on a restarted worker) can link back to it.
	// It is best-effort: a write error is logged and never fails the invocation,
	// and an empty lineage is a no-op. Only the trace identity and tracestate
	// are ever persisted — never baggage.
	RecordTrace(invocation, lineage string)
}

// invocationStateContextKey is the context key carrying the per-message
// InvocationState into the Handler. It is unexported so the stream package owns
// the contract; the runner reads it best-effort via InvocationStateFrom.
type invocationStateContextKey struct{}

// ErrInvocationNotEligible is returned (wrapped) by the runner's Handle when at
// least one matched invocation was skipped because it is protected by an active
// running deadline or a retry backoff (this or another replica), and no
// invocation failed. The stream layer treats it as "leave the message pending
// without counting a retry or routing to the DLQ": the protected invocation may
// still complete or fail on its own, so the message must not be acknowledged.
// This is what fixes the cross-replica ACK hazard: a replica that reclaims a
// message whose invocation is still in flight on another replica must not ACK
// it.
var ErrInvocationNotEligible = errors.New("invocation not eligible")

// ErrInvocationClaimUnconfirmed is returned (wrapped together with
// ErrInvocationNotEligible) by the runner when an invocation's claim could not
// be confirmed because the invocation-state store failed: the claim outcome is
// genuinely unknown, so the message must stay pending and the handler must NOT
// run on that delivery. It exists to make a claim failure distinguishable (for
// logs and tests) from the ordinary protected-skip signal, while preserving the
// exact same pending/no-ACK contract via the wrapped ErrInvocationNotEligible.
var ErrInvocationClaimUnconfirmed = errors.New("invocation claim unconfirmed")

// ErrInvocationExhausted is returned (wrapped) by the runner's Handle when a
// failing invocation's attempts are exhausted AND every other matched invocation
// is complete or also exhausted, so the message is terminal and must be routed
// to the DLQ. The stream layer routes the whole message to the DLQ; every
// exhausted invocation gets its own DLQ entry (see HandlerExhaustedError).
var ErrInvocationExhausted = errors.New("invocation exhausted")

// ExhaustedInvocation identifies one terminal exhausted invocation: the exact
// app and handler, the handler attempt that exhausted (the 1+retries bound
// reached, sourced from the invocation retry state, never the Redis delivery
// count), and the underlying failure cause when it is known.
//
// It is the unit of per-invocation DLQ attribution: the stream writes one DLQ
// entry per ExhaustedInvocation, so a message matching several apps or
// handlers that all exhaust produces one entry each, with exact metadata.
type ExhaustedInvocation struct {
	// App is the exact app name.
	App string
	// Handler is the exact handler string ("module.function").
	Handler string
	// Attempts is the 1-based handler attempt that exhausted. It is always >= 1
	// for a real exhausted invocation.
	Attempts int
	// Err is the underlying failure that exhausted this invocation, when known.
	// It is nil on a terminal-skip redelivery, where the invocation was already
	// marked exhausted by an earlier delivery and its original cause is no
	// longer available.
	Err error
}

// Invocation returns the "<app>/<handler>" invocation ID used by the
// per-message invocation-state hash.
func (e ExhaustedInvocation) Invocation() string {
	return e.App + "/" + e.Handler
}

// Reason returns the human-readable exhaustion reason for this invocation,
// naming the exact app/handler and attempt count so the DLQ `reason`
// field stays consistent with the entry's `handler_attempts` and metadata.
func (e ExhaustedInvocation) Reason() string {
	reason := fmt.Sprintf("app %q handler %q exhausted after %d handler attempts",
		e.App, e.Handler, e.Attempts)
	if e.Err != nil {
		reason += ": " + e.Err.Error()
	}
	return reason
}

// HandlerExhaustedError is the runner's terminal exhaustion signal. It wraps
// ErrInvocationExhausted (so errors.Is keeps matching) and carries the full set
// of exhausted invocations for the message, each with its exact app,
// handler, and exhausted handler attempt. Handle aggregates EVERY exhausted
// matched invocation (both those that exhausted on this delivery and those
// already marked exhausted on a previous delivery), so a multi-app or
// multi-handler message dead-letters each invocation individually with correct
// metadata.
//
// The stream layer extracts Invocations when it dead-letters the message so each
// DLQ entry's handler_attempts is attributed from the handler retry state, never
// from the Redis delivery count. That distinction matters because a handler
// attempt advances on each admitted TryStart claim — normally matching a real
// execution, but a post-claim crash can spend a claim unexecuted, so the
// persisted count can exceed the real execution count — while a message can be
// reclaimed (delivered) many times without claiming a new attempt, so
// deliveries >= handler_attempts and the delivery count stays diagnostic only.
type HandlerExhaustedError struct {
	// Invocations is the exhausted invocation set. It is non-empty on this
	// error in production; an empty set degrades to the bare sentinel reason.
	Invocations []ExhaustedInvocation
}

// Error reports the exhaustion reason in the stable, human-readable form the
// DLQ `reason` field uses: the ErrInvocationExhausted sentinel followed by the
// per-invocation exhaustion message(s). A single invocation reads
// `invocation exhausted: app "fn" handler "h" exhausted after 5 handler
// attempts: ...`; multiple invocations are joined. The sentinel is emitted
// exactly once.
func (e *HandlerExhaustedError) Error() string {
	switch len(e.Invocations) {
	case 0:
		return ErrInvocationExhausted.Error()
	case 1:
		return ErrInvocationExhausted.Error() + ": " + e.Invocations[0].Reason()
	default:
		parts := make([]string, 0, len(e.Invocations))
		for _, iv := range e.Invocations {
			parts = append(parts, iv.Reason())
		}
		return fmt.Sprintf("%s: %d invocations exhausted: %s",
			ErrInvocationExhausted, len(e.Invocations), strings.Join(parts, "; "))
	}
}

// Unwrap returns ErrInvocationExhausted (so errors.Is(err,
// ErrInvocationExhausted) keeps matching) plus each invocation's underlying
// cause, preserving the full error chain for callers that inspect the causes.
func (e *HandlerExhaustedError) Unwrap() []error {
	errs := []error{ErrInvocationExhausted}
	for _, iv := range e.Invocations {
		if iv.Err != nil {
			errs = append(errs, iv.Err)
		}
	}
	return errs
}

// ErrInvocationObsolete is returned (wrapped) by the runner when an invocation
// no longer exists in the current app/template configuration — the
// app or its schedule entry/handler was removed while the message was
// pending. Obsolete invocations are terminal and MUST NOT be retried or
// routed to the DLQ: their removal was an intentional configuration change,
// so the stream layer acknowledges the message instead.
var ErrInvocationObsolete = errors.New("invocation obsolete")

// ErrScheduleInvalid is returned (wrapped) by the runner when a schedule
// occurrence is well-formed as an envelope but is semantically invalid against
// the CURRENT template: the named app and schedule exist and are available, but
// scheduled_at is not a real firing of the schedule's current cron definition
// (including timezone/DST), or the schedule's cron could not be parsed. Such a
// claim can never succeed, so it is terminal and non-retryable: the stream
// routes the message to the DLQ (never a retry, never a silent ACK-as-success).
// It is deliberately distinct from ErrInvocationObsolete (removed configuration,
// ACKed) and from a temporary/unavailable failure (retried).
var ErrScheduleInvalid = errors.New("schedule occurrence invalid")

// WithInvocationState returns a child of ctx carrying the per-message
// InvocationState. The stream layer sets this before invoking the Handler so
// the runner can skip already-completed or in-flight invocations without
// changing the Handler signature.
func WithInvocationState(ctx context.Context, state InvocationState) context.Context {
	return context.WithValue(ctx, invocationStateContextKey{}, state)
}

// noInvocationStateKey is a marker value that masks any InvocationState already
// carried by a context. It lets a caller explicitly opt OUT of the broker
// lifecycle for one invocation (e.g. a DLQ replay), which context.WithValue
// cannot otherwise express: there is no way to remove another package's context
// value.
type noInvocationStateKey struct{}

// WithoutInvocationState returns a child of ctx that reports no InvocationState
// from InvocationStateFrom, even if an ancestor carried one. It is the explicit
// "this invocation must not participate in the broker lifecycle" opt-out: the
// runner's DLQ replay uses it so InvokeHandler always takes its state-free
// single-attempt path, regardless of the caller's context.
func WithoutInvocationState(ctx context.Context) context.Context {
	return context.WithValue(ctx, noInvocationStateKey{}, true)
}

// InvocationStateFrom returns the InvocationState carried in ctx, or (nil,
// false) if absent. A context marked by WithoutInvocationState always reports
// (nil, false), masking any state inherited from an ancestor. Callers must treat
// the value as best-effort context: it never panics and returns false when no
// invocation state was injected (e.g. when the runner is driven directly in
// tests).
func InvocationStateFrom(ctx context.Context) (InvocationState, bool) {
	if ctx.Value(noInvocationStateKey{}) != nil {
		return nil, false
	}
	invState, ok := ctx.Value(invocationStateContextKey{}).(InvocationState)
	return invState, ok
}

// Option configures an invocationState built by NewInvocationState. Options are
// test hooks; production passes none and uses the real clock.
type Option func(*invocationState)

// WithClock overrides the clock used for deadline comparisons and computation.
// It lets tests freeze or advance time without changing production semantics;
// production passes no option, so the real time.Now is used.
func WithClock(next func() time.Time) Option {
	return func(state *invocationState) { state.now = next }
}

// NewInvocationState builds the concrete per-message InvocationState handle the
// stream layer injects. It binds an invocationStateStore to one (stream, group,
// msgID) and captures the delivery context so the runner's calls hit the right
// key. Reads are lazy per-invocation (one HGET per call).
func NewInvocationState(
	ctx context.Context,
	store invocationStateStore,
	stream,
	group,
	msgID string,
	log *slog.Logger,
	opts ...Option,
) InvocationState {
	state := &invocationState{
		ctx:    ctx,
		store:  store,
		stream: stream,
		group:  group,
		msgID:  msgID,
		log:    log,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(state)
	}
	return state
}

// invocationState is the concrete per-message handle the stream layer injects.
type invocationState struct {
	ctx    context.Context
	store  invocationStateStore
	stream string
	group  string
	msgID  string
	log    *slog.Logger
	now    func() time.Time
}

// IsComplete treats a Redis read error as not completed (fail-open): the runner
// re-runs the invocation, preserving at-least-once semantics.
func (state *invocationState) IsComplete(invocation string) bool {
	done, err := state.store.completed(state.ctx, state.stream, state.group, state.msgID, invocation)
	if err != nil {
		state.log.Debug("Invocation state: read failed; treating as not completed", "invocation", invocation, "error", err)
		return false
	}
	return done
}

// IsTerminal reports whether the invocation is terminal (complete or exhausted).
// A Redis read error fails open to false (not terminal), so the message is
// conservatively left pending rather than DLQ'd.
func (state *invocationState) IsTerminal(invocation string) bool {
	terminal, err := state.store.terminal(state.ctx, state.stream, state.group, state.msgID, invocation)
	if err != nil {
		state.log.Debug("Invocation state: read failed; treating as not terminal", "invocation", invocation, "error", err)
		return false
	}
	return terminal
}

// ClaimClassification atomically claims this message's one-time logical-event
// classification and reports whether this delivery won the claim. A store error
// is not fail-open here: classification counters must be exact, so the error is
// logged and (false, err) returned so the caller counts nothing.
func (state *invocationState) ClaimClassification() (bool, error) {
	claimed, err := state.store.claimClassification(state.ctx, state.stream, state.group, state.msgID)
	if err != nil {
		state.log.Debug("Invocation state: classification claim failed; not counting event", "error", err)
		return false, err
	}
	return claimed, nil
}

// TraceReference returns the compact trace lineage recorded by the invocation's
// most recent attempt, or "" when none is recorded. A read error is logged and
// degrades to no reference (best-effort lineage must never fail an invocation).
func (state *invocationState) TraceReference(invocation string) string {
	lineage, err := state.store.traceReference(state.ctx, state.stream, state.group, state.msgID, invocation)
	if err != nil {
		state.log.Debug("Invocation state: trace reference read failed; no link",
			"invocation", invocation, "error", err)
		return ""
	}
	return lineage
}

// RecordTrace persists the invocation's current-attempt trace lineage so a later
// retry can link back to it. A write error is logged only: lineage is diagnostic
// and must never fail an invocation.
func (state *invocationState) RecordTrace(invocation, lineage string) {
	if err := state.store.recordTrace(state.ctx, state.stream, state.group, state.msgID, invocation, lineage); err != nil {
		state.log.Debug("Invocation state: trace record failed; retry will not link",
			"invocation", invocation, "error", err)
	}
}

// MarkComplete CASes the caller's claim (attempt + token) against the active
// marker and records completion. It returns true when the invocation is
// complete (written, or already "ok"), false when the claim is stale or the
// marker is terminal exhausted. A false result is an explicit stale/refused
// outcome, not an error. A write error is logged and returns false: the message
// will simply be re-run later, preserving at-least-once semantics. State
// bookkeeping must never become a new failure source.
func (state *invocationState) MarkComplete(invocation string, claim InvocationClaim) bool {
	ok, err := state.store.markComplete(state.ctx, state.stream, state.group, state.msgID, invocation, claim)
	if err != nil {
		state.log.Warn("Invocation state: mark failed; message will be re-run later", "invocation", invocation, "error", err)
		return false
	}
	if !ok {
		state.log.Debug("Invocation state: completion refused; claim stale or exhausted",
			"invocation", invocation, "attempt", claim.Attempt)
	}
	return ok
}

// TryStart attempts to claim the invocation for a new execution, persisting the
// claim's absolute deadline (now + timeout), attempt, and fresh opaque token as
// the running marker via one atomic script. It returns started true (with the
// confirmed InvocationClaim) when the caller should execute, false when the
// invocation is already complete, exhausted, or protected by an active attempt
// deadline or retry backoff (this or another replica). When not started,
// claim.Attempt is the existing marker's attempt (0 for complete) and wait is
// the duration until the invocation becomes eligible again (0 for terminal
// complete/exhausted).
//
// On a Redis error it returns the error (NOT failed open): the claim outcome is
// unknown, so the runner must leave the message pending and not run the handler
// on this delivery. Running an ambiguous duplicate could race another replica
// that won the same claim. The returned claim is the zero value on the error
// path and must not be used to advance handler-attempt accounting.
func (state *invocationState) TryStart(invocation string, timeout time.Duration) (started bool, claim InvocationClaim, wait time.Duration, err error) {
	now := state.now()
	started, claim, wait, err = state.store.tryStart(state.ctx, state.stream, state.group, state.msgID, invocation, now, now.Add(timeout))
	if err != nil {
		state.log.Debug("Invocation state: try-start failed; claim not confirmed; leaving pending",
			"invocation", invocation, "error", err)
		return false, InvocationClaim{}, 0, err
	}
	return started, claim, wait, nil
}

// ScheduleDescriptor returns the descriptor pinned for this message's schedule
// occurrence, or ok=false when none is pinned (or the value is unreadable). A
// read error fails open to (zero, false): the runner then resolves the current
// template by name, and the atomic TryStartScheduled still guarantees exactly one
// descriptor wins.
func (state *invocationState) ScheduleDescriptor() (ScheduleDescriptor, bool) {
	desc, ok, err := state.store.scheduleDescriptor(state.ctx, state.stream, state.group, state.msgID)
	if err != nil {
		state.log.Debug("Invocation state: schedule descriptor read failed; resolving current template", "error", err)
		return ScheduleDescriptor{}, false
	}
	return desc, ok
}

// TryStartScheduled is the schedule-occurrence admission boundary. It atomically
// pins the descriptor (only when none is pinned) and claims the invocation,
// retrying with the winning descriptor if the message already carries one it did
// not have. When the caller can propose nothing (a non-none rejection) it instead
// atomically RESERVES the terminal rejection — refusing it if the occurrence was
// already admitted — so a rejection can never cancel a live admission and an
// admission can never follow a reservation. See InvocationState.TryStartScheduled
// for the contract.
func (state *invocationState) TryStartScheduled(
	desc ScheduleDescriptor,
	rejection ScheduleRejection,
	invocationFor func(ScheduleDescriptor) string,
) (ScheduleAdmission, error) {
	for attempt := 0; attempt < maxScheduleAdoptionAttempts; attempt++ {
		// Rejection path: the caller can propose nothing. Reserve the terminal
		// rejection atomically, or adopt a descriptor an earlier delivery pinned.
		if rejection != ScheduleRejectionNone {
			rr, err := state.store.reserveScheduleRejection(
				state.ctx, state.stream, state.group, state.msgID, rejection,
			)
			if err != nil {
				// The reservation outcome is unknown: do NOT disposition a
				// rejection on an ambiguous result. Leave the message pending.
				state.log.Debug("Invocation state: schedule rejection reservation failed; leaving pending", "error", err)
				return ScheduleAdmission{}, err
			}
			switch rr.outcome {
			case scheduleRejectionReserved, scheduleRejectionAlreadyReserved:
				return ScheduleAdmission{Rejected: true, Rejection: rr.kind}, nil
			case scheduleRejectionTerminal:
				// The message's hash is terminal-retained (its message already left
				// the PEL): nothing to reject. Report terminal so the stream ACKs
				// idempotently.
				return ScheduleAdmission{}, nil
			case scheduleRejectionInFlight, scheduleRejectionComplete, scheduleRejectionExhausted:
				// A no-descriptor admitted marker (legacy/pre-pinning state): an
				// admitted invocation always beats a rejection, and there is no
				// descriptor to adopt. Complete the marker's real disposition.
				return markerAdmission(rr.markerKind, rr.markers), nil
			case scheduleRejectionAdmitted:
				// Admission-first: another delivery admitted the occurrence before
				// this rejection. An active admission always beats a rejection, so
				// adopt the pinned descriptor and run the normal admission (the
				// present-app path re-reads the descriptor's invocation marker in
				// tryStartScheduled, which already handles a terminal marker).
				if !rr.hasPinned {
					return ScheduleAdmission{}, fmt.Errorf(
						"invocation state: rejected admission without a pinned descriptor for %q", state.msgID)
				}
				desc = rr.pinned
				rejection = ScheduleRejectionNone
				continue
			default:
				return ScheduleAdmission{}, fmt.Errorf("invocation state: unexpected schedule rejection outcome %d", rr.outcome)
			}
		}

		invocation := invocationFor(desc)
		now := state.now()
		res, err := state.store.tryStartScheduled(
			state.ctx, state.stream, state.group, state.msgID, invocation,
			now, now.Add(desc.Timeout), desc, true,
		)
		if err != nil {
			// The claim outcome is unknown: do NOT execute and leave the message
			// pending. No attempt was confirmed, so no retry/exhaustion accounting.
			state.log.Debug("Invocation state: scheduled try-start failed; claim not confirmed; leaving pending",
				"invocation", invocation, "error", err)
			return ScheduleAdmission{}, err
		}
		switch res.outcome {
		case scheduleStartStarted:
			d, _ := pinnedOr(res, desc)
			return ScheduleAdmission{
				Started:    true,
				Claim:      res.claim,
				Descriptor: d,
			}, nil
		case scheduleStartProtected:
			d, _ := pinnedOr(res, desc)
			return ScheduleAdmission{
				Claim:      res.claim,
				Wait:       res.wait,
				Descriptor: d,
			}, nil
		case scheduleStartTerminal:
			d, _ := pinnedOr(res, desc)
			return ScheduleAdmission{
				Claim:      res.claim,
				Descriptor: d,
			}, nil
		case scheduleStartRejected:
			// A terminal pre-admission rejection was reserved by an earlier
			// delivery: no proposal may pin or claim it. Report the reserved kind
			// so the caller dispositions the occurrence deterministically.
			return ScheduleAdmission{Rejected: true, Rejection: res.rejection}, nil
		case scheduleStartNoAdmission:
			// Unreachable from this loop: a proposal is always supplied here. A
			// script reply of no-admission would mean a protocol violation.
			return ScheduleAdmission{}, fmt.Errorf("invocation state: unexpected no-admission with a proposal for %q", state.msgID)
		case scheduleStartConflict:
			// The message carries a descriptor the caller did not supply: adopt it
			// and retry. A pinned descriptor is immutable while the message is
			// recoverable, so the retry observes it as the winner and cannot loop.
			if !res.hasPinned {
				return ScheduleAdmission{}, fmt.Errorf(
					"invocation state: schedule conflict without a pinned descriptor for %q", state.msgID)
			}
			desc = res.pinned
			continue
		default:
			return ScheduleAdmission{}, fmt.Errorf("invocation state: unexpected schedule outcome %d", res.outcome)
		}
	}
	return ScheduleAdmission{}, fmt.Errorf("invocation state: schedule admission did not converge for %q", invocationFor(desc))
}

// pinnedOr returns the pinned descriptor when the script reported one, otherwise
// the fallback (the caller's own proposal). The pinned descriptor always wins on
// a successful admission because it is the one the script persisted.
func pinnedOr(res scheduleStartResult, fallback ScheduleDescriptor) (ScheduleDescriptor, bool) {
	if res.hasPinned {
		return res.pinned, true
	}
	return fallback, false
}

// ReserveScheduleRejection is the reserve-only pre-admission rejection boundary.
// It never claims or starts an invocation: it atomically reserves the caller's
// terminal rejection (refusing it if the occurrence was already admitted) and
// reports the disposition. See InvocationState.ReserveScheduleRejection for the
// contract. It is used by a replica whose app is absent and therefore cannot
// execute an adopted descriptor, so the caller must never be handed a Started
// admission: a pinned descriptor's invocation marker is resolved here (active →
// pending, complete → ACK, exhausted → DLQ then ACK) rather than adopted.
func (state *invocationState) ReserveScheduleRejection(kind ScheduleRejection) (ScheduleAdmission, error) {
	rr, err := state.store.reserveScheduleRejection(state.ctx, state.stream, state.group, state.msgID, kind)
	if err != nil {
		// The reservation outcome is unknown: do NOT disposition a rejection on an
		// ambiguous result. Leave the message pending.
		state.log.Debug("Invocation state: schedule rejection reservation failed; leaving pending", "error", err)
		return ScheduleAdmission{}, err
	}
	switch rr.outcome {
	case scheduleRejectionReserved, scheduleRejectionAlreadyReserved:
		// The reservation is in force (this call wrote it, or an earlier delivery
		// did): the stored kind is authoritative.
		return ScheduleAdmission{Rejected: true, Rejection: rr.kind}, nil
	case scheduleRejectionTerminal:
		// The message's hash is terminal-retained (its message already left the
		// PEL): nothing to reject. A zero admission tells the caller to ACK
		// idempotently.
		return ScheduleAdmission{}, nil
	case scheduleRejectionAdmitted, scheduleRejectionInFlight, scheduleRejectionComplete, scheduleRejectionExhausted:
		// An admitted invocation exists (a pinned descriptor and/or an invocation
		// marker). This caller cannot execute an adopted descriptor (its app is
		// absent), so complete the marker's real disposition: active/no marker →
		// pending; exhausted → the still-owed invocations (DLQ then ACK); complete
		// → the success tail (ACK).
		return markerAdmission(rr.markerKind, rr.markers), nil
	default:
		return ScheduleAdmission{}, fmt.Errorf("invocation state: unexpected schedule rejection outcome %d", rr.outcome)
	}
}

// markerAdmission maps a reserve scan's marker classification to the runner's
// disposition, shared by the pinned-descriptor and no-descriptor reserve paths so
// both resolve a terminal marker identically. An ACTIVE marker (or none, i.e. a
// pinned descriptor whose attempt has not reached a terminal marker) keeps the
// message PENDING; an exhausted marker reports the still-owed invocations (or
// pending when the identity is unattributable); a complete marker is the success
// tail (a zero admission → ACK).
func markerAdmission(kind scheduleMarkerKind, markers []scheduleMarker) ScheduleAdmission {
	switch kind {
	case scheduleMarkerExhausted:
		if ex := exhaustedInvocationsFromMarkers(markers); len(ex) > 0 {
			return ScheduleAdmission{ExhaustedInvocations: ex}
		}
		return ScheduleAdmission{InFlight: true}
	case scheduleMarkerComplete:
		return ScheduleAdmission{}
	default: // scheduleMarkerActive or scheduleMarkerNone
		return ScheduleAdmission{InFlight: true}
	}
}

// exhaustedInvocationsFromMarkers converts the reserve scan's exhausted markers
// into the runner's per-invocation exhaustion metadata. Each marker's invocation
// is the "<app>/<handler>" hash field it was recorded under; a field without a
// separator (or an empty component, or a non-positive attempt) is not a real
// invocation identity, so it is skipped rather than dead-lettered as an
// unattributable entry. The caller falls back to pending when nothing remains.
func exhaustedInvocationsFromMarkers(markers []scheduleMarker) []ExhaustedInvocation {
	out := make([]ExhaustedInvocation, 0, len(markers))
	for _, m := range markers {
		app, handler, ok := strings.Cut(m.Invocation, "/")
		if !ok || app == "" || handler == "" || m.Attempt < 1 {
			continue
		}
		out = append(out, ExhaustedInvocation{App: app, Handler: handler, Attempts: m.Attempt})
	}
	return out
}

// RecordFailure persists a failed attempt's retry backoff so a later delivery
// is gated until now+backoff, returning whether the transition applied. The
// claim is the one this caller confirmed via TryStart; the store CASes
// attempt+token against the active marker so a stale owner cannot overwrite a
// newer claim's marker. A refused (stale/terminal) result is logged at debug and
// is not an error. A write error is logged and returns false: if the marker is
// lost, the invocation becomes eligible immediately (at-least-once), and the
// message stays pending for a later delivery regardless.
func (state *invocationState) RecordFailure(invocation string, claim InvocationClaim, backoff time.Duration) bool {
	ok, err := state.store.finishFailure(state.ctx, state.stream, state.group, state.msgID, invocation, claim, backoff, state.now())
	if err != nil {
		state.log.Warn("Invocation state: record failure failed; leaving field as-is (eligible immediately)",
			"invocation", invocation, "error", err)
		return false
	}
	if !ok {
		// The marker was terminal (ok/exhausted) or owned by a newer claim: no
		// state was written (correctly), so nothing to schedule.
		state.log.Debug("Invocation state: failure not recorded; invocation terminal or superseded",
			"invocation", invocation, "attempt", claim.Attempt)
		return false
	}
	state.log.Debug("Invocation state: failure recorded; next attempt eligible",
		"invocation", invocation, "next", state.now().Add(backoff))
	return true
}

// MarkExhausted records that the invocation's attempts are exhausted, making it
// terminal, CASed on the caller's claim (attempt + token), returning whether the
// transition applied. A refused (stale/terminal) result is logged at debug. A
// write error is logged and returns false: if the marker is lost, a later
// delivery may re-run the invocation once (at-least-once), which is safe.
func (state *invocationState) MarkExhausted(invocation string, claim InvocationClaim) bool {
	ok, err := state.store.markExhausted(state.ctx, state.stream, state.group, state.msgID, invocation, claim)
	if err != nil {
		state.log.Warn("Invocation state: mark exhausted failed", "invocation", invocation, "error", err)
		return false
	}
	if !ok {
		state.log.Debug("Invocation state: exhaustion refused; claim stale or terminal",
			"invocation", invocation, "attempt", claim.Attempt)
	}
	return ok
}
