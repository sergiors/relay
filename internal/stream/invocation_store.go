package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// claimTokenBytes is the size of the crypto-random per-claim nonce. 16 bytes
// (128 bits) makes a token collision between two independent claims
// computationally impossible, which is what lets the token — not the attempt
// number — be the authoritative claim identity. The token is opaque: it is never
// logged, metricked, or written to the DLQ.
const claimTokenBytes = 16

// newClaimToken returns a fresh crypto-random claim token (lowercase hex). A
// failure is returned (never a panic) so the caller can fail the claim closed:
// the runner must leave the message pending and must NOT execute the handler on
// an unverifiable claim.
func newClaimToken() (string, error) {
	var b [claimTokenBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate invocation claim token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// scheduleStartOutcome classifies one evaluation of the schedule admission
// boundary. It is internal: the conflict case is resolved by the
// InvocationState.TryStartScheduled adoption loop and never reaches the runner.
type scheduleStartOutcome int

const (
	// scheduleStartStarted means the invocation was claimed for a new attempt.
	scheduleStartStarted scheduleStartOutcome = iota
	// scheduleStartProtected means the invocation is protected by an active
	// running deadline or retry backoff (this or another replica).
	scheduleStartProtected
	// scheduleStartTerminal means the invocation is complete, exhausted, or its
	// hash is terminal-retained: never eligible again.
	scheduleStartTerminal
	// scheduleStartConflict means the caller's proposed descriptor differs from
	// one already pinned by another delivery; no claim was made. The caller must
	// adopt the returned pinned descriptor and retry within the same store call.
	scheduleStartConflict
	// scheduleStartNoAdmission means no descriptor is pinned AND the caller
	// proposed none (its schedule NAME is gone): the occurrence was never
	// admitted, so it is obsolete.
	scheduleStartNoAdmission
	// scheduleStartRejected means a TERMINAL pre-admission rejection was already
	// RESERVED for this message by an earlier delivery: no descriptor may ever be
	// pinned and no invocation may be claimed. The reserved kind (invalid or
	// obsolete) is carried on the result so the caller dispositions the
	// occurrence exactly as the first delivery decided, even if the current
	// template has since changed.
	scheduleStartRejected
)

// scheduleRejectionOutcome classifies one evaluation of the rejection-reservation
// boundary (see invocationStore.reserveScheduleRejection). It is the
// disposition-side companion of scheduleStartOutcome.
type scheduleRejectionOutcome int

const (
	// scheduleRejectionReserved means this call atomically wrote the reservation:
	// the occurrence is now terminally rejected and no admission can follow.
	scheduleRejectionReserved scheduleRejectionOutcome = iota
	// scheduleRejectionAlreadyReserved means a reservation already existed (a
	// redelivery after a crash, or a concurrent replica): the stored kind is
	// authoritative and this call wrote nothing.
	scheduleRejectionAlreadyReserved
	// scheduleRejectionAdmitted means a descriptor is already pinned: another
	// delivery ADMITTED the occurrence first, so the rejection is refused and the
	// caller must adopt the pinned descriptor instead. This is the admission-first
	// outcome that stops a rejection from cancelling a live admission.
	scheduleRejectionAdmitted
	// scheduleRejectionInFlight means an ACTIVE invocation marker (running / retry
	// backoff) exists for the message but no descriptor is pinned: the occurrence
	// WAS admitted by a delivery whose descriptor is not readable (a legacy/
	// pre-pinning claim, or a hand-built hash) and that attempt may still complete.
	// The rejection is refused and NO write happens; the caller keeps the message
	// PENDING (never ACK/DLQ) so the in-flight attempt is not cancelled.
	scheduleRejectionInFlight
	// scheduleRejectionExhausted means a no-descriptor TERMINAL exhausted marker
	// ("exhausted:<n>:<token>[:dlq]") owns the message: a legacy/pre-pinning
	// admitted invocation already exhausted and its DLQ disposition is still owed.
	// The rejection is refused and NO write happens; the caller routes the
	// invocation(s) in result.markers to the DLQ (idempotently — an already-":dlq"
	// marker is skipped by the existing persistence check) and the message is then
	// ACKed. It never overwrites the exhausted marker.
	scheduleRejectionExhausted
	// scheduleRejectionComplete means a no-descriptor TERMINAL "ok" marker owns the
	// message: a legacy/pre-pinning admitted invocation already succeeded. The
	// rejection is refused and NO write happens; the caller ACKs the message
	// idempotently.
	scheduleRejectionComplete
	// scheduleRejectionTerminal means the message's hash is terminal-retained
	// (its message already left the PEL): there is nothing to reject and no
	// admission can follow.
	scheduleRejectionTerminal
)

// scheduleMarker identifies ONE legacy/pre-pinning admitted invocation found by
// the reserve-only rejection scan: the hash field it was recorded under (the
// "<app>/<handler>" invocation ID) and, for an exhausted marker, its retained
// attempt count. Only exhausted markers need this identity (to dead-letter the
// invocation); active/complete markers carry no per-invocation attribution.
type scheduleMarker struct {
	// Invocation is the "<app>/<handler>" hash field the marker was found under.
	Invocation string
	// Attempt is the exhausted marker's retained attempt count (>=1).
	Attempt int
}

// scheduleMarkerKind classifies the invocation marker(s) found by the reserve
// scan, with the fixed precedence active > exhausted > complete > none. It is
// the same classification for a pinned descriptor (a current-version delivery
// pinned + claimed, then completed/exhausted) and for a no-descriptor legacy
// marker, so both callers share one disposition.
type scheduleMarkerKind int

const (
	// scheduleMarkerNone means no invocation marker was found: for a pinned
	// descriptor the caller must keep the message pending (admitted, no
	// disposition yet); with no descriptor the reservation may be written.
	scheduleMarkerNone scheduleMarkerKind = iota
	// scheduleMarkerActive means a running / retry-backoff marker exists: the
	// attempt may still complete, so the message stays pending.
	scheduleMarkerActive
	// scheduleMarkerExhausted means a terminal exhausted marker exists: each
	// listed invocation still owes a per-invocation DLQ entry.
	scheduleMarkerExhausted
	// scheduleMarkerComplete means a terminal "ok" marker exists: the admitted
	// invocation already succeeded (ACK).
	scheduleMarkerComplete
)

// scheduleRejectionResult is the store-level result of one reserveScheduleRejection
// evaluation. kind is the authoritative rejection (the stored one on
// alreadyReserved, the requested one on reserved); pinned is set on admitted;
// markerKind/markers classify the invocation marker(s) found by the scan (for the
// admitted outcome and the no-descriptor marker outcomes).
type scheduleRejectionResult struct {
	outcome    scheduleRejectionOutcome
	kind       ScheduleRejection
	pinned     ScheduleDescriptor
	hasPinned  bool
	markerKind scheduleMarkerKind
	markers    []scheduleMarker
}

// scheduleStartResult is the store-level result of ONE atomic schedule-admission
// attempt (see invocationStore.tryStartScheduled). hasPinned distinguishes a
// genuinely pinned descriptor from "none pinned" even when the descriptor's
// schedule name is empty (the state-free fallback path).
type scheduleStartResult struct {
	outcome   scheduleStartOutcome
	claim     InvocationClaim
	wait      time.Duration
	pinned    ScheduleDescriptor
	hasPinned bool
	// rejection is set only for scheduleStartRejected: the reserved terminal
	// rejection kind (invalid or obsolete) that owns the message.
	rejection ScheduleRejection
}

// maxScheduleAdoptionAttempts bounds the descriptor adoption loop: at most one
// conflict can be observed because the pinned descriptor is immutable while the
// message is recoverable, so two rounds always suffice. The bound is defensive.
const maxScheduleAdoptionAttempts = 4

// ttlMillis converts a TTL to integer milliseconds for the PEXPIRE argument.
func ttlMillis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

// terminalField is the reserved invocation-state hash field that marks a hash as
// no longer recoverable: the message it belongs to has left the PEL (a
// successful XACK on the success/obsolete/DLQ path, or a purged missing-payload
// PEL reference), so ackAndRetainTerminal (or, without an ACK, retainTerminal)
// switched the hash to terminal retention. While the field is present every
// lifecycle script is inert: a stale transition from an in-memory delivery can
// neither mutate the retained state nor remove the retention TTL (which would
// resurrect an unrecoverable hash). Like classificationField and traceFieldPrefix
// it cannot collide with a real invocation ID (a leading "__" is not a legal
// app name).
const terminalField = "__terminal"

// retentionTTLMillis returns the terminal retention TTL in integer milliseconds
// for the PEXPIRE argument passed to the retain script. Zero means terminal
// retention is DISABLED: the retain script still writes the terminal marker
// atomically but leaves the hash persistent (no expiry), so terminal/stale-
// transition guards are unchanged while no TTL is applied.
func retentionTTLMillis(retention time.Duration) int64 {
	return ttlMillis(retention)
}

// toInt64 coerces a value returned from a Lua EVAL reply (an int64 by default,
// but possibly a string or float depending on the go-redis decoder) to int64.
// It is defensive: a reply that cannot be represented yields 0, which the
// callers treat as a script-protocol violation (caught by length/position
// checks) rather than a valid result.
func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// invocationStateKey returns the Redis key holding a message's invocation-state
// hash. The key is scoped by stream and group so multiple groups/consumers
// reading the same message ID never collide, and by msgID so each message has
// its own hash. The "relay:" prefix keeps the namespace Relay-owned.
//
// WHY percent-encoding: Redis stream/group names are arbitrary strings and
// colons are legal in them, so raw concatenation could alias two distinct
// (stream, group) pairs onto one key (e.g. stream="a:b", group="c" vs
// stream="a", group="b:c"), cross-linking invocation state between groups for
// the same msgID. Percent-encoding the components makes the encoding injective:
// the ":" separators are unambiguous because any literal ":" in a component is
// escaped as "%3A" (and "%" as "%25" so the escape itself cannot be forged by a
// name containing "%3A"). msgID is a Redis stream ID ("<ms>-<seq>", digits and
// dashes only, as reported verbatim by go-redis), so it is safe to leave raw; it
// is encoded too only for uniformity.
func invocationStateKey(stream, group, msgID string) string {
	return "relay:invocation:" + encodeComponent(stream) + ":" + encodeComponent(group) + ":" + encodeComponent(msgID)
}

// encodeComponent percent-encodes a single key component so the ":" separators
// in invocationStateKey are unambiguous. Only "%" and ":" need escaping: "%"
// first so a name containing a literal "%3A" cannot be mistaken for an escaped
// ":".
func encodeComponent(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	return strings.ReplaceAll(s, ":", "%3A")
}

// invocationKind is the parsed lifecycle state of a single invocation's field
// value. It is the discriminated union the store reads back from the value
// string.
type invocationKind int

const (
	// kindEligible means the field is absent or unparseable: the invocation may
	// execute.
	kindEligible invocationKind = iota
	// kindComplete means the invocation succeeded on a previous delivery.
	kindComplete
	// kindRunning means an attempt is (or was) executing, protected until its
	// deadline.
	kindRunning
	// kindNextAttempt means a failed attempt is waiting out its retry backoff,
	// protected until its deadline.
	kindNextAttempt
	// kindExhausted means the invocation's attempts are exhausted: it is
	// terminal and never eligible again (skipped like complete, but distinct so
	// the runner can tell a message whose invocations are all terminal).
	kindExhausted
)

// terminalAckResult is the unambiguous outcome of ackAndRetainTerminal,
// reported from ONE atomic operation: whether the message left the group PEL
// and whether its invocation-state hash was switched to terminal retention.
type terminalAckResult struct {
	// Removed is true when the XACK removed the message from the group PEL.
	// false means the message was already not pending (an idempotent no-op),
	// which is still a terminal disposition.
	Removed bool
	// Retained is true when the invocation-state hash existed and now carries
	// the terminal marker (plus, when retention is enabled, the retention TTL).
	// false means there was no hash to retain (a stateless message): the ACK
	// still happened and no empty key was created.
	Retained bool
}

// invocationStateStore is the per-message invocation-state persistence seam:
// the concrete *invocationStore implements it against Redis; tests may
// substitute a fake. Every active-claim-originated transition takes the
// InvocationClaim returned by tryStart and CASes attempt+token against the
// current ACTIVE marker; a mismatch is returned as an explicit stale result
// (false), never a Redis error.
type invocationStateStore interface {
	// ackAndRetainTerminal acknowledges the message AND switches its
	// invocation-state hash to terminal retention in ONE Lua script, so the
	// post-XACK orphan window (a hash whose message left the PEL but never got a
	// terminal marker/TTL) cannot exist. It is the production ACK
	// boundary: the success, obsolete-schedule, malformed/DLQ, and
	// missing-payload paths all acknowledge through it, never a separate
	// XAck + retainTerminal pair.
	//
	// The terminal marker is staged (PERSISTent, no TTL) BEFORE the XACK and the
	// retention TTL is applied only AFTER it, so a failed ACK never leaves an
	// expiring TTL on a still-pending message. An absent hash is still
	// acknowledged (no empty key is created); an already-terminal hash is an
	// idempotent no-op. retention is the resolved window: >0 applies that
	// PEXPIRE TTL, <=0 writes the marker but leaves the hash PERSISTENT.
	//
	// A returned error means the ACK did not resolve (a wrong-type state key
	// refused before any write, or a Redis/transport failure): the caller MUST
	// leave the message pending.
	ackAndRetainTerminal(ctx context.Context, stream, group, msgID string, retention time.Duration) (terminalAckResult, error)
	completed(ctx context.Context, stream, group, msgID, invocation string) (bool, error)
	// tryStart atomically claims the invocation for a new execution when
	// eligible. On success it returns (started=true, claim) where claim carries
	// the confirmed attempt and a fresh opaque token. When not started, claim is
	// the existing marker's attempt (0 for "ok", the exhausted attempt for an
	// exhausted marker) and wait>0 while the invocation is protected by an active
	// deadline (0 when terminal).
	tryStart(
		ctx context.Context,
		stream, group, msgID, invocation string,
		now, deadline time.Time,
	) (started bool, claim InvocationClaim, wait time.Duration, err error)
	// tryStartScheduled is the schedule-path admission: it atomically pins the
	// caller's descriptor (when none is pinned) and claims the invocation in one
	// script. desc is the descriptor the caller would execute under; known reports
	// whether the caller actually knows a descriptor (false when it could neither
	// read a pinned one nor resolve a current schedule). When known is false the
	// invocation string is ignored and the script either hands back the pinned
	// descriptor to adopt (conflict) or reports no admission (obsolete). A message
	// that already carries a RESERVED pre-admission rejection reports
	// scheduleStartRejected and never pins or claims, so a reserved rejection can
	// never be raced into an admission. It returns the raw scheduleStartResult for
	// ONE script evaluation; the InvocationState handle wraps it in the bounded
	// adoption loop. A store/transport error is returned (NOT failed open) so no
	// handler runs on an ambiguous claim.
	tryStartScheduled(
		ctx context.Context,
		stream, group, msgID, invocation string,
		now, deadline time.Time,
		desc ScheduleDescriptor,
		known bool,
	) (scheduleStartResult, error)
	// reserveScheduleRejection atomically reserves a TERMINAL pre-admission
	// rejection (invalid or obsolete) for a schedule occurrence, but only when the
	// occurrence was never admitted (see reserveScheduleRejectionScript). It is the
	// disposition half of the admission boundary: a no-admission observation
	// (tryStartScheduled report 4) is completed by this call, which either writes
	// the reservation (so every later admission refuses) or reports that the
	// occurrence was ADMITTED first (so the caller adopts the pinned descriptor
	// instead of rejecting). The invocation marker(s) are classified with a fixed
	// precedence (active > exhausted > complete > none), for a pinned descriptor and
	// a no-descriptor legacy marker alike: scheduleRejectionAdmitted carries the
	// pinned descriptor plus the classification, scheduleRejectionInFlight keeps the
	// message pending, scheduleRejectionExhausted carries each still-owed invocation
	// (DLQ then ACK), and scheduleRejectionComplete is the success tail (ACK). A
	// store/transport error is returned (NOT failed open) so no rejection is
	// dispositioned on an ambiguous outcome.
	reserveScheduleRejection(
		ctx context.Context,
		stream, group, msgID string,
		kind ScheduleRejection,
	) (scheduleRejectionResult, error)
	// finishFailure CASes the caller's claim against the current active marker
	// and, on a match, writes the retry backoff marker at now+backoff. It returns
	// ok=false (an explicit stale/terminal result, never an error) when the claim
	// no longer owns the marker.
	finishFailure(
		ctx context.Context,
		stream, group, msgID, invocation string,
		claim InvocationClaim,
		backoff time.Duration,
		now time.Time,
	) (ok bool, err error)
	// markComplete CASes the caller's claim against the current active marker
	// and, on a match, writes the terminal "ok". It returns false (an explicit
	// stale/refused result, never an error) when the marker is no longer owned
	// by the claim or is already exhausted.
	markComplete(ctx context.Context, stream, group, msgID, invocation string, claim InvocationClaim) (bool, error)
	// markExhausted CASes the caller's claim against the current active marker
	// and, on a match, writes "exhausted:<attempt>:<token>" (the claim identity
	// is retained so the later DLQ upgrade can CAS it). It returns false when the
	// marker is not owned by the claim.
	markExhausted(ctx context.Context, stream, group, msgID, invocation string, claim InvocationClaim) (bool, error)
	// persistDLQ atomically appends one DLQ entry to dlqStream and records its
	// persistence in the message's invocation-state hash, in a single Lua script
	// (see persistDLQScript), so a crash or error between the append and the
	// marker can never leave a duplicate or unmarked entry (F-008).
	//
	// For a real invocation it requires an existing exhausted marker matching the
	// supplied claim (attempt+token) and upgrades it to ":dlq"; for the
	// malformed-message placeholder (invocation == "") it records a reserved
	// message-scoped field instead of inventing a handler identity. wrote reports
	// whether THIS call appended the entry; wrote=false with err=nil means the
	// entry was already recorded, so no write happened and no DLQ counter should
	// be incremented. An error means the outcome is UNKNOWN or refused: nothing
	// was written, and the caller must NOT ACK the source message.
	persistDLQ(
		ctx context.Context,
		dlqStream string,
		stream, group, msgID, invocation string,
		claim InvocationClaim,
		entry map[string]any,
	) (wrote bool, err error)
	// exhaustedState reads the invocation's exhausted marker, returning its
	// retained claim identity and whether its DLQ entry is already persisted. It
	// lets routeToDLQ skip an invocation whose entry was already written AND
	// supply the exact claim identity to the DLQ-upgrade CAS, so a partial
	// multi-entry write is completed without duplicating the entries that
	// succeeded and without downgrading a newer state.
	exhaustedState(ctx context.Context, stream, group, msgID, invocation string) (claim InvocationClaim, dlq bool, ok bool, err error)
	terminal(ctx context.Context, stream, group, msgID, invocation string) (bool, error)
	// claimClassification atomically claims the one-time event classification
	// for this message (an atomic Lua HSETNX on a reserved field). It returns
	// true only for the first caller across redeliveries and replicas.
	claimClassification(ctx context.Context, stream, group, msgID string) (bool, error)
	// traceReference returns the compact trace lineage persisted for this
	// invocation by its most recent attempt (see RecordTrace), or "" when none
	// was ever recorded. redis.Nil (field absent) is ("", nil).
	traceReference(ctx context.Context, stream, group, msgID, invocation string) (string, error)
	// scheduleDescriptor reads the message's pinned schedule admission descriptor
	// from the reserved scheduleField, or ok=false when none is pinned or the
	// value does not decode. redis.Nil (field absent) is (zero, false, nil).
	scheduleDescriptor(ctx context.Context, stream, group, msgID string) (ScheduleDescriptor, bool, error) // recordTrace atomically persists the compact trace lineage of this
	// invocation's most recent attempt under a reserved sibling field, clearing
	// any legacy TTL in the same script. It never disturbs the invocation's
	// lifecycle value, and it is inert once the hash is terminal-retained.
	recordTrace(ctx context.Context, stream, group, msgID, invocation, lineage string) error
	// retainTerminal switches the message's invocation-state hash to terminal
	// retention: it writes the reserved terminal marker and applies the retention
	// TTL in one atomic step. It must be called only AFTER the message is no
	// longer recoverable (a successful XACK, or a cleared missing-payload PEL
	// reference), because retention removes recoverability. It is monotonic and
	// never re-extends an already-terminal hash. A retention failure is logged,
	// never fatal: the hash is simply left persistent (a leak, never a premature
	// loss).
	//
	// It is the NON-XACK retention path: a missing-payload PEL reference the
	// server already purged (so there is no ACK to run), plus store-level tests.
	// Every production path that ACKs uses ackAndRetainTerminal instead, which
	// performs the ACK and the retention in a single script.
	//
	// retention is the resolved terminal window: a positive value applies that
	// PEXPIRE TTL, while zero (or negative) DISABLES retention — the marker is
	// still written atomically but the hash is left PERSISTENT instead of
	// expiring. The caller owns the resolved value (the consumer carries it from
	// ConsumerConfig.InvocationRetention), so an explicit operator disable is
	// never silently replaced by a default.
	retainTerminal(ctx context.Context, stream, group, msgID string, retention time.Duration) error
	// makeRecoverable clears any legacy/recoverable-time TTL from an EXISTING
	// invocation-state hash so it is PERSISTENT for as long as its message is
	// pending. It is called at the start of processing a delivery, before the
	// state is relied on, so a hash written by a pre-persistence Relay (with the
	// old fixed TTL) cannot expire under a redelivery. It is a no-op for a
	// missing hash and — critically — for an already terminal-retained hash (it
	// must never remove terminal retention).
	//
	// An error is a Redis-state failure, not a best-effort migration: the hash
	// may still carry its old TTL and could expire mid-delivery, so the caller
	// must NOT rely on the state, dispatch the handler, or ACK the message. It
	// leaves the message pending for a later reclaim instead, so a transient
	// Redis fault self-heals without losing the message.
	makeRecoverable(ctx context.Context, stream, group, msgID string) error
}

// classificationField is the reserved invocation-state hash field that records
// whether this message's logical-event classification (received/matched/
// unmatched) has already been claimed. It cannot collide with a real invocation
// ID, which is always "<app>/<handler>": app names are validated to
// start with [a-z0-9], so a leading "__" is not a legal app name.
const classificationField = "__classification"

// traceFieldPrefix is the reserved invocation-state hash field prefix under
// which each invocation's most recent attempt trace lineage is persisted, as
// "<prefix><invocation>" (e.g. "__trace:fn/index.run"). Like classificationField
// it cannot collide with a real invocation ID (a leading "__" is not a legal
// app name), and it is a SIBLING of the lifecycle value rather than part of
// it, so recording a lineage never disturbs eligibility parsing. The value is the
// compact traceparent[|tracestate] form (see tracing.SpanContextToString) and
// never contains baggage.
const traceFieldPrefix = "__trace:"

// scheduleField is the reserved invocation-state hash field that pins a schedule
// occurrence's admission contract (see ScheduleDescriptor). Like
// classificationField and traceFieldPrefix it cannot collide with a real
// invocation ID (a leading "__" is not a legal app name), and it is a
// sibling of the lifecycle values rather than part of them, so pinning a
// descriptor never disturbs eligibility parsing.
//
// It is written exactly once, atomically with the FIRST successful schedule
// claim (see tryStartScheduledScript), so concurrent replicas with different
// templates cannot both pin: exactly one proposal wins and every other delivery
// adopts it. While the message is recoverable the field persists (the hash has no
// TTL); terminal retention applies its TTL to the whole hash after the ACK,
// exactly like every other field.
const scheduleField = "__schedule"

// scheduleRejectionField is the reserved invocation-state hash field that
// reserves a TERMINAL pre-admission rejection (invalid or obsolete) for a
// schedule occurrence, so the rejection decision is atomic against a concurrent
// admission (F-030). Like terminalField, classificationField, traceFieldPrefix,
// and scheduleField it cannot collide with a real invocation ID (a leading "__"
// is not a legal app name); like them it is a SIBLING of the lifecycle values
// rather than part of one, so reserving a rejection never disturbs eligibility
// parsing.
//
// It is written exactly once, atomically, and only when no descriptor is pinned
// (see reserveScheduleRejectionScript): an admitted occurrence can never acquire
// a reservation, and a reserved occurrence can never be admitted. Its value is
// the rejection kind (see ScheduleRejection). While the message is recoverable
// the field persists (the hash has no TTL); terminal retention applies its TTL to
// the whole hash after the ACK, exactly like every other field.
const scheduleRejectionField = "__schedule_rejected"

// dlqPlaceholderField is the reserved invocation-state hash field that records
// that the malformed-message placeholder DLQ entry for this message has already
// been persisted (see persistDLQScript). A malformed message never reaches a
// handler, so it has no "<app>/<handler>" invocation to carry a per-invocation
// persistence marker; this message-scoped field takes its place and is in the
// SAME per-message hash (already keyed by stream/group/msgID), so no fabricated
// function or handler identity is invented. Like terminalField,
// classificationField, traceFieldPrefix, and scheduleField it cannot collide with
// a real invocation ID (a leading "__" is not a legal app name).
const dlqPlaceholderField = "__dlq"

// traceField returns the reserved hash field holding invocation's persisted
// trace lineage.
func traceField(invocation string) string {
	return traceFieldPrefix + invocation
}

// invocationStore is a thin Redis-backed store for per-message invocation
// state. Each key is a HASH mapping an invocation ID ("<app>/<handler>")
// to a short value describing that invocation's lifecycle for this message.
//
// The value grammar (a single string, so the same field convention stays
// greppable and forward-compatible). Deadlines are integer Unix MILLISECONDS;
// tokens are opaque crypto-random hex and are never logged, metricked, or
// written to the DLQ:
//
//	"ok"                                        → completed on a previous delivery
//	"running:<deadline_ms>:<attempt>:<token>"   → an attempt is (or was)
//	                                executing, protected until the absolute
//	                                Unix-ms deadline <deadline_ms>; <attempt> is
//	                                the 1-based handler attempt; <token> is the
//	                                opaque claim identity
//	"next_attempt_at:<deadline_ms>:<attempt>:<token>" → a failed attempt is
//	                                waiting out its retry backoff, protected
//	                                until <deadline_ms>; <token> is retained so
//	                                only this claim may write the next state
//	"exhausted:<attempt>:<token>"               → attempts exhausted; terminal,
//	                                never eligible, DLQ entry NOT yet persisted;
//	                                the claim identity is retained so the DLQ
//	                                upgrade can CAS it
//	"exhausted:<attempt>:<token>:dlq"           → attempts exhausted AND this
//	                                invocation's DLQ entry has been persisted;
//	                                terminal, never eligible, and never re-written
//	                                to the DLQ
//	(absent)                                    → eligible to execute
//
// "ok" stays bare because attempts/identity are no longer needed after
// completion. Any value that does not parse under this grammar is treated as
// eligible.
//
// The ":dlq" suffix records per-invocation DLQ persistence without scanning the
// DLQ stream: routeToDLQ consults it (exhaustedState) to skip an invocation
// whose entry was already written, which makes retrying a partially-written
// multi-entry DLQ (after an XACK failure or crash) idempotent.
//
// The hash also carries reserved sibling fields that are never invocation IDs:
// classificationField (the once-per-event classification claim), terminalField
// (the terminal-retention marker), and one traceFieldPrefix field per invocation
// (the last attempt's compact lineage).
//
// Lifetime: while the message is still recoverable (present in the PEL and
// therefore redeliverable) the hash is PERSISTENT — no TTL. Only once the
// message has left the PEL (a successful atomic ACK on the success/obsolete/DLQ
// path, or a purged missing-payload PEL reference) does the hash switch to
// terminal retention (the configured invocation window): every ACK path does it
// in the SAME script as the XACK (ackAndRetainTerminal), so no post-ACK orphan
// window exists; a purged reference with no ACK to run uses retainTerminal. A
// hash that never gets retained is leaked, never prematurely expired: safe by
// construction.
//
// It is the stream layer's domain (Redis), but the runner decides which
// invocations match, so the store is exposed to the runner through the
// InvocationState interface carried in the delivery context.
type invocationStore struct {
	client *redis.Client
}

// completed reports whether the invocation has already completed for this
// message; redis.Nil (field absent) means not completed.
func (store *invocationStore) completed(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (bool, error) {
	value, err := store.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "ok", nil
}

// terminal reports whether the invocation is terminal (complete or exhausted);
// redis.Nil (field absent) means not terminal.
func (store *invocationStore) terminal(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (bool, error) {
	value, err := store.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	kind, _, _, ok := parseInvocationState(value)
	if !ok {
		return false, nil
	}
	return kind == kindComplete || kind == kindExhausted, nil
}

// markComplete records that the invocation completed for this message via an
// atomic Lua script, CASed against the caller's claim (attempt + token). It
// writes "ok" only when the current ACTIVE marker is still owned by that exact
// claim, and treats an already-"ok" marker as an idempotent success. It
// deliberately PRESERVES an exhausted marker (":dlq" or not) and refuses a stale
// claim: success must never downgrade an exhaustion (that would re-open a
// terminal invocation and could resurrect a message already routed to the DLQ)
// and a stale owner must never overwrite a newer attempt. It returns false for
// both refusals (an explicit stale result, never a Redis error). The HSET and
// PERSIST happen in the same script, so the completed marker keeps the hash
// PERSISTENT (recoverable); the retention TTL is applied only after the XACK.
func (store *invocationStore) markComplete(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	claim InvocationClaim,
) (bool, error) {
	if !claim.valid() {
		return false, nil
	}
	key := invocationStateKey(stream, group, msgID)
	ok, err := markCompleteScript.Run(ctx, store.client, []string{key},
		invocation,
		claim.Attempt,
		claim.Token,
		terminalField,
	).Int64()
	if err != nil {
		return false, err
	}
	return ok == 1, nil
}

// tryStart attempts to claim the invocation for a new execution via one atomic
// Lua script. Go generates a crypto-random claim token BEFORE the EVAL; the
// script checks eligibility, then writes "running:<deadline_ms>:<n>:<token>" and
// PERSISTs the key, all in the SAME script. So concurrent callers cannot both
// observe a marker that is absent or past its deadline: exactly one EVAL claims
// it (with its unique token), and every other caller sees the new running marker
// and is protected (started=false, wait>0). There is no separate TTL command, so
// a claim can never be left without a TTL.
//
// A hash already switched to terminal retention (its message left the PEL) is
// never re-opened: the script reports it terminal without writing.
//
// Eligibility is exact integer-millisecond: a running/next_attempt_at marker is
// protected iff now_ms < deadline_ms and eligible iff now_ms >= deadline_ms,
// compared as decimal strings in Lua (no floating point, no clock-skew fudge).
//
// The decision outcomes:
//
//	"ok"                         → already complete; not started (attempt 0, no wait)
//	"exhausted:<n>:<token>"      → attempts exhausted; not started (attempt n, no wait)
//	"running|next_attempt_at:<dl_ms>:<n>:<token>" with now < dl
//	                             → a protected attempt is in flight or waiting
//	                               out its backoff; not started, wait = dl-now (ms)
//	terminal-retained hash       → not started (attempt 0, no wait)
//	absent, deadline elapsed, or unparseable → eligible: claim with a fresh
//	                               token at attempt n+1 (or 1) and report started
//
// A new claim token is generated with crypto/rand; a generation failure is
// returned (NOT failed open) so the runner leaves the message pending rather
// than execute under an unverifiable claim. A transport error is likewise
// returned: the claim outcome is genuinely unknown, so the runner must leave the
// message pending rather than run an ambiguous duplicate.
func (store *invocationStore) tryStart(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	now,
	deadline time.Time,
) (started bool, claim InvocationClaim, wait time.Duration, err error) {
	token, err := newClaimToken()
	if err != nil {
		return false, InvocationClaim{}, 0, err
	}
	key := invocationStateKey(stream, group, msgID)
	res, err := tryStartScript.Run(ctx, store.client, []string{key},
		invocation,
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(deadline.UnixMilli(), 10),
		token,
		terminalField,
	).Slice()
	if err != nil {
		return false, InvocationClaim{}, 0, err
	}
	if len(res) < 3 {
		// The script always returns three elements; a short reply is a protocol
		// violation, so treat the claim as unconfirmed rather than guess.
		return false, InvocationClaim{}, 0, fmt.Errorf("tryStart: unexpected script reply of length %d", len(res))
	}
	started = toInt64(res[0]) == 1
	attempt := int(toInt64(res[1]))
	if started {
		claim = InvocationClaim{Attempt: attempt, Token: token}
		return true, claim, 0, nil
	}
	// Not started: the reply carries the existing marker's attempt and, when
	// protected, its deadline (as a millisecond string) for the exact wait.
	claim = InvocationClaim{Attempt: attempt}
	dlStr, ok := res[2].(string)
	if !ok {
		return false, claim, 0, fmt.Errorf("tryStart: unexpected deadline reply type %T", res[2])
	}
	if dlStr != "0" {
		dlMs, perr := strconv.ParseInt(dlStr, 10, 64)
		if perr != nil {
			return false, claim, 0, fmt.Errorf("tryStart: bad deadline reply %q: %w", dlStr, perr)
		}
		wait = time.Duration(dlMs-now.UnixMilli()) * time.Millisecond
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
	}
	return false, claim, wait, nil
}

// tryStartScheduled is the schedule-path atomic admission. It runs
// tryStartScheduledScript, which in ONE EVAL pins the caller's descriptor (only
// when none is pinned) and claims the invocation, so the descriptor and the claim
// become visible together and exactly one of two racing proposals wins. A caller
// with no descriptor (known=false: its schedule NAME is gone and nothing was
// pinned) either adopts the pinned descriptor or is told the occurrence was never
// admitted (obsolete).
//
// It returns the raw ONE-evaluation result; the bounded adoption loop lives in
// invocationState.TryStartScheduled. The pinned descriptor is returned on every
// outcome so the caller can adopt a winning proposal it lost the race to.
//
// desc/known describe what the caller knows: known=false means it could neither
// read a pinned descriptor nor resolve the current schedule, so it supplies no
// proposal and the script either hands back the pinned descriptor (conflict) or
// reports no admission (obsolete). invocation is the invocation ID derived from
// desc and is ignored when known is false.
func (store *invocationStore) tryStartScheduled(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	now,
	deadline time.Time,
	desc ScheduleDescriptor,
	known bool,
) (scheduleStartResult, error) {
	token, err := newClaimToken()
	if err != nil {
		return scheduleStartResult{}, err
	}
	encoded := ""
	if known {
		encoded = encodeScheduleDescriptor(desc)
	}
	key := invocationStateKey(stream, group, msgID)
	res, err := tryStartScheduledScript.Run(ctx, store.client, []string{key},
		invocation,
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(deadline.UnixMilli(), 10),
		token,
		terminalField,
		scheduleField,
		encoded,
		scheduleRejectionField,
	).Slice()
	if err != nil {
		return scheduleStartResult{}, err
	}
	if len(res) < 4 {
		// The script always returns at least four elements; a short reply is a
		// protocol violation, so treat the claim as unconfirmed rather than guess.
		return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected script reply of length %d", len(res))
	}
	outcomeCode := toInt64(res[0])
	attempt := int(toInt64(res[1]))
	dlStr, ok := res[2].(string)
	if !ok {
		return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected deadline reply type %T", res[2])
	}
	pinnedStr, ok := res[3].(string)
	if !ok {
		return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected pinned reply type %T", res[3])
	}
	pinned, hasPinned := decodeScheduleDescriptor(pinnedStr)

	out := scheduleStartResult{claim: InvocationClaim{Attempt: attempt}, pinned: pinned, hasPinned: hasPinned}
	switch outcomeCode {
	case 0: // started
		out.outcome = scheduleStartStarted
		out.claim = InvocationClaim{Attempt: attempt, Token: token}
	case 1: // protected
		out.outcome = scheduleStartProtected
		if dlStr != "0" {
			dlMs, perr := strconv.ParseInt(dlStr, 10, 64)
			if perr != nil {
				return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: bad deadline reply %q: %w", dlStr, perr)
			}
			out.wait = time.Duration(dlMs-now.UnixMilli()) * time.Millisecond
			if out.wait < time.Millisecond {
				out.wait = time.Millisecond
			}
		}
	case 2: // terminal
		out.outcome = scheduleStartTerminal
	case 3: // conflict: adopt the pinned descriptor
		out.outcome = scheduleStartConflict
	case 4: // no admission
		out.outcome = scheduleStartNoAdmission
	case 5: // rejected: a terminal pre-admission rejection is already reserved
		if len(res) < 5 {
			return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: rejected reply missing the reservation kind")
		}
		rejStr, ok := res[4].(string)
		if !ok {
			return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected rejection reply type %T", res[4])
		}
		rej, ok := decodeScheduleRejection(rejStr)
		if !ok {
			// A foreign/corrupt reservation must surface (recoverable, inspectable)
			// rather than be silently ACKed: treat it as invalid so the caller
			// dead-letters it.
			rej = ScheduleRejectionInvalid
		}
		out.outcome = scheduleStartRejected
		out.rejection = rej
	default:
		return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected outcome code %d", outcomeCode)
	}
	return out, nil
}

// reserveScheduleRejection atomically reserves a terminal pre-admission
// rejection for a schedule occurrence via reserveScheduleRejectionScript. It
// writes the reservation only when the occurrence was never admitted, so a
// rejection can never cancel a live admission. A message with a pinned descriptor
// reports scheduleRejectionAdmitted, carrying the pinned descriptor for adoption
// AND the invocation marker's classification, so a reserve-only (app-absent)
// caller can complete a terminal disposition while TryStartScheduled still adopts.
// A no-descriptor marker reports scheduleRejectionInFlight (active → pending),
// scheduleRejectionExhausted (each exhausted "<app>/<handler>" + attempt still
// owed a DLQ entry), or scheduleRejectionComplete ("ok" → ACK). It never ACKs and
// never writes a terminal marker, so an invalid reservation leaves the message
// pending until its DLQ placeholder is persisted.
func (store *invocationStore) reserveScheduleRejection(
	ctx context.Context,
	stream, group, msgID string,
	kind ScheduleRejection,
) (scheduleRejectionResult, error) {
	if kind == ScheduleRejectionNone {
		return scheduleRejectionResult{}, errNoRejectionKind
	}
	res, err := reserveScheduleRejectionScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID)},
		terminalField,
		scheduleRejectionField,
		scheduleField,
		encodeScheduleRejection(kind),
	).Slice()
	if err != nil {
		return scheduleRejectionResult{}, err
	}
	if len(res) < 2 {
		// The script always returns at least two elements; a short reply is a
		// protocol violation, so treat the reservation as unconfirmed rather than
		// guess.
		return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: unexpected script reply of length %d", len(res))
	}
	code := toInt64(res[0])
	value, ok := res[1].(string)
	if !ok {
		return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: unexpected reply type %T", res[1])
	}
	out := scheduleRejectionResult{}
	switch code {
	case 0: // terminal-retained
		out.outcome = scheduleRejectionTerminal
	case 1: // already reserved: the stored kind is authoritative
		out.outcome = scheduleRejectionAlreadyReserved
		if rej, ok := decodeScheduleRejection(value); ok {
			out.kind = rej
		} else {
			out.kind = ScheduleRejectionInvalid
		}
	case 2: // admitted first: adopt the pinned descriptor + marker classification
		desc, ok := decodeScheduleDescriptor(value)
		if !ok {
			return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: pinned descriptor does not decode")
		}
		kindStr, ok := res[2].(string)
		if !ok {
			return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: unexpected marker kind reply type %T", res[2])
		}
		mk, markers, merr := decodeScheduleMarkerPayload(kindStr, res[3:])
		if merr != nil {
			return scheduleRejectionResult{}, merr
		}
		out.outcome = scheduleRejectionAdmitted
		out.pinned, out.hasPinned = desc, true
		out.markerKind, out.markers = mk, markers
	case 3: // reserved by this call
		out.outcome = scheduleRejectionReserved
		if rej, ok := decodeScheduleRejection(value); ok {
			out.kind = rej
		} else {
			out.kind = kind
		}
	case 4: // no-descriptor admitted marker: classify the terminal disposition
		mk, markers, merr := decodeScheduleMarkerPayload(value, res[2:])
		if merr != nil {
			return scheduleRejectionResult{}, merr
		}
		out.markerKind, out.markers = mk, markers
		switch mk {
		case scheduleMarkerActive:
			out.outcome = scheduleRejectionInFlight
		case scheduleMarkerComplete:
			out.outcome = scheduleRejectionComplete
		case scheduleMarkerExhausted:
			out.outcome = scheduleRejectionExhausted
		default:
			return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: unexpected marker kind %q", value)
		}
	default:
		return scheduleRejectionResult{}, fmt.Errorf("reserveScheduleRejection: unexpected outcome code %d", code)
	}
	return out, nil
}

// decodeScheduleMarkerPayload parses a reserve reply's marker kind and its
// optional trailing {invocation, attempt} pairs. Only 'exhausted' carries pairs;
// any other kind with trailing elements is a protocol violation. It is the single
// decoder shared by the admitted (code 2) and no-descriptor (code 4) replies.
func decodeScheduleMarkerPayload(kindStr string, rest []any) (scheduleMarkerKind, []scheduleMarker, error) {
	var kind scheduleMarkerKind
	switch kindStr {
	case "active":
		kind = scheduleMarkerActive
	case "complete":
		kind = scheduleMarkerComplete
	case "exhausted":
		kind = scheduleMarkerExhausted
	case "none":
		kind = scheduleMarkerNone
	default:
		return scheduleMarkerNone, nil, fmt.Errorf("reserveScheduleRejection: unexpected marker kind %q", kindStr)
	}
	if kind != scheduleMarkerExhausted {
		if len(rest) != 0 {
			return scheduleMarkerNone, nil, fmt.Errorf("reserveScheduleRejection: %s marker reply has %d unexpected trailing elements", kindStr, len(rest))
		}
		return kind, nil, nil
	}
	markers, err := decodeScheduleMarkers(rest)
	if err != nil {
		return scheduleMarkerNone, nil, err
	}
	return kind, markers, nil
}

// decodeScheduleMarkers parses the trailing {invocation, attempt} pairs of an
// "exhausted" reserve reply into scheduleMarkers. A malformed pair is a protocol
// violation (the script writes only field names and canonical attempts), so it is
// refused rather than guessed. An attempt must be >= 1 (the marker grammar
// enforces it) and the invocation must be a non-empty "<app>/<handler>" field.
func decodeScheduleMarkers(rest []any) ([]scheduleMarker, error) {
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil, fmt.Errorf("reserveScheduleRejection: exhausted reply has %d trailing elements, want a positive even count", len(rest))
	}
	out := make([]scheduleMarker, 0, len(rest)/2)
	for i := 0; i < len(rest); i += 2 {
		inv, ok := rest[i].(string)
		if !ok {
			return nil, fmt.Errorf("reserveScheduleRejection: exhausted invocation reply type %T", rest[i])
		}
		if inv == "" {
			return nil, fmt.Errorf("reserveScheduleRejection: exhausted reply has an empty invocation")
		}
		attempt := int(toInt64(rest[i+1]))
		if attempt < 1 {
			return nil, fmt.Errorf("reserveScheduleRejection: exhausted invocation %q has attempt %d, want >= 1", inv, attempt)
		}
		out = append(out, scheduleMarker{Invocation: inv, Attempt: attempt})
	}
	return out, nil
}

// finishFailure records a failed attempt by CASing the caller's claim against
// the current active marker and, on a match, atomically persisting a
// "next_attempt_at" marker so the invocation is gated by its retry backoff until
// now+backoff. The script reads the current marker and:
//
//   - writes next_attempt_at ONLY when the current running/next-attempt marker
//     is owned by the caller's exact claim (attempt AND token). A stale owner
//     whose running deadline elapsed and whose invocation was re-claimed by a
//     newer claim CASes against a different attempt/token and is a no-op, so it
//     can never overwrite the newer marker;
//   - preserves a terminal "ok" or exhausted marker, and an absent/unparseable
//     marker, as a no-op (returns false);
//
// The returned bool reports whether the transition was applied. A false is an
// explicit stale/terminal result, NOT a Redis error. The HSET and PERSIST happen
// in the same script, so the backoff marker keeps the hash PERSISTENT (the
// message is still recoverable and must not expire before its deadline). A
// transport error is returned so the caller can leave the field as-is
// (immediately eligible — at-least-once).
func (store *invocationStore) finishFailure(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	claim InvocationClaim,
	backoff time.Duration,
	now time.Time,
) (bool, error) {
	if !claim.valid() {
		return false, nil
	}
	key := invocationStateKey(stream, group, msgID)
	next := now.Add(backoff)
	ok, err := finishFailureScript.Run(ctx, store.client, []string{key},
		invocation,
		claim.Attempt,
		claim.Token,
		strconv.FormatInt(next.UnixMilli(), 10),
		terminalField,
	).Int64()
	if err != nil {
		return false, err
	}
	return ok == 1, nil
}

// markExhausted records that the invocation's attempts are exhausted via the
// atomic markExhausted script, CASed against the caller's claim (attempt +
// token). It writes the terminal "exhausted:<attempt>:<token>" marker (retaining
// the claim identity for the later DLQ upgrade) so a redelivery skips it without
// re-running, but deliberately does NOT add the ":dlq" suffix: the DLQ entry has
// not been persisted yet. A terminal/already-ok/absent marker, or a stale claim,
// is a no-op (returns false). It returns false for those refusals (an explicit
// stale result, never a Redis error).
func (store *invocationStore) markExhausted(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	claim InvocationClaim,
) (bool, error) {
	if !claim.valid() {
		return false, nil
	}
	key := invocationStateKey(stream, group, msgID)
	ok, err := markExhaustedScript.Run(ctx, store.client, []string{key},
		invocation,
		claim.Attempt,
		claim.Token,
		terminalField,
	).Int64()
	if err != nil {
		return false, err
	}
	return ok == 1, nil
}

// persistDLQ atomically appends one DLQ entry to dlqStream and records its
// persistence in the message's invocation-state hash via the atomic
// persistDLQScript (see its contract). For a real invocation it CASes the
// retained exhausted identity (attempt + token); for the placeholder
// (invocation == "") it records the message-scoped dlqPlaceholderField. It is
// called only after the entry's fields are built, so the append and the marker
// can never diverge.
//
// wrote reports whether this call appended the entry; wrote=false with err=nil
// means the entry/placeholder was already recorded (a no-op, so no new entry and
// no DLQ counter increment). A refused store verdict (wrong hash type,
// terminal-retained hash, or a missing/foreign exhausted marker) and a
// Redis/transport error both return err != nil: nothing was written and the
// caller must leave the message pending (no ACK) because persistence is
// uncertain.
func (store *invocationStore) persistDLQ(
	ctx context.Context,
	dlqStream, stream, group, msgID, invocation string,
	claim InvocationClaim,
	entry map[string]any,
) (bool, error) {
	attempt, token := "", ""
	if invocation != "" {
		attempt = strconv.Itoa(claim.Attempt)
		token = claim.Token
	}
	args := make([]any, 0, 5+2*len(entry))
	args = append(args, terminalField, invocation, dlqPlaceholderField, attempt, token)
	args = append(args, flatDLQEntry(entry)...)
	res, err := persistDLQScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID), dlqStream},
		args...,
	).Int64()
	if err != nil {
		return false, err
	}
	switch res {
	case 1:
		return true, nil
	case 0:
		return false, nil
	default:
		return false, fmt.Errorf("DLQ persistence refused for invocation %q (state %d)", invocation, res)
	}
}

// flatDLQEntry flattens a DLQ entry field map into the ordered key/value ARGV
// list persistDLQScript forwards to XADD. Keys are sorted so the wire arguments
// are deterministic (Redis does not care about field order, but a stable order
// keeps tests and packet captures reproducible).
func flatDLQEntry(entry map[string]any) []any {
	keys := make([]string, 0, len(entry))
	for k := range entry {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := make([]any, 0, 2*len(entry))
	for _, k := range keys {
		// Stringify every value so the script's XADD always receives bulk
		// strings, exactly as the former map-based XAdd encoded them.
		args = append(args, k, fmt.Sprint(entry[k]))
	}
	return args
}

// exhaustedState reads the invocation's exhausted marker, returning its retained
// claim identity (attempt + token), whether its DLQ entry is already persisted
// (the ":dlq" suffix), and whether a well-formed exhausted marker was present.
// redis.Nil (field absent) and any non-exhausted marker return ok=false.
//
// It serves routeToDLQ twice: the dlq flag lets it skip an invocation whose
// entry was already written, and the retained claim identity is the exact CAS
// key for the subsequent atomic persistDLQ append+upgrade, so a stale/foreign
// marker can never be appended or upgraded. A read error returns
// (zero, false, false, err); the caller fails safe by treating the entry as not
// persisted (a duplicate is allowed under at-least-once, while skipping a
// required write would lose the entry).
func (store *invocationStore) exhaustedState(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (claim InvocationClaim, dlq bool, ok bool, err error) {
	value, err := store.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return InvocationClaim{}, false, false, nil
	}
	if err != nil {
		return InvocationClaim{}, false, false, err
	}
	exClaim, dlq, ok := parseExhaustedValue(value)
	if !ok {
		return InvocationClaim{}, false, false, nil
	}
	return exClaim, dlq, true, nil
}

// ackAndRetainTerminal acknowledges the message and switches its
// invocation-state hash to terminal retention via ONE atomic script (see
// ackAndRetainTerminalScript). It is the production ACK boundary: it replaces
// the former bare XAck followed by a separate retainTerminal call, whose gap
// could leak a persistent invocation hash once the message had already left the
// PEL. The store shells out to a single EVAL, so the ACK and the
// retention always take effect together (or, on a transport failure, neither
// does).
func (store *invocationStore) ackAndRetainTerminal(
	ctx context.Context,
	stream,
	group,
	msgID string,
	retention time.Duration,
) (terminalAckResult, error) {
	res, err := ackAndRetainTerminalScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID), stream},
		group,
		msgID,
		terminalField,
		retentionTTLMillis(retention),
	).Slice()
	if err != nil {
		return terminalAckResult{}, err
	}
	if len(res) < 2 {
		// The script always returns two elements; a short reply is a protocol
		// violation, so treat the outcome as unconfirmed rather than guess.
		return terminalAckResult{}, fmt.Errorf("ackAndRetainTerminal: unexpected script reply of length %d", len(res))
	}
	return terminalAckResult{
		Removed:  toInt64(res[0]) == 1,
		Retained: toInt64(res[1]) == 1,
	}, nil
}

// retainTerminal switches the message's invocation-state hash to terminal
// retention via the atomic retainTerminal script: it writes the reserved
// terminal marker and applies the retention TTL in one step. It is the
// NON-XACK retention path: it runs only when there is no message to ACK — a
// missing-payload PEL reference the server already purged — plus store-level
// tests. Every production path that ACKs uses ackAndRetainTerminal, which does
// the ACK and the retention in a single script. The marker makes every lifecycle
// script inert, so a stale in-memory delivery cannot mutate the retained state
// or remove the TTL. The upgrade is monotonic: a repeated call never re-extends
// an already-terminal hash. A missing key is a no-op (nothing to retain).
//
// retention is the resolved terminal window: >0 applies the PEXPIRE TTL, while
// <=0 disables retention (the marker is written but the hash is left PERSISTENT,
// never inheriting a residual TTL). The script receives the millisecond value,
// so a disabled window is passed as 0 (see retentionTTLMillis).
func (store *invocationStore) retainTerminal(ctx context.Context, stream, group, msgID string, retention time.Duration) error {
	_, err := retainTerminalScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID)},
		terminalField,
		retentionTTLMillis(retention),
	).Int64()
	return err
}

// makeRecoverable clears any TTL from the message's invocation-state hash so it
// is PERSISTENT while the message is pending. It is the migration hook for a
// hash written by a pre-persistence Relay (or any legacy fixed-TTL write): a
// message that sat pending longer than the old TTL would otherwise lose its
// attempt/reclaim accounting on redelivery. It is a no-op for a missing hash and
// for an already terminal-retained hash (retention must never be removed).
//
// An error is returned to the caller as a Redis-state failure, never swallowed:
// the hash may still carry its old TTL and could expire mid-delivery, so the
// caller must not rely on it, run the handler, or ACK the message. The caller
// leaves the message pending so a later reclaim retries it.
func (store *invocationStore) makeRecoverable(ctx context.Context, stream, group, msgID string) error {
	_, err := makeRecoverableScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID)},
		terminalField,
	).Int64()
	return err
}

// claimClassification atomically claims this message's one-time logical-event
// classification. It uses an atomic Lua script around HSETNX on the reserved
// classificationField: exactly one caller (across redeliveries, reclaims, and
// replicas) receives true and therefore counts the event once; every later
// delivery of the same message sees the field present and receives false. The
// claim is written before the runner classifies, so a crash between the claim
// and the metric increment can only LOSE a count for that event — it can never
// double-count one. The key is PERSISTed in the same script, but only when the
// claim actually wrote the field, so repeated losers add no writes.
//
// The claim lives and dies with the message's invocation-state hash: a terminal
// path (successful ACK, obsolete ACK, or DLQ routing) switches that hash to
// terminal retention only after the message leaves the PEL, and the retention
// marker makes a stale claim request report "already taken". While the message
// is recoverable the hash is persistent, so the claim can never expire out from
// under a redelivery and re-classify the same logical event.
//
// On a Redis error it returns (false, err): the caller must NOT count the
// event, because it cannot prove the claim. Failing open here would risk
// double-counting on redelivery, and classification counters are exact
// partition counts, not at-least-once accounting.
func (store *invocationStore) claimClassification(ctx context.Context, stream, group, msgID string) (bool, error) {
	key := invocationStateKey(stream, group, msgID)
	set, err := claimClassificationScript.Run(ctx, store.client, []string{key},
		classificationField,
		terminalField,
	).Int()
	if err != nil {
		return false, err
	}
	return set == 1, nil
}

// traceReference reads the compact trace lineage recorded for this invocation by
// its most recent attempt, under the reserved sibling field. redis.Nil (never
// recorded) is ("", nil). A read error returns ("", err); the caller treats it
// as no reference (best-effort lineage must never fail an invocation).
func (store *invocationStore) traceReference(ctx context.Context, stream, group, msgID, invocation string) (string, error) {
	value, err := store.client.HGet(ctx, invocationStateKey(stream, group, msgID), traceField(invocation)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return value, nil
}

// scheduleDescriptor reads the message's pinned schedule admission descriptor
// from the reserved scheduleField. redis.Nil (field absent) and a value that does
// not decode both report ok=false (no pinned descriptor), so a foreign or corrupt
// marker degrades to "resolve the current template" rather than being misread.
func (store *invocationStore) scheduleDescriptor(ctx context.Context, stream, group, msgID string) (ScheduleDescriptor, bool, error) {
	value, err := store.client.HGet(ctx, invocationStateKey(stream, group, msgID), scheduleField).Result()
	if err == redis.Nil {
		return ScheduleDescriptor{}, false, nil
	}
	if err != nil {
		return ScheduleDescriptor{}, false, err
	}
	desc, ok := decodeScheduleDescriptor(value)
	return desc, ok, nil
}

// recordTrace persists the compact trace lineage of this invocation's most
// recent attempt under the reserved sibling field, via one atomic Lua script
// that also clears any legacy TTL (the message is recoverable, so its state must
// be persistent). Only the lifecycle hash field is touched; the invocation's
// lifecycle value is never disturbed, and a terminal-retained hash is left
// untouched. An empty lineage is a no-op (nothing to record).
func (store *invocationStore) recordTrace(ctx context.Context, stream, group, msgID, invocation, lineage string) error {
	if lineage == "" {
		return nil
	}
	key := invocationStateKey(stream, group, msgID)
	_, err := recordTraceScript.Run(ctx, store.client, []string{key},
		traceField(invocation),
		lineage,
		terminalField,
	).Int64()
	return err
}

// runningValue encodes a protected attempt as the field value
// "running:<deadline_ms>:<attempt>:<token>". The "running:" prefix distinguishes
// it from the "ok" completion sentinel; the deadline is the integer Unix-ms at
// which the attempt is considered abandoned; the token is the opaque claim
// identity.
func runningValue(deadline time.Time, claim InvocationClaim) string {
	return "running:" + strconv.FormatInt(deadline.UnixMilli(), 10) + ":" +
		strconv.Itoa(claim.Attempt) + ":" + claim.Token
}

// nextAttemptValue encodes a failed attempt's retry deadline and claim as
// "next_attempt_at:<deadline_ms>:<attempt>:<token>".
func nextAttemptValue(deadline time.Time, claim InvocationClaim) string {
	return "next_attempt_at:" + strconv.FormatInt(deadline.UnixMilli(), 10) + ":" +
		strconv.Itoa(claim.Attempt) + ":" + claim.Token
}

// exhaustedValue encodes the terminal exhausted state, retaining the exhausted
// claim's attempt and token so the later DLQ upgrade can CAS it. With
// dlqPersisted false it is "exhausted:<attempt>:<token>"; with true it appends
// the ":dlq" suffix.
func exhaustedValue(claim InvocationClaim, dlqPersisted bool) string {
	value := "exhausted:" + strconv.Itoa(claim.Attempt) + ":" + claim.Token
	if dlqPersisted {
		value += ":dlq"
	}
	return value
}

// validClaimToken reports whether tok is a well-formed claim token: a non-empty
// run of lowercase hex digits, matching the Lua parser's [0-9a-f]+ (tokens are
// generated by newClaimToken). It is strict so a corrupt token can never be
// mistaken for a real claim.
func validClaimToken(tok string) bool {
	if tok == "" {
		return false
	}
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return len(tok) <= 64
}

// isExhaustedDLQValue reports whether v is a valid "exhausted:<attempt>:<token>:dlq"
// marker (attempt >= 1, well-formed token), i.e. the invocation's DLQ entry has
// been persisted. It is strict so a corrupt or near-miss marker can never be
// mistaken for a persisted entry and cause a required DLQ write to be skipped.
func isExhaustedDLQValue(v string) bool {
	_, dlq, ok := parseExhaustedValue(v)
	return ok && dlq
}

// parseExhaustedValue parses "exhausted:<attempt>:<token>[:dlq]" into its claim
// and DLQ-persistence flag. ok=false for any malformed value.
func parseExhaustedValue(v string) (claim InvocationClaim, dlq bool, ok bool) {
	rest, found := strings.CutPrefix(v, "exhausted:")
	if !found {
		return InvocationClaim{}, false, false
	}
	if suffix, trimmed := strings.CutSuffix(rest, ":dlq"); trimmed {
		rest = suffix
		dlq = true
	}
	attemptStr, token, found := strings.Cut(rest, ":")
	if !found {
		return InvocationClaim{}, false, false
	}
	if !canonicalUint(attemptStr) {
		return InvocationClaim{}, false, false
	}
	n, err := strconv.Atoi(attemptStr)
	if err != nil || n < 1 || !validClaimToken(token) {
		return InvocationClaim{}, false, false
	}
	return InvocationClaim{Attempt: n, Token: token}, dlq, true
}

// parseActiveValue parses "<deadline_ms>:<attempt>:<token>" (the suffix after a
// "running:"/"next_attempt_at:" prefix) into its deadline and claim. ok=false
// for any malformed value.
func parseActiveValue(s string) (deadline time.Time, claim InvocationClaim, ok bool) {
	dlStr, rest, found := strings.Cut(s, ":")
	if !found {
		return time.Time{}, InvocationClaim{}, false
	}
	attemptStr, token, found := strings.Cut(rest, ":")
	if !found {
		return time.Time{}, InvocationClaim{}, false
	}
	if !canonicalUint(dlStr) {
		return time.Time{}, InvocationClaim{}, false
	}
	dlMs, err := strconv.ParseInt(dlStr, 10, 64)
	if err != nil {
		return time.Time{}, InvocationClaim{}, false
	}
	n, err := strconv.Atoi(attemptStr)
	if err != nil || n < 1 || !validClaimToken(token) {
		return time.Time{}, InvocationClaim{}, false
	}
	return time.UnixMilli(dlMs), InvocationClaim{Attempt: n, Token: token}, true
}

// canonicalUint reports whether s is a canonical non-negative decimal integer:
// a non-empty run of digits with no leading zero (so "0" is canonical, "01" is
// not). The Lua eligibility compare (lt_uint) is exact only for canonical
// values, so the Go parser rejects non-canonical deadlines too, keeping the two
// sides consistent and treating a corrupt deadline as eligible.
func canonicalUint(s string) bool {
	if s == "" {
		return false
	}
	if len(s) > 1 && s[0] == '0' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// parseInvocationState decodes a field value into its kind, deadline (for
// active markers), and the claim's attempt count. It returns ok=false for any
// value that is not a well-formed marker (e.g. a corrupt marker), which the
// caller treats as eligible.
func parseInvocationState(v string) (kind invocationKind, deadline time.Time, attempts int, ok bool) {
	switch {
	case v == "ok":
		return kindComplete, time.Time{}, 0, true
	case strings.HasPrefix(v, "running:"):
		dl, claim, ok := parseActiveValue(v[len("running:"):])
		if !ok {
			return kindEligible, time.Time{}, 0, false
		}
		return kindRunning, dl, claim.Attempt, true
	case strings.HasPrefix(v, "next_attempt_at:"):
		dl, claim, ok := parseActiveValue(v[len("next_attempt_at:"):])
		if !ok {
			return kindEligible, time.Time{}, 0, false
		}
		return kindNextAttempt, dl, claim.Attempt, true
	case strings.HasPrefix(v, "exhausted:"):
		claim, _, ok := parseExhaustedValue(v)
		if !ok {
			return kindEligible, time.Time{}, 0, false
		}
		return kindExhausted, time.Time{}, claim.Attempt, true
	default:
		return kindEligible, time.Time{}, 0, false
	}
}
