package stream

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// DefaultInvocationRetention is the fallback terminal invocation-state retention
// window applied when a ConsumerConfig does not explicitly configure one (a nil
// InvocationRetention pointer). It mirrors config.DefaultInvocationRetention
// (48h); a leaf package cannot import config, so the default lives here too. The
// worker always passes the config-resolved value, so this fallback is for direct
// NewConsumer callers and internal construction only.
//
// It is how long a message's invocation-state hash survives AFTER the message
// has left the PEL — a successful atomic ack+retain on the success, obsolete, or
// DLQ path, or a purged missing-payload PEL reference retained without an ACK.
// Recoverable state (any hash whose message is still pending and can therefore
// be redelivered) is deliberately PERSISTENT, with NO TTL: it must never expire
// out from under a redelivery, which would reset the attempt/reclaim accounting.
// The retention TTL is applied only once the message is no longer recoverable:
// by ackAndRetainTerminal (the ACK and the marker+TTL in one atomic script) for
// every path that ACKs, or by retainTerminal (marker+TTL only, no ACK) for a
// missing-payload PEL reference the server already purged.
//
// It is a retention window for terminal bookkeeping (so a completed/exhausted
// marker survives long enough to be inspected) and a cleanup safety net: if the
// retention never runs, the hash is simply leaked rather than lost — a
// persistent leak is strictly safer than premature state loss. Since the ACK and
// the retention share one atomic script, there is no longer a crash window in
// which a hash is left persistent after its message has left the PEL; a
// failed ACK stages only a PERSISTent marker (no TTL) and the TTL is applied
// once the ACK succeeds.
const DefaultInvocationRetention = 48 * time.Hour

// InvocationClaim identifies ONE successful claim of an invocation's execution
// slot. It is the compare-and-set identity every active-claim-originated
// transition must present:
//
//   - Attempt is the 1-based handler attempt of this claim (the count that
//     drives retry/exhaustion, persisted as handler_attempts).
//   - Token is a crypto-random, opaque nonce generated immediately before the
//     claim's EVAL. The script writes it only when the claim wins, so it is a
//     unique per-claim identity that cannot be guessed or shared.
//
// Token is deliberately opaque: it must never be logged, used as a metric
// label, or persisted to the DLQ. It exists only so the store can reject a
// stale owner whose attempt number happens to coincide with the live marker
// (e.g. an attempt whose lease was reclaimed by a flow that re-used the count);
// the attempt number alone is not a sufficient CAS key.
type InvocationClaim struct {
	Attempt int
	Token   string
}

// valid reports whether the claim carries a usable attempt and token. A zero
// claim (no confirmed claim) is rejected by every CAS transition rather than
// reaching Redis.
func (c InvocationClaim) valid() bool {
	return c.Attempt >= 1 && c.Token != ""
}

// ScheduleDescriptor is the immutable admission contract of ONE schedule
// occurrence: the schedule's stable NAME, the exact handler it admitted, the
// capped handler timeout, and the retry budget. It is NOT sensitive (no secret,
// no payload) and is persisted in the invocation-state hash, so it may be read
// back on any delivery.
//
// It exists to freeze a schedule occurrence's execution contract at its FIRST
// admission. Before admission the occurrence resolves the CURRENT template by
// schedule NAME on every delivery (a handler change under the same name takes
// effect, and a removed name is obsolete). After the first successful admission
// the descriptor pins handler/timeout/retries, so a later template change (or the
// schedule's removal) can no longer reset the claim/attempts or cancel an
// invocation that is mid-retry/DLQ: it completes under the contract it started
// with. The schedule NAME remains the occurrence/dedup identity; the descriptor
// is execution provenance only, and invocation/DLQ attribution stays
// handler-based ("<app>/<handler>").
type ScheduleDescriptor struct {
	// Schedule is the schedule's stable name (the template schedules[].name).
	Schedule string
	// Handler is the exact handler admitted ("module.function").
	Handler string
	// Timeout is the capped per-invocation handler timeout.
	Timeout time.Duration
	// Retries is the retry budget (additional attempts after the first).
	Retries int
}

// scheduleDescriptorPrefix versions the encoded descriptor grammar. A value that
// does not decode (a corrupt, future, or legacy value in the reserved field) is
// treated as absent, so a foreign marker can never be mistaken for a descriptor.
const scheduleDescriptorPrefix = "sd1"

// encodeScheduleDescriptor renders a descriptor as the opaque value stored in the
// reserved scheduleField. The names are base64url-encoded so the ":"-separated
// grammar is unambiguous regardless of name contents; the timeout is integer
// milliseconds and the retries a non-negative integer.
func encodeScheduleDescriptor(d ScheduleDescriptor) string {
	return scheduleDescriptorPrefix + ":" +
		strconv.FormatInt(d.Timeout.Milliseconds(), 10) + ":" +
		strconv.Itoa(d.Retries) + ":" +
		base64.RawURLEncoding.EncodeToString([]byte(d.Schedule)) + ":" +
		base64.RawURLEncoding.EncodeToString([]byte(d.Handler))
}

// decodeScheduleDescriptor parses the encoded descriptor value. ok=false for any
// malformed value (wrong prefix, missing/extra fields, non-canonical timeout or
// retries, bad base64), so a corrupt field degrades to "no pinned descriptor"
// rather than being misread.
func decodeScheduleDescriptor(v string) (ScheduleDescriptor, bool) {
	rest, found := strings.CutPrefix(v, scheduleDescriptorPrefix+":")
	if !found {
		return ScheduleDescriptor{}, false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != 4 {
		return ScheduleDescriptor{}, false
	}
	timeoutMs, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || timeoutMs < 0 || !canonicalUint(parts[0]) {
		return ScheduleDescriptor{}, false
	}
	retries, err := strconv.Atoi(parts[1])
	if err != nil || retries < 0 || !canonicalUint(parts[1]) {
		return ScheduleDescriptor{}, false
	}
	schedule, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return ScheduleDescriptor{}, false
	}
	handler, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil {
		return ScheduleDescriptor{}, false
	}
	return ScheduleDescriptor{
		Schedule: string(schedule),
		Handler:  string(handler),
		Timeout:  time.Duration(timeoutMs) * time.Millisecond,
		Retries:  retries,
	}, true
}

// ScheduleRejection classifies a TERMINAL pre-admission rejection of a schedule
// occurrence: the occurrence was never admitted and can never be admitted, so it
// must be dispositioned without running a handler. The two kinds are distinct and
// must stay distinct end to end:
//
//   - ScheduleRejectionObsolete: the schedule NAME (or its app) is gone from the
//     current configuration. The removal is intentional, so the occurrence is
//     ACKed — never retried, never dead-lettered.
//   - ScheduleRejectionInvalid: the occurrence is well-formed as an envelope but
//     can never succeed against the current template (a non-firing scheduled_at,
//     or an unparseable current cron). It is terminal and non-retryable, so it is
//     dead-lettered.
//
// A rejection is RESERVED in the invocation-state hash before any disposition
// (see reserveScheduleRejectionScript), and the reservation is authoritative for
// every later delivery: admission refuses a reserved message, and a retry uses
// the reserved kind rather than re-deriving it from a possibly-changed template.
// ScheduleRejectionNone means "no rejection": the caller has a descriptor to
// propose.
type ScheduleRejection int

const (
	// ScheduleRejectionNone means no rejection: the caller proposes a descriptor.
	ScheduleRejectionNone ScheduleRejection = iota
	// ScheduleRejectionObsolete is a removed schedule/app: ACK, never DLQ.
	ScheduleRejectionObsolete
	// ScheduleRejectionInvalid is a non-firing/corrupt occurrence: DLQ.
	ScheduleRejectionInvalid
)

// errNoRejectionKind is returned by reserveScheduleRejection when the caller
// supplies no rejection kind: reserving an empty kind would be meaningless and
// is a programming error, so it is refused rather than written.
var errNoRejectionKind = errors.New("schedule rejection requires a kind")

// scheduleRejectionObsoleteValue and scheduleRejectionInvalidValue are the
// persisted encodings of the reserved rejection field. They are stable strings
// (not numbers) so the invocation-state hash stays inspectable with redis-cli.
const (
	scheduleRejectionObsoleteValue = "obsolete"
	scheduleRejectionInvalidValue  = "invalid"
)

// encodeScheduleRejection renders a rejection kind as the value stored in the
// reserved scheduleRejectionField. ScheduleRejectionNone encodes as "" (no
// reservation).
func encodeScheduleRejection(r ScheduleRejection) string {
	switch r {
	case ScheduleRejectionObsolete:
		return scheduleRejectionObsoleteValue
	case ScheduleRejectionInvalid:
		return scheduleRejectionInvalidValue
	default:
		return ""
	}
}

// decodeScheduleRejection parses a reserved rejection value. ok=false for
// ScheduleRejectionNone and any unrecognized value; the caller treats an
// unrecognized (corrupt/foreign) reservation as INVALID so it surfaces in the
// DLQ (recoverable, inspectable) rather than being silently ACKed as removed
// configuration.
func decodeScheduleRejection(v string) (ScheduleRejection, bool) {
	switch v {
	case scheduleRejectionObsoleteValue:
		return ScheduleRejectionObsolete, true
	case scheduleRejectionInvalidValue:
		return ScheduleRejectionInvalid, true
	default:
		return ScheduleRejectionNone, false
	}
}

// ScheduleAdmission is the resolved result of the schedule admission boundary as
// seen by the runner: the descriptor that owns the message, whether this caller
// claimed the invocation, whether the occurrence was terminally REJECTED before
// admission (never admitted and never admissible), whether an admission exists
// that this caller must NOT act on (InFlight: keep the message pending), and
// whether a legacy no-descriptor admitted invocation still owes a DLQ
// disposition (ExhaustedInvocations). A zero ScheduleAdmission with
// Started/Rejected/InFlight all false and no ExhaustedInvocations means the
// invocation is protected or terminal (Claim/Wait carry the marker details,
// exactly like TryStart).
type ScheduleAdmission struct {
	// Started is true when the caller should execute the handler for this attempt.
	Started bool
	// Claim is the confirmed claim when Started, or the existing marker's claim
	// (Attempt 0 for complete/terminal-retained, >0 for exhausted/protected) when
	// not started.
	Claim InvocationClaim
	// Wait is the duration until the invocation becomes eligible again; > 0 means
	// it is protected by an active running deadline or a retry backoff.
	Wait time.Duration
	// Descriptor is the immutable descriptor that owns the message: the pinned
	// one (adopted if this caller's proposal lost a race), or the caller's
	// proposal on a successful first pin. It is the single source of truth for
	// the handler, capped timeout, and retry budget for this message from now on,
	// and is always set unless Rejected, InFlight, or ExhaustedInvocations is set.
	Descriptor ScheduleDescriptor
	// Rejected is true when the occurrence was terminally rejected before
	// admission and can never be admitted: no descriptor is pinned and none ever
	// will be. The rejection is reserved in the invocation-state hash, so every
	// later delivery (including a redelivery after a crash) reports the SAME
	// Rejection and dispositions it identically.
	Rejected bool
	// Rejection distinguishes the two terminal rejection kinds: obsolete (removed
	// configuration → ACK) or invalid (non-firing/corrupt → DLQ). It is set only
	// when Rejected is true.
	Rejection ScheduleRejection
	// InFlight is true when the message is already ADMITTED but this caller must
	// NOT act on the admission: a reserve-only caller whose app is absent and
	// cannot execute the pinned descriptor, or a rejection arriving after an
	// admission whose descriptor is not pinned and whose marker is ACTIVE
	// (a legacy/pre-pinning running or retry-backoff claim that may still
	// complete). The message must stay PENDING — never ACKed, never DLQ'd — so a
	// replica that can run the invocation completes it. It is mutually exclusive
	// with Started, Rejected, and ExhaustedInvocations.
	InFlight bool
	// ExhaustedInvocations is set when a legacy/pre-pinning no-descriptor exhausted
	// marker (or several, under different handler identities) owns the message:
	// every listed invocation is already exhausted and still owes a per-invocation
	// DLQ entry. The runner returns a HandlerExhaustedError carrying them, so the
	// stream validates each marker and persists the missing PostgreSQL rows
	// idempotently, then ACKs. It is mutually exclusive with Started, Rejected,
	// and InFlight. A no-descriptor legacy "ok" marker instead yields a ZERO
	// admission (nothing to do: the completion ACK is the normal success tail).
	ExhaustedInvocations []ExhaustedInvocation
}
