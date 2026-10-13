package stream

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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

// claimTokenBytes is the size of the crypto-random per-claim nonce. 16 bytes
// (128 bits) makes a token collision between two independent claims
// computationally impossible, which is what lets the token — not the attempt
// number — be the authoritative claim identity. The token is opaque: it is never
// logged, metricked, or written to the DLQ.
const claimTokenBytes = 16

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
	// stream persists the missing entries (idempotently — an already-":dlq" marker
	// is skipped) and then ACKs. It is mutually exclusive with Started, Rejected,
	// and InFlight. A no-descriptor legacy "ok" marker instead yields a ZERO
	// admission (nothing to do: the completion ACK is the normal success tail).
	ExhaustedInvocations []ExhaustedInvocation
}

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

// The lifecycle transitions below are implemented as atomic Lua scripts (EVAL)
// rather than read-then-write pipelines. A pipeline is not atomic: two replicas
// can both HGET "absent"/"elapsed" and both HSET a running marker, permitting a
// dual start, and a stale owner's failure can overwrite a newer attempt's marker
// after the stale attempt's deadline elapsed. Redis executes a script atomically
// (single-threaded), so at most one concurrent EVAL observes a marker that is
// absent or past its deadline and claims it; every other caller observes the new
// marker and is protected.
//
// Timestamps are integer Unix MILLISECONDS passed as decimal STRINGS end to end.
// The eligibility comparison is a length-then-lexicographic compare of those
// strings (lt_uint), which is mathematically exact for canonical non-negative
// decimal integers and uses no tonumber at all. No timestamp arithmetic happens
// in Lua: Go computes the TTL, the wait, and the deadline. This removes the
// IEEE-754 double comparison hazard entirely — a running marker is protected iff
// now_ms < deadline_ms and eligible iff now_ms >= deadline_ms, with no skew
// fudge.

// luaInvocationHelpers is the shared Lua preamble for every invocation-state
// script. It parses the marker grammar WITHOUT any timestamp arithmetic:
// deadlines stay decimal strings and eligibility is a length-then-lexicographic
// compare of canonical non-negative decimal integers (lt_uint), which is exact
// for any magnitude. Attempt numbers are tiny and may be converted with tonumber;
// claim tokens are opaque hex strings and are NEVER converted.
const luaInvocationHelpers = `
local function lt_uint(a, b)
  -- Both a, b are canonical non-negative decimal strings (no leading zeros):
  -- fewer digits => smaller; equal length => lexicographic compare. A
  -- non-canonical deadline is rejected by parse_active below, so this stays
  -- exact.
  if #a ~= #b then return #a < #b end
  return a < b
end

local function parse_active(v, prefix)
  -- "prefix:<deadline_ms>:<attempt>:<token>" -> deadline, attempt, token.
  -- The deadline must be canonical ([1-9]%d*): Unix-ms is always positive with
  -- no leading zeros, so a leading-zero value is corrupt and is treated as
  -- eligible rather than compared inexactly.
  local rest = string.match(v, '^'..prefix..':(.*)$')
  if not rest then return nil end
  local dl, a, tok = string.match(rest, '^([1-9]%d*):(%d+):([0-9a-f]+)$')
  if not dl then return nil end
  if tonumber(a) < 1 then return nil end
  return dl, a, tok
end

local function parse_exhausted(v)
  -- "exhausted:<attempt>:<token>[:dlq]" -> attempt, token, dlq
  local body = string.match(v, '^exhausted:(.*)$')
  if not body then return nil end
  local dlq = false
  if string.sub(body, -4) == ':dlq' then
    dlq = true
    body = string.sub(body, 1, #body - 4)
  end
  local a, tok = string.match(body, '^([1-9]%d*):([0-9a-f]+)$')
  if not a then return nil end
  return a, tok, dlq
end
`

var (
	// tryStartScript atomically claims an invocation for a new attempt when
	// eligible, or reports why it is not (protected by an active deadline, or
	// terminal). The claim token is generated by Go and supplied here; it is
	// written only on a winning claim, so the token identifies exactly one
	// confirmed claim. See invocationStore.tryStart for the full contract.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field, ARGV[2] = now (unix-ms string),
	// ARGV[3] = new running deadline (unix-ms string), ARGV[4] = new claim
	// token, ARGV[5] = terminal marker field.
	// Returns {started, attempt, deadline_or_0}: started 1 when claimed and
	// attempt is the new 1-based attempt; otherwise attempt describes the
	// existing marker (0 for "ok") and the third element is the existing
	// deadline string when protected (so Go computes the exact wait in ms), or
	// "0" when terminal.
	//
	// A winning claim PERSISTs the key: while the message is recoverable its
	// state must never expire (a legacy TTL is removed in the same atomic step).
	tryStartScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[5]) == 1 then
  -- Terminal-retained (its message already left the PEL): never re-open it.
  return {0, 0, '0'}
end
local v = redis.call('HGET', KEYS[1], ARGV[1])
local now = ARGV[2]
if v then
  if v == 'ok' then
    return {0, 0, '0'}
  end
  local ea = parse_exhausted(v)
  if ea then
    return {0, tonumber(ea), '0'}
  end
  local dl, a = parse_active(v, 'running')
  if not dl then dl, a = parse_active(v, 'next_attempt_at') end
  if dl then
    -- Protected strictly while now < deadline (exact integer-ms compare).
    if lt_uint(now, dl) then
      return {0, tonumber(a), dl}
    end
    -- Deadline reached (now >= deadline): eligible. Carry the attempt forward.
    local na = tonumber(a) + 1
    redis.call('HSET', KEYS[1], ARGV[1], 'running:'..ARGV[3]..':'..na..':'..ARGV[4])
    redis.call('PERSIST', KEYS[1])
    return {1, na, '0'}
  end
end
-- Absent/unparseable: a fresh claim starts at attempt 1.
redis.call('HSET', KEYS[1], ARGV[1], 'running:'..ARGV[3]..':1:'..ARGV[4])
redis.call('PERSIST', KEYS[1])
return {1, 1, '0'}
`)

	// tryStartScheduledScript atomically performs the SCHEDULE admission: it pins
	// the caller's ScheduleDescriptor (if none is pinned yet) and claims the
	// invocation in the SAME script, so the descriptor, the attempt, and the
	// claim token become visible together. It is used only by the schedule path;
	// the event path uses tryStartScript unchanged.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field (the one derived from the descriptor the caller
	//           will run), ARGV[2] = now (unix-ms string), ARGV[3] = new running
	//           deadline (unix-ms string, computed by Go from the descriptor it
	//           executes), ARGV[4] = new claim token, ARGV[5] = terminal marker
	//           field, ARGV[6] = schedule-descriptor field, ARGV[7] = the caller's
	//           descriptor (its own proposal, or the one it already pinned and
	//           verified; "" when it has none), ARGV[8] = schedule-rejection field.
	//
	// Returns a reply {outcome, attempt, deadline, pinned} (plus a fifth element
	// {rejection} for outcome 5):
	//
	//	0 started       attempt = new 1-based attempt; pinned = owning descriptor
	//	1 protected     attempt = existing; deadline = existing deadline string;
	//	                pinned = owning descriptor
	//	2 terminal      attempt = existing (0 for "ok"/retained); pinned = owning
	//	                descriptor (or "")
	//	3 conflict      pinned = the descriptor already pinned, which the caller
	//	                must adopt and retry (it either had none, or disagreed);
	//	                no claim was made
	//	4 no admission  no descriptor is pinned and the caller has none → the
	//	                occurrence was never admitted and its schedule is gone
	//	                (obsolete). No write: the caller must RESERVE the rejection
	//	                (reserveScheduleRejectionScript) before dispositioning it,
	//	                so the decision becomes atomic against a concurrent
	//	                admission.
	//	5 rejected      a terminal pre-admission rejection was already reserved for
	//	                this message; rejection = its kind. No claim was made and
	//	                the caller must NOT pin or admit: it dispositions the
	//	                reserved kind instead.
	//
	// The claim is made ONLY when the caller supplies the exact descriptor it will
	// execute (proposed == pinned, or it is the first to pin proposed). This keeps
	// the persisted running deadline consistent with the descriptor: Go derives
	// the deadline from the descriptor it executes, so the caller must know it
	// before claiming. A caller that only knows the pinned descriptor by adopting
	// it re-enters with it, which terminates because the pinned descriptor is
	// immutable while the message is recoverable.
	tryStartScheduledScript = redis.NewScript(luaInvocationHelpers + `
local function claim(v, now)
  -- Shared eligibility/claim decision over the caller's invocation field value.
  -- Returns one of:
  --   {'started', attempt}
  --   {'protected', attempt, deadline}
  --   {'terminal', attempt}
  --   {'eligible', attempt}   (absent/unparseable: fresh claim at attempt 1)
  if v then
    if v == 'ok' then return {'terminal', 0} end
    local ea = parse_exhausted(v)
    if ea then return {'terminal', tonumber(ea)} end
    local dl, a = parse_active(v, 'running')
    if not dl then dl, a = parse_active(v, 'next_attempt_at') end
    if dl then
      if lt_uint(now, dl) then return {'protected', tonumber(a), dl} end
      return {'eligible', tonumber(a)}
    end
  end
  return {'eligible', 0}
end

if redis.call('HEXISTS', KEYS[1], ARGV[5]) == 1 then
  -- Terminal-retained (its message already left the PEL): never re-open it.
  return {2, 0, '0', redis.call('HGET', KEYS[1], ARGV[6]) or ''}
end

local rejected = redis.call('HGET', KEYS[1], ARGV[8])
if rejected then
  -- A terminal pre-admission rejection was reserved by an earlier delivery (or a
  -- concurrent replica) BEFORE any admission: the occurrence can never be
  -- admitted, so no proposal may pin or claim it. The reserved kind is returned
  -- so the caller dispositions the occurrence exactly as the first delivery
  -- decided, even if the current template has since changed.
  return {5, 0, '0', '', rejected}
end

local pinned = redis.call('HGET', KEYS[1], ARGV[6])
local proposed = ARGV[7]

if pinned then
  -- A descriptor is already pinned by an earlier delivery. Only a caller that
  -- supplies EXACTLY that descriptor may claim; anyone else (no descriptor, or a
  -- different proposal) adopts it and retries, so an admitted occurrence is never
  -- cancelled and never re-admitted under another handler.
  if proposed == '' or proposed ~= pinned then
    return {3, 0, '0', pinned}
  end
elseif proposed == '' then
  -- No descriptor pinned and the caller has none to propose: the occurrence was
  -- never admitted and its schedule is gone — obsolete. Deliberately NO write:
  -- the caller reserves the rejection separately, atomically with the check for
  -- a concurrent admission (see reserveScheduleRejectionScript).
  return {4, 0, '0', ''}
end

local decision = claim(redis.call('HGET', KEYS[1], ARGV[1]), ARGV[2])
if decision[1] == 'protected' then
  return {1, decision[2], decision[3], pinned or proposed}
end
if decision[1] == 'terminal' then
  return {2, decision[2], '0', pinned or proposed}
end
-- Eligible: pin the descriptor (only if absent) and claim, atomically.
if not pinned then
  redis.call('HSET', KEYS[1], ARGV[6], proposed)
end
local na = tonumber(decision[2]) + 1
redis.call('HSET', KEYS[1], ARGV[1], 'running:'..ARGV[3]..':'..na..':'..ARGV[4])
redis.call('PERSIST', KEYS[1])
return {0, na, '0', pinned or proposed}
`)

	// reserveScheduleRejectionScript atomically reserves a TERMINAL pre-admission
	// rejection (invalid or obsolete) for a schedule occurrence, but only when the
	// occurrence was never admitted — so a rejection can never cancel or overwrite
	// a live admission. It is the disposition half of the admission boundary (F-030):
	// tryStartScheduledScript deliberately writes nothing on a no-admission
	// observation, and this script is what makes the subsequent disposition atomic
	// against a concurrent admission.
	//
	// It resolves the race in a fixed precedence:
	//
	//   - the message's hash is terminal-retained (its message already left the
	//     PEL): nothing to reject; no write.
	//   - a reservation already exists (a redelivery after a crash, or a
	//     concurrent replica): the STORED kind is returned unchanged, so the
	//     reservation is written exactly once and is idempotent.
	//   - a descriptor is already pinned (another replica ADMITTED first): the
	//     rejection is REFUSED and the pinned descriptor returned WITH the
	//     invocation-marker classification, so the caller adopts it (the
	//     TryStartScheduled present-app path) or, when it cannot execute an adopted
	//     descriptor (the reserve-only app-absent path), completes the marker's
	//     terminal disposition. A pinned descriptor can accompany a terminal
	//     marker — a current-version delivery pinned + claimed, then marked "ok" or
	//     exhausted, then crashed before its ACK/DLQ — so the pinned branch must
	//     still classify markers rather than blindly report "admitted".
	//   - an ACTIVE invocation marker (running / retry backoff) exists with no
	//     descriptor: the occurrence was admitted by a delivery whose descriptor
	//     is not readable (a legacy/pre-pinning claim, or a hand-built hash) and
	//     that attempt may still complete. The rejection is REFUSED and NO write
	//     happens; the caller keeps the message PENDING.
	//   - a TERMINAL invocation marker exists with no descriptor: a legacy/
	//     pre-pinning admitted invocation already reached success ("ok") or
	//     exhaustion ("exhausted:<n>:<token>[:dlq]"). The rejection is REFUSED and
	//     NO write happens, because a reservation would cancel the owed disposition
	//     and silently drop a DLQ entry. Instead the scan reports the terminal
	//     outcome so the caller completes it: "exhausted" returns each exhausted
	//     "<app>/<handler>" + attempt (so the stream can persist the missing DLQ
	//     entry and ACK), and "ok" reports success (so the caller ACKs). An ACTIVE
	//     marker always wins over a terminal one: if any active marker exists the
	//     scan reports only "active", because the message cannot be terminalized
	//     while an attempt may still complete.
	//   - otherwise the reservation is written and PERSISTed. Every later
	//     tryStartScheduledScript then reports outcome 5 (rejected) and can never
	//     pin or claim, so a rejection that wins the race permanently prevents
	//     admission.
	//
	// The reservation is written while the message is still recoverable (the DLQ
	// entry may not be persisted and the XACK may not have run), so it keeps the
	// key PERSISTENT with NO TTL: a redelivery after a crash must observe it to
	// complete the disposition idempotently, and terminal retention is applied
	// only later by the ACK path. This script never ACKs and never writes a
	// terminal marker: an invalid reservation must not leave the PEL until its DLQ
	// placeholder is persisted.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = terminal marker field, ARGV[2] = schedule-rejection field,
	// ARGV[3] = schedule-descriptor field, ARGV[4] = rejection kind
	// ("obsolete"/"invalid").
	// Returns a reply {outcome, value, markerKind, ...}:
	//
	//	0 terminal          value = ''           (hash terminal-retained; no write)
	//	1 already reserved  value = stored kind  (idempotent; no write)
	//	2 admitted          value = pinned, markerKind = active|complete|exhausted|none
	//	                                          (adopt it; rejection refused)
	//	3 reserved          value = new kind     (this call wrote the reservation)
	//	4 active|complete|exhausted [inv attempt]... (no descriptor; the marker's
	//	                                          disposition; no write)
	//
	// For 'exhausted' (code 2 or 4) the reply continues with repeated
	// {invocation, attempt} pairs for every exhausted marker still owed a DLQ.
	reserveScheduleRejectionScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  return {0, ''}
end
local existing = redis.call('HGET', KEYS[1], ARGV[2])
if existing then
  return {1, existing}
end
-- Classify every invocation marker present (if any), with a fixed precedence:
-- an ACTIVE marker (running/backoff) WINS and keeps the message pending;
-- otherwise a terminal exhausted marker reports each "<app>/<handler>" + attempt
-- still owed a DLQ entry; otherwise a terminal "ok" reports a completion ACK.
-- This runs whether or not a descriptor is pinned, because a pinned descriptor
-- can accompany a terminal marker (a current-version delivery pinned + claimed,
-- then completed/exhausted but crashed before its ACK/DLQ): a rejection must
-- never overwrite that disposition.
local all = redis.call('HGETALL', KEYS[1])
local active = false
local complete = false
local exhausted = {}
for i = 1, #all, 2 do
  local v = all[i+1]
  if v == 'ok' then
    complete = true
  else
    local dl = parse_active(v, 'running')
    if not dl then dl = parse_active(v, 'next_attempt_at') end
    if dl then
      active = true
    else
      local a = parse_exhausted(v)
      if a then
        exhausted[#exhausted+1] = all[i]
        exhausted[#exhausted+1] = a
      end
    end
  end
end
local function marker_payload()
  local out = {}
  if active then
    out[1] = 'active'
    return out
  end
  if #exhausted > 0 then
    out[1] = 'exhausted'
    for j = 1, #exhausted do out[#out+1] = exhausted[j] end
    return out
  end
  if complete then
    out[1] = 'complete'
    return out
  end
  out[1] = 'none'
  return out
end
local pinned = redis.call('HGET', KEYS[1], ARGV[3])
if pinned then
  -- A descriptor is pinned: return it for adoption PLUS the marker
  -- classification, so TryStartScheduled still adopts the descriptor while a
  -- reserve-only (app-absent) caller can complete a terminal disposition.
  local reply = {2, pinned}
  local payload = marker_payload()
  for j = 1, #payload do reply[#reply+1] = payload[j] end
  return reply
end
local payload = marker_payload()
if payload[1] ~= 'none' then
  -- No descriptor, but an admitted marker exists: report its disposition.
  local reply = {4}
  for j = 1, #payload do reply[#reply+1] = payload[j] end
  return reply
end
redis.call('HSET', KEYS[1], ARGV[2], ARGV[4])
redis.call('PERSIST', KEYS[1])
return {3, ARGV[4]}
`)

	// finishFailureScript atomically records a failed attempt's retry backoff,
	// but only when the current ACTIVE marker is STILL owned by the caller's
	// exact claim (both attempt AND token). A stale owner is a no-op, so it can
	// never overwrite a newer attempt's marker; a terminal "ok"/exhausted marker
	// or an absent marker is likewise a no-op (the caller gets recorded=0).
	// There is deliberately no "preserve the failure anyway" branch: a transition
	// with no matching live claim is not ours to write.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field, ARGV[2] = claim attempt, ARGV[3] = claim token,
	// ARGV[4] = next-attempt deadline (unix-ms string), ARGV[5] = terminal
	// marker field.
	// Returns 1 when the marker was written, 0 otherwise.
	//
	// A recorded failure keeps the key PERSISTENT (removing any legacy TTL): the
	// message is still recoverable and its retry accounting must survive.
	finishFailureScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[5]) == 1 then return 0 end
local v = redis.call('HGET', KEYS[1], ARGV[1])
if not v then return 0 end
local dl, a, tok = parse_active(v, 'running')
if not dl then dl, a, tok = parse_active(v, 'next_attempt_at') end
if not dl then return 0 end
if a ~= ARGV[2] or tok ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], 'next_attempt_at:'..ARGV[4]..':'..a..':'..tok)
redis.call('PERSIST', KEYS[1])
return 1
`)

	// markCompleteScript atomically records success, but only when the current
	// ACTIVE marker is owned by the caller's exact claim (attempt + token).
	// An already-"ok" marker is an idempotent success (returns 1). A terminal
	// exhausted marker is preserved (returns 0: success must never downgrade an
	// exhaustion) and an absent/stale marker is refused (returns 0).
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field, ARGV[2] = claim attempt, ARGV[3] = claim token,
	// ARGV[4] = terminal marker field.
	// Returns 1 when complete (written or already ok), 0 when preserved/refused.
	//
	// The success marker is written while the message is still recoverable, so it
	// keeps the key PERSISTENT (no TTL): retention is applied only after the
	// successful XACK, by ackAndRetainTerminal (the ACK and the retention share
	// one atomic script).
	markCompleteScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[4]) == 1 then return 0 end
local v = redis.call('HGET', KEYS[1], ARGV[1])
if v == 'ok' then return 1 end
if not v then return 0 end
local dl, a, tok = parse_active(v, 'running')
if not dl then dl, a, tok = parse_active(v, 'next_attempt_at') end
if not dl then return 0 end
if a ~= ARGV[2] or tok ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], 'ok')
redis.call('PERSIST', KEYS[1])
return 1
`)

	// markExhaustedScript atomically writes the terminal exhausted marker, but
	// only when the current ACTIVE marker is owned by the caller's exact claim
	// (attempt + token). The written value retains the claim identity
	// ("exhausted:<attempt>:<token>") so the later DLQ-persistence upgrade can
	// CAS the same exhausted claim. "ok"/exhausted/absent markers are preserved
	// (returns 0).
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field, ARGV[2] = claim attempt, ARGV[3] = claim token,
	// ARGV[4] = terminal marker field.
	// Returns 1 when written, 0 otherwise.
	//
	// The exhaustion marker is written while the message is still recoverable
	// (the DLQ entry may not be persisted and the XACK may not have run), so it
	// keeps the key PERSISTENT: a redelivery must still observe the exhaustion.
	markExhaustedScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[4]) == 1 then return 0 end
local v = redis.call('HGET', KEYS[1], ARGV[1])
if not v then return 0 end
local dl, a, tok = parse_active(v, 'running')
if not dl then dl, a, tok = parse_active(v, 'next_attempt_at') end
if not dl then return 0 end
if a ~= ARGV[2] or tok ~= ARGV[3] then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], 'exhausted:'..a..':'..tok)
redis.call('PERSIST', KEYS[1])
return 1
`)

	// persistDLQScript ATOMICALLY appends one DLQ entry to the DLQ stream AND
	// records its persistence in the message's invocation-state hash, in one
	// script, so a crash or error between the append and the marker can never
	// happen (F-008): either both take effect (returns 1) or neither does.
	//
	// It serves both DLQ entry kinds:
	//
	//   - A real exhausted invocation (ARGV[2] non-empty): the marker must
	//     ALREADY be a well-formed exhausted marker whose retained identity
	//     matches the supplied claim (attempt + token). On a match the entry is
	//     appended and the marker is upgraded to "exhausted:<attempt>:<token>:dlq"
	//     (the existing convention). An already-":dlq" marker is monotonic and
	//     written nothing (returns 0). A missing, foreign, or non-exhausted
	//     marker is REFUSED (returns -1) without writing, so a stale/guessed
	//     claim can never append an unattributable entry or downgrade a newer
	//     marker.
	//   - The malformed-message placeholder (ARGV[2] empty): a message-scoped
	//     reserved field (ARGV[3]) records that this message's placeholder entry
	//     has been persisted. It is set in the SAME hash as the invocation
	//     markers (the hash is already scoped by stream/group/msgID), so it needs
	//     no fabricated app/handler identity. An already-marked placeholder is a
	//     no-op (returns 0).
	//
	// KEYS[1] = invocation-state hash; KEYS[2] = DLQ stream.
	// ARGV[1] = terminal marker field,
	// ARGV[2] = invocation field ("" for the placeholder),
	// ARGV[3] = placeholder marker field,
	// ARGV[4] = exhausted attempt (decimal string; "" for the placeholder),
	// ARGV[5] = exhausted claim token ("" for the placeholder),
	// ARGV[6..] = flat XADD field/value pairs (an even, non-zero count).
	// Returns 1 when this call appended the entry, 0 when it was already
	// recorded (no write), -1 when refused (no write).
	//
	// Every validation runs BEFORE the XADD. A Lua error after a successful
	// write cannot roll that write back, so a wrong-type or terminal-retained
	// hash and a missing/mismatched exhausted marker must all be rejected before
	// any write, leaving no partial effect. Within a single script no other
	// command interleaves, so once the hash type is validated the post-XADD HSET
	// cannot fail.
	//
	// The persistence marker is written while the message is still recoverable
	// (the XACK may not have run), so it keeps the key PERSISTENT: a redelivery
	// after a failed XACK must see it to stay idempotent.
	//
	// Redis Cluster: this script touches two keys (the invocation-state hash and
	// the DLQ stream). A clustered deployment would require both to hash to the
	// same slot (e.g. a shared hash tag); Relay uses a single-node *redis.Client,
	// so the keys are always co-located.
	persistDLQScript = redis.NewScript(luaInvocationHelpers + `
local kt = redis.call('TYPE', KEYS[1])
if kt['ok'] ~= 'none' and kt['ok'] ~= 'hash' then return -1 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return -1 end

local n = #ARGV
if n <= 5 or (n - 5) % 2 ~= 0 then return -1 end

local invocation = ARGV[2]
if invocation ~= '' then
  local v = redis.call('HGET', KEYS[1], invocation)
  if not v then return -1 end
  local a, tok, dlq = parse_exhausted(v)
  if not a then return -1 end
  if a ~= ARGV[4] or tok ~= ARGV[5] then return -1 end
  if dlq then return 0 end
  redis.call('XADD', KEYS[2], '*', unpack(ARGV, 6, n))
  redis.call('HSET', KEYS[1], invocation, 'exhausted:'..a..':'..tok..':dlq')
  redis.call('PERSIST', KEYS[1])
  return 1
end

if redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1 then return 0 end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 6, n))
redis.call('HSET', KEYS[1], ARGV[3], '1')
redis.call('PERSIST', KEYS[1])
return 1
`)

	// claimClassificationScript atomically claims the one-time event
	// classification with HSETNX and, only when it actually wrote the field,
	// clears any legacy TTL (the message is still recoverable, so its state must
	// be persistent).
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = classification field, ARGV[2] = terminal marker field.
	claimClassificationScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[2]) == 1 then
  -- Terminal-retained: classification has already been settled; report the
  -- claim as already taken so a stale caller never counts the event again.
  return 0
end
local set = redis.call('HSETNX', KEYS[1], ARGV[1], '1')
if set == 1 then
  redis.call('PERSIST', KEYS[1])
end
return set
`)

	// recordTraceScript atomically persists the compact trace lineage sibling
	// field and clears any legacy TTL in the same step. A terminal-retained hash
	// is left untouched: retention only runs after the message left the PEL, so a
	// stale in-memory delivery must not extend or re-open it.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = trace field, ARGV[2] = lineage, ARGV[3] = terminal marker field.
	recordTraceScript = redis.NewScript(`
if redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1 then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PERSIST', KEYS[1])
return 1
`)

	// makeRecoverableScript clears any TTL from an existing invocation-state
	// hash, making it persistent, but NEVER touches an already terminal-retained
	// hash (its message left the PEL and its retention TTL must be preserved).
	// A missing key is a no-op. It is the legacy/pre-persistence migration hook:
	// a hash written with the old fixed TTL becomes persistent as soon as its
	// message is processed again.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = terminal marker field.
	// Returns 1 when the key is now recoverable (persistent), 0 when it is
	// terminal-retained or absent.
	makeRecoverableScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
redis.call('PERSIST', KEYS[1])
return 1
`)

	// retainTerminalScript switches a message's invocation-state hash to TERMINAL
	// retention: it writes the reserved terminal marker and applies the retention
	// TTL, both in one atomic step. It is used only for the NON-XACK retention
	// case: a missing-payload PEL reference the server already purged (nothing to
	// ACK), plus store-level tests. Every production path that ACKs the message
	// uses ackAndRetainTerminalScript, which performs the ACK and the retention in
	// one script so no post-ACK orphan window exists.
	//
	// The marker makes every lifecycle script inert (see each script's HEXISTS
	// guard), so a stale transition from an in-memory delivery can neither mutate
	// the retained state nor PERSIST the key (which would remove the retention
	// TTL and resurrect an unrecoverable hash). It is monotonic: an
	// already-terminal hash is a no-op, so a repeated call (e.g. a redelivery
	// that races the retention) never re-extends the TTL. A missing key (the
	// retention already expired, or state was never written) returns 0.
	//
	// ARGV[2] is the terminal retention TTL in integer milliseconds, where a
	// non-positive value means terminal retention is DISABLED (configured as
	// empty/0/negative): the marker is still written atomically, but the key is
	// PERSISTed instead of expiring, so terminal bookkeeping survives with no
	// TTL and stale-transition guards are unchanged. This matters because a
	// freshly written hash may inherit no TTL while a legacy one could carry a
	// residual TTL — PERSIST guarantees the disabled contract explicitly.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = terminal marker field, ARGV[2] = retention TTL ms (>0) or
	// disabled (<=0).
	// Returns 1 when the hash is now terminal-retained, 0 when it does not exist.
	retainTerminalScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 1 end
redis.call('HSET', KEYS[1], ARGV[1], '1')
local ttl = tonumber(ARGV[2])
if ttl == nil or ttl <= 0 then
  redis.call('PERSIST', KEYS[1])
else
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return 1
`)

	// ackAndRetainTerminalScript atomically acknowledges a message AND switches
	// its invocation-state hash to terminal retention in ONE script, so the
	// post-XACK orphan window cannot exist: a crash or error can no
	// longer leave a hash whose message already left the PEL without a terminal
	// marker/TTL.
	//
	// Ordering is deliberate and PEL-safe:
	//
	//   1. Validate everything that could fail AFTER the irreversible XACK:
	//      parse the retention TTL and inspect the state key's type. A non-hash
	//      state key is refused (an error reply) BEFORE any write or ACK, so a
	//      wrong-type key leaves no partial effect and the caller leaves the
	//      message pending.
	//   2. When the hash exists and is not already terminal, stage the terminal
	//      marker and PERSIST it (NO TTL) BEFORE the ACK. Staging before the ACK
	//      is what makes a failed ACK safe: the marker is PERSISTENT, so a
	//      still-pending message's state can never acquire (or keep) an expiring
	//      TTL and expire out from under a redelivery; a redelivery merely
	//      observes the terminal marker (its lifecycle transitions are inert) and
	//      re-acks.
	//   3. XACK. A returned count of 0 (the entry was already absent) is a
	//      success, exactly like the former bare XAck whose .Err() ignored it.
	//      XACK runs after the marker is staged, so even if XACK raises (e.g. a
	//      wrong-type stream) the hash is left terminal-marked and persistent,
	//      never an unmarked orphan and never expiring.
	//   4. Apply the retention TTL ONLY now, after the ACK, and only when
	//      retention is enabled (ttl > 0) and the hash HAS NO TTL already
	//      (PTTL < 0). Never re-extend or shorten an existing retention TTL, so
	//      repeated calls are monotonic; but a marker a previous failed ACK
	//      staged without a TTL DOES get its window once the ACK finally
	//      succeeds, so an interrupted terminal-ack cannot leave a permanent
	//      leak either. The TTL can no longer land on a still-pending message,
	//      and PTTL/PEXPIRE on the validated hash cannot error, so no command
	//      after the ACK can fail and recreate an unmarked orphan.
	//
	// KEYS[1] = invocation-state hash; KEYS[2] = stream.
	// ARGV[1] = group, ARGV[2] = message ID, ARGV[3] = terminal marker field,
	// ARGV[4] = retention TTL ms (>0) or <=0 for disabled.
	// Returns {removed, retained}: removed 1 when the message left the PEL (0
	// when it was already absent), retained 1 when the hash existed and now
	// carries the terminal marker.
	ackAndRetainTerminalScript = redis.NewScript(`
local ttl = tonumber(ARGV[4])
if ttl == nil then ttl = 0 end

local kt = redis.call('TYPE', KEYS[1])
local kind = kt['ok']
if kind ~= 'none' and kind ~= 'hash' then
  return redis.error_reply('ackAndRetainTerminal: invocation-state key is not a hash')
end

local hasHash = kind == 'hash'
local alreadyTerminal = false
if hasHash then
  alreadyTerminal = redis.call('HEXISTS', KEYS[1], ARGV[3]) == 1
end

if hasHash and not alreadyTerminal then
  redis.call('HSET', KEYS[1], ARGV[3], '1')
  redis.call('PERSIST', KEYS[1])
end

local removed = redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])

if hasHash and ttl > 0 and redis.call('PTTL', KEYS[1]) < 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end

return {removed, hasHash and 1 or 0}
`)
)

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
//	exhausted       → "exhausted:<n>:<token>"               (MarkExhausted, CASed on the claim)
//	DLQ persisted   → "exhausted:<n>:<token>:dlq"           (stream layer's atomic persistDLQ append, CASed on the exhausted claim)
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
