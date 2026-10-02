package stream

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// invocationRetentionTTL is how long a message's invocation-state hash survives
// AFTER the message has left the PEL — a successful XACK on the success,
// obsolete, or DLQ path, or a cleared missing-payload PEL reference. Recoverable
// state (any hash whose message is still pending and can therefore be
// redelivered) is deliberately PERSISTENT, with NO TTL: it must never expire out
// from under a redelivery, which would reset the attempt/reclaim accounting. The
// retention TTL is applied only by retainTerminal, atomically with the reserved
// terminal marker, once the message is no longer recoverable.
//
// It is a retention window for terminal bookkeeping (so a completed/exhausted
// marker survives long enough to be inspected) and a cleanup safety net: if the
// post-ACK retention never runs (a crash in the ACK→retain gap), the hash is
// simply leaked rather than lost — a persistent leak is strictly safer than
// premature state loss.
const invocationRetentionTTL = 7 * 24 * time.Hour

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
)

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
}

// maxScheduleAdoptionAttempts bounds the descriptor adoption loop: at most one
// conflict can be observed because the pinned descriptor is immutable while the
// message is recoverable, so two rounds always suffice. The bound is defensive.
const maxScheduleAdoptionAttempts = 4

// ScheduleAdmission is the resolved result of the schedule admission boundary as
// seen by the runner: the descriptor that owns the message, whether this caller
// claimed the invocation, and whether the occurrence is obsolete (never admitted
// and no longer configured). A zero ScheduleAdmission with Obsolete=false and
// Started=false means the invocation is protected or terminal (Claim/Wait carry
// the marker details, exactly like TryStart).
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
	// and is always set unless Obsolete is true.
	Descriptor ScheduleDescriptor
	// Obsolete is true when no descriptor was ever pinned for this message and
	// the caller had no current schedule to propose (its schedule NAME is gone):
	// the occurrence is obsolete and must be ACKed, never retried or DLQ'd.
	Obsolete bool
}

// ttlMillis converts a TTL to integer milliseconds for the PEXPIRE argument.
func ttlMillis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

// terminalField is the reserved invocation-state hash field that marks a hash as
// no longer recoverable: the message it belongs to has left the PEL (a
// successful XACK on the success/obsolete/DLQ path, or a cleared/purged
// missing-payload PEL reference), so retainTerminal switched the hash to
// terminal retention (invocationRetentionTTL). While the field is present every
// lifecycle script is inert: a stale transition from an in-memory delivery can
// neither mutate the retained state nor remove the retention TTL (which would
// resurrect an unrecoverable hash). Like classificationField and traceFieldPrefix
// it cannot collide with a real invocation ID (a leading "__" is not a legal
// app name).
const terminalField = "__terminal"

// retentionTTLMillis returns the terminal retention TTL in integer milliseconds
// for the PEXPIRE argument passed to the retain script.
func retentionTTLMillis() int64 {
	return ttlMillis(invocationRetentionTTL)
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
	//           verified; "" when it has none).
	//
	// Returns a 4-element reply {outcome, attempt, deadline, pinned}:
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
	//	                (obsolete)
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
  -- never admitted and its schedule is gone — obsolete.
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
	// successful XACK via retainTerminal.
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

	// markExhaustedDLQScript upgrades an EXISTING exhausted marker to the
	// ":dlq" form, but only when its retained identity matches the exhausted
	// claim (attempt + token). An existing ":dlq" marker is monotonic (returns
	// 1); "ok"/active/absent markers or a different (newer) exhausted identity
	// are preserved (returns 0), so a stale XADD outcome can never downgrade a
	// newer exhausted marker or a success.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = invocation field, ARGV[2] = exhausted attempt, ARGV[3] =
	// exhausted token, ARGV[4] = terminal marker field.
	// Returns 1 when the exhausted marker already/now records the DLQ, 0 else.
	//
	// The DLQ-persistence marker is written after the XADD but before the XACK,
	// so it keeps the key PERSISTENT: a redelivery after a failed XACK must see
	// it to stay idempotent.
	markExhaustedDLQScript = redis.NewScript(luaInvocationHelpers + `
if redis.call('HEXISTS', KEYS[1], ARGV[4]) == 1 then return 0 end
local v = redis.call('HGET', KEYS[1], ARGV[1])
if not v then return 0 end
local a, tok, dlq = parse_exhausted(v)
if not a then return 0 end
if a ~= ARGV[2] or tok ~= ARGV[3] then return 0 end
if dlq then return 1 end
redis.call('HSET', KEYS[1], ARGV[1], 'exhausted:'..a..':'..tok..':dlq')
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
	// TTL, both in one atomic step. It is called only AFTER the message is no
	// longer recoverable (a successful XACK on the success/obsolete/DLQ path, or
	// a cleared/purged missing-payload PEL reference).
	//
	// The marker makes every lifecycle script inert (see each script's HEXISTS
	// guard), so a stale transition from an in-memory delivery can neither mutate
	// the retained state nor PERSIST the key (which would remove the retention
	// TTL and resurrect an unrecoverable hash). It is monotonic: an
	// already-terminal hash is a no-op, so a repeated call (e.g. a redelivery
	// that races the retention) never re-extends the TTL. A missing key (the
	// retention already expired, or state was never written) returns 0.
	//
	// KEYS[1] = invocation-state hash;
	// ARGV[1] = terminal marker field, ARGV[2] = retention TTL ms.
	// Returns 1 when the hash is now terminal-retained, 0 when it does not exist.
	retainTerminalScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 1 end
redis.call('HSET', KEYS[1], ARGV[1], '1')
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
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

// invocationStateStore is the per-message invocation-state persistence seam:
// the concrete *invocationStore implements it against Redis; tests may
// substitute a fake. Every active-claim-originated transition takes the
// InvocationClaim returned by tryStart and CASes attempt+token against the
// current ACTIVE marker; a mismatch is returned as an explicit stale result
// (false), never a Redis error.
type invocationStateStore interface {
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
	// descriptor to adopt (conflict) or reports no admission (obsolete). It
	// returns the raw scheduleStartResult for ONE script evaluation; the
	// InvocationState handle wraps it in the bounded adoption loop. A
	// store/transport error is returned (NOT failed open) so no handler runs on an
	// ambiguous claim.
	tryStartScheduled(
		ctx context.Context,
		stream, group, msgID, invocation string,
		now, deadline time.Time,
		desc ScheduleDescriptor,
		known bool,
	) (scheduleStartResult, error)
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
	// markExhaustedDLQ upgrades an EXISTING exhausted marker matching the given
	// exhausted claim (attempt+token) to the terminal "...:dlq" form, recording
	// that this invocation's DLQ entry has been persisted. It is written only
	// after a successful XADD so a redelivery (e.g. after an XACK failure) can
	// skip the write idempotently. It never downgrades a newer exhausted marker
	// or a success.
	markExhaustedDLQ(ctx context.Context, stream, group, msgID, invocation string, claim InvocationClaim) (bool, error)
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
	retainTerminal(ctx context.Context, stream, group, msgID string) error
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
// message has left the PEL (a successful XACK on the success/obsolete/DLQ path,
// or a cleared/purged missing-payload PEL reference) does retainTerminal switch
// it to invocationRetentionTTL. A hash that never gets retained is leaked, never
// prematurely expired: safe by construction.
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
	).Slice()
	if err != nil {
		return scheduleStartResult{}, err
	}
	if len(res) < 4 {
		// The script always returns four elements; a short reply is a protocol
		// violation, so treat the claim as unconfirmed rather than guess.
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
	default:
		return scheduleStartResult{}, fmt.Errorf("tryStartScheduled: unexpected outcome code %d", outcomeCode)
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

// markExhaustedDLQ upgrades the invocation's exhausted marker to
// "exhausted:<attempt>:<token>:dlq" via the atomic markExhaustedDLQ script,
// recording that its DLQ entry has been persisted. It CASes the retained
// exhausted identity (attempt + token) so a stale XADD outcome can never
// downgrade a newer exhausted marker or a success: only the exact exhausted
// claim that owns the marker is upgraded. It is called only AFTER a successful
// XADD, so a later redelivery can skip the (already-written) entry without
// scanning the DLQ stream. An existing ":dlq" marker returns true (monotonic);
// "ok"/active/absent/other-identity markers return false.
func (store *invocationStore) markExhaustedDLQ(
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
	ok, err := markExhaustedDLQScript.Run(ctx, store.client, []string{key},
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

// exhaustedState reads the invocation's exhausted marker, returning its retained
// claim identity (attempt + token), whether its DLQ entry is already persisted
// (the ":dlq" suffix), and whether a well-formed exhausted marker was present.
// redis.Nil (field absent) and any non-exhausted marker return ok=false.
//
// It serves routeToDLQ twice: the dlq flag lets it skip an invocation whose
// entry was already written, and the retained claim identity is the exact CAS
// key for the subsequent markExhaustedDLQ upgrade, so a stale/foreign marker can
// never be upgraded. A read error returns (zero, false, false, err); the caller
// fails safe by treating the entry as not persisted (a duplicate is allowed
// under at-least-once, while skipping a required write would lose the entry).
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

// retainTerminal switches the message's invocation-state hash to terminal
// retention via the atomic retainTerminal script: it writes the reserved
// terminal marker and applies the retention TTL in one step. It is called only
// AFTER the message is no longer recoverable — a successful XACK on the
// success/obsolete/DLQ path, or a cleared/purged missing-payload PEL reference —
// because retention removes recoverability. The marker makes every lifecycle
// script inert, so a stale in-memory delivery cannot mutate the retained state
// or remove the TTL. The upgrade is monotonic: a repeated call never re-extends
// an already-terminal hash. A missing key is a no-op (nothing to retain).
func (store *invocationStore) retainTerminal(ctx context.Context, stream, group, msgID string) error {
	_, err := retainTerminalScript.Run(ctx, store.client,
		[]string{invocationStateKey(stream, group, msgID)},
		terminalField,
		retentionTTLMillis(),
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
//	DLQ persisted   → "exhausted:<n>:<token>:dlq"           (MarkExhaustedDLQ, CASed on the exhausted claim)
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
// (invocationRetentionTTL) via the reserved terminal marker.
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
	// proposed is the descriptor resolved from the CURRENT template by schedule
	// NAME (handler, capped timeout, retries); propose=false means the schedule
	// NAME is gone, so nothing can be proposed. invocationFor maps a descriptor to
	// its "<app>/<handler>" invocation ID: on a lost race the winning
	// descriptor owns the message and its handler (not the caller's) is invoked,
	// so the callback is re-evaluated for the adopted descriptor. The loop is
	// bounded and terminates because a pinned descriptor is immutable while the
	// message is recoverable.
	//
	// It returns a ScheduleAdmission. Obsolete is true only when nothing was ever
	// pinned and the caller proposed nothing (the occurrence was never admitted
	// and its schedule is gone → the stream ACKs, never retries or DLQs). An error
	// is a Redis/transport failure (or token-generation failure): the claim
	// outcome is unknown, so the caller leaves the message pending and does not
	// execute.
	//
	// desc/known describe what the caller knows: known=false means it could
	// neither read a pinned descriptor nor resolve the current schedule.
	TryStartScheduled(desc ScheduleDescriptor, known bool, invocationFor func(ScheduleDescriptor) string) (ScheduleAdmission, error)
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
// from the Redis delivery count. That distinction matters because a message can
// be reclaimed (delivered) many times while a handler attempt advances only on
// real executions, so deliveries >= handler_attempts.
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
// not have. See InvocationState.TryStartScheduled for the contract.
func (state *invocationState) TryStartScheduled(
	desc ScheduleDescriptor,
	known bool,
	invocationFor func(ScheduleDescriptor) string,
) (ScheduleAdmission, error) {
	for attempt := 0; attempt < maxScheduleAdoptionAttempts; attempt++ {
		invocation := ""
		now := state.now()
		deadline := now
		if known {
			invocation = invocationFor(desc)
			deadline = now.Add(desc.Timeout)
		}
		res, err := state.store.tryStartScheduled(
			state.ctx, state.stream, state.group, state.msgID, invocation,
			now, deadline, desc, known,
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
		case scheduleStartNoAdmission:
			// Nothing was ever pinned and the caller has no descriptor: the
			// occurrence was never admitted and its schedule is gone (obsolete).
			return ScheduleAdmission{Obsolete: true}, nil
		case scheduleStartConflict:
			// The message carries a descriptor the caller did not supply: adopt it
			// and retry. A pinned descriptor is immutable while the message is
			// recoverable, so the retry observes it as the winner and cannot loop.
			if !res.hasPinned {
				return ScheduleAdmission{}, fmt.Errorf(
					"invocation state: schedule conflict without a pinned descriptor for %q", state.msgID)
			}
			desc = res.pinned
			known = true
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
