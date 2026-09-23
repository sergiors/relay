package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// invocationStateTTL is how long a message's invocation-state hash survives
// after its last write. It is comfortably longer than the maximum pending
// lifetime (a rule's 1+Retries attempts × reclaim cadence ~2m + idle headroom),
// so a message that is abandoned or deleted from the stream is eventually
// cleaned up by Redis even if the eager clear on completion never runs. The
// eager clear (see processMessage/routeToDLQ) is the primary cleanup; the TTL
// is the safety net.
const invocationStateTTL = 7 * 24 * time.Hour

// clockSkewTolerance widens the expiry evaluation of a running marker by this
// much: deadline comparisons across replicas depend on synchronized clocks, and
// treating a marker as expired slightly early costs at most one extra attempt of
// overlap (allowed under at-least-once) while never lengthening the protected
// window by unbounded skew. It must stay far smaller than the minimum sensible
// handler timeout.
const clockSkewTolerance = time.Second

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
// substitute a fake.
type invocationStateStore interface {
	completed(ctx context.Context, stream, group, msgID, invocation string) (bool, error)
	markComplete(ctx context.Context, stream, group, msgID, invocation string) error
	tryStart(
		ctx context.Context,
		stream, group, msgID, invocation string,
		now, deadline time.Time,
	) (started bool, attempt int, wait time.Duration, err error)
	finishFailure(
		ctx context.Context,
		stream, group, msgID, invocation string,
		backoff time.Duration,
		now time.Time,
	) (time.Time, error)
	markExhausted(ctx context.Context, stream, group, msgID, invocation string, attempts int) error
	// markExhaustedDLQ upgrades an exhausted invocation's marker to the
	// terminal "exhausted:<attempts>:dlq" form, recording that this
	// invocation's DLQ entry has been persisted. It is written only after a
	// successful XADD so a redelivery (e.g. after an XACK failure) can skip the
	// write idempotently instead of duplicating the entry.
	markExhaustedDLQ(ctx context.Context, stream, group, msgID, invocation string, attempts int) error
	// exhaustedPersisted reports whether the invocation's marker already records
	// a persisted DLQ entry ("exhausted:<attempts>:dlq"). It lets routeToDLQ
	// skip an invocation whose entry was already written on a previous delivery,
	// so a partial multi-entry write is completed without duplicating the
	// entries that succeeded.
	exhaustedPersisted(ctx context.Context, stream, group, msgID, invocation string) (bool, error)
	terminal(ctx context.Context, stream, group, msgID, invocation string) (bool, error)
	// claimClassification atomically claims the one-time event classification
	// for this message (HSETNX on a reserved field). It returns true only for
	// the first caller across redeliveries and replicas.
	claimClassification(ctx context.Context, stream, group, msgID string) (bool, error)
	clear(ctx context.Context, stream, group, msgID string) error
}

// classificationField is the reserved invocation-state hash field that records
// whether this message's logical-event classification (received/matched/
// unmatched) has already been claimed. It cannot collide with a real invocation
// ID, which is always "<function>/<handler>": function names are validated to
// start with [a-z0-9], so a leading "__" is not a legal function name.
const classificationField = "__classification"

// invocationStore is a thin Redis-backed store for per-message invocation
// state. Each key is a HASH mapping an invocation ID ("<function>/<handler>")
// to a short value describing that invocation's lifecycle for this message.
//
// The value grammar (a single string, so the same field convention stays
// greppable and forward-compatible):
//
//	"ok"                          → completed on a previous delivery
//	"running:<dl>#<attempts>"     → an attempt is (or was) executing, protected
//	                                until the absolute Unix-nano deadline <dl>;
//	                                <attempts> is the 1-based attempt number
//	"next_attempt_at:<dl>#<attempts>" → a failed attempt is waiting out its retry
//	                                backoff, protected until <dl>
//	"exhausted:<attempts>"         → attempts exhausted; terminal, never eligible,
//	                                DLQ entry NOT yet persisted
//	"exhausted:<attempts>:dlq"     → attempts exhausted AND this invocation's DLQ
//	                                entry has been persisted; terminal, never
//	                                eligible, and never re-written to the DLQ
//	(absent)                      → eligible to execute
//
// "ok" stays bare because attempts are no longer needed after completion. Any
// value that does not parse under this grammar is treated as eligible.
//
// The ":dlq" suffix records per-invocation DLQ persistence without scanning the
// DLQ stream: routeToDLQ consults it (exhaustedPersisted) to skip an invocation
// whose entry was already written, which makes retrying a partially-written
// multi-entry DLQ (after an XACK failure or crash) idempotent.
//
// It is the stream layer's domain (Redis), but the runner decides which
// invocations match, so the store is exposed to the runner through the
// InvocationState interface carried in the delivery context.
type invocationStore struct {
	client *redis.Client
}

// completed reports whether the invocation has already completed for this
// message; redis.Nil (field absent) means not completed.
func (p *invocationStore) completed(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (bool, error) {
	v, err := p.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v == "ok", nil
}

// terminal reports whether the invocation is terminal (complete or exhausted);
// redis.Nil (field absent) means not terminal.
func (p *invocationStore) terminal(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (bool, error) {
	v, err := p.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	kind, _, _, ok := parseInvocationState(v)
	if !ok {
		return false, nil
	}
	return kind == kindComplete || kind == kindExhausted, nil
}

// markComplete records that the invocation completed for this message. HSET and
// EXPIRE are pipelined so the TTL is refreshed on every write without an extra
// round trip. Writing "ok" overwrites any "running:<...>" or
// "next_attempt_at:<...>" marker the same invocation carried, so a successful
// attempt atomically transitions the field from protected to complete.
func (p *invocationStore) markComplete(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) error {
	key := invocationStateKey(stream, group, msgID)
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, "ok")
	pipe.Expire(ctx, key, invocationStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// tryStart attempts to claim the invocation for a new execution. It reads the
// current field value and decides eligibility:
//
//	"ok"            → already complete; not started (attempt 0, no wait)
//	"exhausted:<n>" → attempts exhausted; not started (attempt n, no wait)
//	"running:<dl>#<n>" with now < dl → a protected attempt is in flight (this or
//	                  another replica); not started, wait = dl - now
//	"next_attempt_at:<dl>#<n>" with now < dl → a failed attempt is waiting out its
//	                  backoff; not started, wait = dl - now
//	absent, expired, or unparseable → eligible: HSET "running:<deadline>#<n+1>"
//	                  and report started with attempt n+1
//
// The decision is read-then-write (atomic-ish): two replicas can both read
// "absent" and both start, which is safe under at-least-once (duplicates are
// allowed; handlers must be idempotent). The deadline is the absolute time at
// which the attempt is considered abandoned, so a crashed worker's marker
// self-expires and recovery waits it out rather than racing the live attempt.
//
// An attempt is protected until the deadline minus clockSkewTolerance: the
// tolerance absorbs cross-replica clock skew in the safe direction. A slightly
// fast replica writes an inflated deadline; a slower replica evaluating later
// would otherwise see it still-active longer than intended. Treating a marker
// as expired slightly early costs at most one extra attempt of overlap within
// the tolerance (allowed under at-least-once, and handlers are idempotent),
// while never lengthening the protected window by unbounded skew. The
// tolerance must stay far smaller than the minimum sensible handler timeout.
func (p *invocationStore) tryStart(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	now,
	deadline time.Time,
) (started bool, attempt int, wait time.Duration, err error) {
	key := invocationStateKey(stream, group, msgID)
	v, err := p.client.HGet(ctx, key, invocation).Result()
	if err == redis.Nil {
		// Field absent: eligible.
	} else if err != nil {
		return false, 0, 0, err
	} else {
		kind, dl, n, ok := parseInvocationState(v)
		if !ok {
			// Unparseable: eligible.
		} else {
			switch kind {
			case kindComplete:
				return false, 0, 0, nil
			case kindExhausted:
				return false, n, 0, nil
			case kindRunning, kindNextAttempt:
				if now.Add(-clockSkewTolerance).Before(dl) {
					// Protected until dl (minus the clock-skew tolerance, so a
					// slightly-fast replica's inflated deadline does not block a
					// slower replica beyond the intended window).
					return false, n, dl.Sub(now), nil
				}
				// Expired: eligible, carrying the attempt count forward.
				attempt = n
			default:
				// kindEligible: eligible.
			}
		}
	}
	// Absent, expired, or unparseable: start a new attempt. HSET + EXPIRE are
	// pipelined so the TTL is refreshed on the write without an extra round trip.
	attempt++
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, runningValue(deadline, attempt))
	pipe.Expire(ctx, key, invocationStateTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, 0, 0, err
	}
	return true, attempt, 0, nil
}

// finishFailure records a failed attempt by persisting a "next_attempt_at"
// marker so the invocation is gated by its retry backoff until the returned
// deadline. It reads the current field to preserve the attempt count (the
// attempt that just failed), then HSETs the next-attempt marker and refreshes
// the TTL in a pipeline. If the field went missing (a race), attempts defaults
// to 1. On a read error it returns the error so the caller can fail open (leave
// the field as-is, making the invocation eligible immediately — at-least-once).
func (p *invocationStore) finishFailure(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	backoff time.Duration,
	now time.Time,
) (nextDeadline time.Time, err error) {
	key := invocationStateKey(stream, group, msgID)
	v, err := p.client.HGet(ctx, key, invocation).Result()
	if err == redis.Nil {
		// Field missing (race): treat as attempt 1.
		v = ""
	} else if err != nil {
		return time.Time{}, err
	}
	attempts := 1
	if kind, _, n, ok := parseInvocationState(v); ok && kind != kindComplete {
		attempts = n
	}
	nextDeadline = now.Add(backoff)
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, nextAttemptValue(nextDeadline, attempts))
	pipe.Expire(ctx, key, invocationStateTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return time.Time{}, err
	}
	return nextDeadline, nil
}

// markExhausted records that the invocation's attempts are exhausted, writing
// the terminal "exhausted:<attempts>" marker so a redelivery skips it without
// re-running. HSET + EXPIRE are pipelined. It deliberately does NOT set the
// ":dlq" suffix: the DLQ entry has not been persisted yet. If this write
// overwrites a marker that already carried ":dlq" (e.g. a concurrent
// redelivery raced a completed DLQ write), the suffix is lost and the entry is
// re-written on redelivery; that is the at-least-once duplicate window, not a
// correctness loss.
func (p *invocationStore) markExhausted(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	attempts int,
) error {
	return p.writeExhausted(ctx, stream, group, msgID, invocation, attempts, false)
}

// markExhaustedDLQ upgrades the invocation's exhausted marker to
// "exhausted:<attempts>:dlq", recording that its DLQ entry has been persisted.
// HSET + EXPIRE are pipelined. It is called only AFTER a successful XADD, so a
// later redelivery can skip the (already-written) entry without scanning the
// DLQ stream.
func (p *invocationStore) markExhaustedDLQ(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	attempts int,
) error {
	return p.writeExhausted(ctx, stream, group, msgID, invocation, attempts, true)
}

// writeExhausted writes the terminal exhausted marker, optionally with the
// ":dlq" persistence suffix, refreshing the TTL in the same pipeline.
func (p *invocationStore) writeExhausted(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	attempts int,
	dlqPersisted bool,
) error {
	key := invocationStateKey(stream, group, msgID)
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, exhaustedValue(attempts, dlqPersisted))
	pipe.Expire(ctx, key, invocationStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// exhaustedPersisted reports whether the invocation's marker already records a
// persisted DLQ entry ("exhausted:<attempts>:dlq"). redis.Nil (field absent)
// and any other exhausted marker (without the suffix) return false, so the
// entry is (re-)written. A read error returns (false, err); the caller fails
// safe by treating the entry as not persisted (a duplicate is allowed under
// at-least-once, while skipping a required write would lose the entry).
func (p *invocationStore) exhaustedPersisted(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) (bool, error) {
	v, err := p.client.HGet(ctx, invocationStateKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return isExhaustedDLQValue(v), nil
}

// clear deletes the message's invocation-state hash entirely. It is called
// eagerly on completion (successful ACK or DLQ routing) so the key does not
// linger.
func (p *invocationStore) clear(ctx context.Context, stream, group, msgID string) error {
	return p.client.Del(ctx, invocationStateKey(stream, group, msgID)).Err()
}

// claimClassification atomically claims this message's one-time logical-event
// classification. It uses HSETNX on the reserved classificationField, which is
// atomic in Redis: exactly one caller (across redeliveries, reclaims, and
// replicas) receives true and therefore counts the event once; every later
// delivery of the same message sees the field present and receives false. The
// claim is written before the runner classifies, so a crash between the claim
// and the metric increment can only LOSE a count for that event — it can never
// double-count one. The TTL is refreshed alongside the write so the claim is
// cleaned up with the rest of the message's invocation state.
//
// The claim lives and dies with the message's invocation-state hash: terminal
// paths (successful ACK or DLQ routing) clear that hash only after the message
// leaves the PEL, so no redelivery can follow a clear. The claim also cannot
// outlive the invocationStateTTL; a message left pending longer than the TTL
// could in principle be re-classified, but that TTL is comfortably longer than
// the maximum pending lifetime (see invocationStateTTL).
//
// On a Redis error it returns (false, err): the caller must NOT count the
// event, because it cannot prove the claim. Failing open here would risk
// double-counting on redelivery, and classification counters are exact
// partition counts, not at-least-once accounting.
func (p *invocationStore) claimClassification(ctx context.Context, stream, group, msgID string) (bool, error) {
	key := invocationStateKey(stream, group, msgID)
	pipe := p.client.Pipeline()
	set := pipe.HSetNX(ctx, key, classificationField, "1")
	pipe.Expire(ctx, key, invocationStateTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return set.Val(), nil
}

// runningValue encodes a protected attempt's absolute deadline and attempt
// number as the field value "running:<unixnano>#<attempts>". The "running:"
// prefix distinguishes it from the "ok" completion sentinel; the Unix-nano
// suffix is the deadline at which the attempt is considered abandoned.
func runningValue(deadline time.Time, attempts int) string {
	return "running:" + strconv.FormatInt(deadline.UnixNano(), 10) + "#" + strconv.Itoa(attempts)
}

// nextAttemptValue encodes a failed attempt's retry deadline and attempt number
// as "next_attempt_at:<unixnano>#<attempts>".
func nextAttemptValue(deadline time.Time, attempts int) string {
	return "next_attempt_at:" + strconv.FormatInt(deadline.UnixNano(), 10) + "#" + strconv.Itoa(attempts)
}

// exhaustedValue encodes the terminal exhausted state. With dlqPersisted false
// it is "exhausted:<attempts>"; with true it appends the ":dlq" suffix
// ("exhausted:<attempts>:dlq") to record that the invocation's DLQ entry has
// been persisted.
func exhaustedValue(attempts int, dlqPersisted bool) string {
	v := "exhausted:" + strconv.Itoa(attempts)
	if dlqPersisted {
		v += ":dlq"
	}
	return v
}

// isExhaustedDLQValue reports whether v is exactly the valid
// "exhausted:<attempts>:dlq" marker (attempts >= 1), i.e. the invocation's DLQ
// entry has been persisted. It is strict so a corrupt or near-miss marker can
// never be mistaken for a persisted entry and cause a required DLQ write to be
// skipped.
func isExhaustedDLQValue(v string) bool {
	if !strings.HasPrefix(v, "exhausted:") || !strings.HasSuffix(v, ":dlq") {
		return false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(v[len("exhausted:"):], ":dlq"))
	return err == nil && n >= 1
}

// parseInvocationState decodes a field value into its kind, deadline (for
// running/next-attempt markers), and attempt count. It returns ok=false for any
// value that is not a well-formed marker (e.g. a corrupt marker), which the
// caller treats as eligible.
func parseInvocationState(v string) (kind invocationKind, deadline time.Time, attempts int, ok bool) {
	switch {
	case v == "ok":
		return kindComplete, time.Time{}, 0, true
	case strings.HasPrefix(v, "running:"):
		dl, n, ok := parseDeadlineAttempts(v[len("running:"):])
		if !ok {
			return kindEligible, time.Time{}, 0, false
		}
		return kindRunning, dl, n, true
	case strings.HasPrefix(v, "next_attempt_at:"):
		dl, n, ok := parseDeadlineAttempts(v[len("next_attempt_at:"):])
		if !ok {
			return kindEligible, time.Time{}, 0, false
		}
		return kindNextAttempt, dl, n, true
	case strings.HasPrefix(v, "exhausted:"):
		// The attempts part is mandatory; an optional ":dlq" suffix records
		// that the invocation's DLQ entry has been persisted. Both forms parse
		// to kindExhausted with the same attempt count.
		attemptsStr := strings.TrimSuffix(v[len("exhausted:"):], ":dlq")
		n, err := strconv.Atoi(attemptsStr)
		if err != nil || n < 1 {
			return kindEligible, time.Time{}, 0, false
		}
		return kindExhausted, time.Time{}, n, true
	default:
		return kindEligible, time.Time{}, 0, false
	}
}

// parseDeadlineAttempts parses "<unixnano>#<attempts>". The attempt count is
// mandatory: a marker without it does not parse (treated as eligible).
func parseDeadlineAttempts(s string) (time.Time, int, bool) {
	dlStr, attemptsStr, hasHash := strings.Cut(s, "#")
	n, err := strconv.ParseInt(dlStr, 10, 64)
	if err != nil || !hasHash {
		return time.Time{}, 0, false
	}
	a, err := strconv.Atoi(attemptsStr)
	if err != nil || a < 1 {
		return time.Time{}, 0, false
	}
	return time.Unix(0, n), a, true
}

// InvocationState is the read/write view of a single message's invocation
// state, carried in the delivery context so the runner can decide whether to
// execute an invocation. The invocation argument is the full
// "<function>/<handler>" ID; the stream layer never parses it.
//
// The lifecycle of a single invocation's field value:
//
//	attempt starts  → "running:<deadline>#<n>"        (TryStart)
//	completes       → "ok"                            (MarkComplete, overwrites)
//	fails (retry)   → "next_attempt_at:<deadline>#<n>" (RecordFailure)
//	exhausted       → "exhausted:<n>"                 (MarkExhausted)
//
// A crash mid-attempt leaves "running:<deadline>#<n>", which self-expires at
// its deadline; recovery waits it out (bounded staleness of at most one
// timeout). A failed attempt's "next_attempt_at" marker similarly self-expires
// if the worker crashes before the message is reclaimed.
type InvocationState interface {
	IsComplete(invocation string) bool
	MarkComplete(invocation string)
	// TryStart claims the invocation for a new execution. It returns started
	// true (with the 1-based attempt number) when the caller should execute,
	// false otherwise. When not started, wait is the duration until the
	// invocation becomes eligible again: wait > 0 means it is protected by an
	// active running deadline or a retry backoff (this or another replica), and
	// wait == 0 means it is terminal (complete or exhausted) and will never be
	// eligible again.
	TryStart(invocation string, timeout time.Duration) (started bool, attempt int, wait time.Duration)
	// RecordFailure persists a failed attempt's retry backoff so the invocation
	// is gated until now+backoff. The attempt count is read from the store.
	RecordFailure(invocation string, backoff time.Duration)
	// MarkExhausted records that the invocation's attempts are exhausted,
	// making it terminal (skipped like complete on redelivery).
	MarkExhausted(invocation string, attempts int)
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

// ErrInvocationExhausted is returned (wrapped) by the runner's Handle when a
// failing invocation's attempts are exhausted AND every other matched invocation
// is complete or also exhausted, so the message is terminal and must be routed
// to the DLQ. The stream layer routes the whole message to the DLQ; every
// exhausted invocation gets its own DLQ entry (see HandlerExhaustedError).
var ErrInvocationExhausted = errors.New("invocation exhausted")

// ExhaustedInvocation identifies one terminal exhausted invocation: the exact
// function and handler, the handler attempt that exhausted (the 1+retries bound
// reached, sourced from the invocation retry state, never the Redis delivery
// count), and the underlying failure cause when it is known.
//
// It is the unit of per-invocation DLQ attribution: the stream writes one DLQ
// entry per ExhaustedInvocation, so a message matching several functions or
// handlers that all exhaust produces one entry each, with exact metadata.
type ExhaustedInvocation struct {
	// Function is the exact function name.
	Function string
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

// Invocation returns the "<function>/<handler>" invocation ID used by the
// per-message invocation-state hash.
func (e ExhaustedInvocation) Invocation() string {
	return e.Function + "/" + e.Handler
}

// Reason returns the human-readable exhaustion reason for this invocation,
// naming the exact function/handler and attempt count so the DLQ `reason`
// field stays consistent with the entry's `handler_attempts` and metadata.
func (e ExhaustedInvocation) Reason() string {
	reason := fmt.Sprintf("function %q handler %q exhausted after %d handler attempts",
		e.Function, e.Handler, e.Attempts)
	if e.Err != nil {
		reason += ": " + e.Err.Error()
	}
	return reason
}

// HandlerExhaustedError is the runner's terminal exhaustion signal. It wraps
// ErrInvocationExhausted (so errors.Is keeps matching) and carries the full set
// of exhausted invocations for the message, each with its exact function,
// handler, and exhausted handler attempt. Handle aggregates EVERY exhausted
// matched invocation (both those that exhausted on this delivery and those
// already marked exhausted on a previous delivery), so a multi-function or
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
// `invocation exhausted: function "fn" handler "h" exhausted after 5 handler
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
// no longer exists in the current function/template configuration — the
// function or its schedule entry/handler was removed while the message was
// pending. Obsolete invocations are terminal and MUST NOT be retried or
// routed to the DLQ: their removal was an intentional configuration change,
// so the stream layer acknowledges the message instead.
var ErrInvocationObsolete = errors.New("invocation obsolete")

// WithInvocationState returns a child of ctx carrying the per-message
// InvocationState. The stream layer sets this before invoking the Handler so
// the runner can skip already-completed or in-flight invocations without
// changing the Handler signature.
func WithInvocationState(ctx context.Context, p InvocationState) context.Context {
	return context.WithValue(ctx, invocationStateContextKey{}, p)
}

// InvocationStateFrom returns the InvocationState carried in ctx, or (nil,
// false) if absent. Callers must treat the value as best-effort context: it
// never panics and returns false when no invocation state was injected (e.g.
// when the runner is driven directly in tests).
func InvocationStateFrom(ctx context.Context) (InvocationState, bool) {
	p, ok := ctx.Value(invocationStateContextKey{}).(InvocationState)
	return p, ok
}

// Option configures an invocationState built by NewInvocationState. Options are
// test hooks; production passes none and uses the real clock.
type Option func(*invocationState)

// WithClock overrides the clock used for deadline comparisons and computation.
// It lets tests freeze or advance time without changing production semantics;
// production passes no option, so the real time.Now is used.
func WithClock(next func() time.Time) Option {
	return func(p *invocationState) { p.now = next }
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
	p := &invocationState{
		ctx:    ctx,
		store:  store,
		stream: stream,
		group:  group,
		msgID:  msgID,
		log:    log,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
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
func (p *invocationState) IsComplete(invocation string) bool {
	done, err := p.store.completed(p.ctx, p.stream, p.group, p.msgID, invocation)
	if err != nil {
		p.log.Debug("Invocation state: read failed; treating as not completed", "invocation", invocation, "error", err)
		return false
	}
	return done
}

// IsTerminal reports whether the invocation is terminal (complete or exhausted).
// A Redis read error fails open to false (not terminal), so the message is
// conservatively left pending rather than DLQ'd.
func (p *invocationState) IsTerminal(invocation string) bool {
	terminal, err := p.store.terminal(p.ctx, p.stream, p.group, p.msgID, invocation)
	if err != nil {
		p.log.Debug("Invocation state: read failed; treating as not terminal", "invocation", invocation, "error", err)
		return false
	}
	return terminal
}

// ClaimClassification atomically claims this message's one-time logical-event
// classification and reports whether this delivery won the claim. A store error
// is not fail-open here: classification counters must be exact, so the error is
// logged and (false, err) returned so the caller counts nothing.
func (p *invocationState) ClaimClassification() (bool, error) {
	claimed, err := p.store.claimClassification(p.ctx, p.stream, p.group, p.msgID)
	if err != nil {
		p.log.Debug("Invocation state: classification claim failed; not counting event", "error", err)
		return false, err
	}
	return claimed, nil
}

// MarkComplete logs but does not fail the handler on a write error: the message
// will simply be re-run later, preserving at-least-once semantics. State
// bookkeeping must never become a new failure source.
func (p *invocationState) MarkComplete(invocation string) {
	if err := p.store.markComplete(p.ctx, p.stream, p.group, p.msgID, invocation); err != nil {
		p.log.Warn("Invocation state: mark failed; message will be re-run later", "invocation", invocation, "error", err)
	}
}

// TryStart attempts to claim the invocation for a new execution, persisting the
// attempt's absolute deadline (now + timeout) as the running marker. It returns
// started true (with the 1-based attempt number) when the caller should execute,
// false when the invocation is already complete, exhausted, or protected by an
// active attempt deadline or retry backoff (this or another replica). When not
// started, wait is the duration until the invocation becomes eligible again
// (0 for terminal complete/exhausted).
//
// On a Redis error it fails OPEN (returns started=true, no wait): bookkeeping
// being down must not break at-least-once delivery, and running a duplicate is
// safe (handlers are idempotent) while never blocking recovery. The attempt
// number on the fail-open path is best-effort: when the read succeeded but the
// write failed, the would-be attempt number is returned so the runner's
// exhaustion decision still advances during a partial outage; when even the
// read failed, attempt 1 is returned (nothing was known). The persisted
// deadline matches the local timer by construction: the runner passes the same
// capped timeout to TryStart and to context.WithTimeout.
func (p *invocationState) TryStart(invocation string, timeout time.Duration) (started bool, attempt int, wait time.Duration) {
	now := p.now()
	started, attempt, wait, err := p.store.tryStart(p.ctx, p.stream, p.group, p.msgID, invocation, now, now.Add(timeout))
	if err != nil {
		p.log.Debug("Invocation state: try-start failed; failing open (running)", "invocation", invocation, "error", err)
		if attempt < 1 {
			attempt = 1
		}
		return true, attempt, 0
	}
	return started, attempt, wait
}

// RecordFailure persists a failed attempt's retry backoff so a later delivery
// is gated until now+backoff. The attempt count is read from the store. A write
// error is logged only: if the marker is lost, the invocation becomes eligible
// immediately (at-least-once), and the message stays pending for a later
// delivery regardless.
func (p *invocationState) RecordFailure(invocation string, backoff time.Duration) {
	next, err := p.store.finishFailure(p.ctx, p.stream, p.group, p.msgID, invocation, backoff, p.now())
	if err != nil {
		p.log.Warn("Invocation state: record failure failed; leaving field as-is (eligible immediately)",
			"invocation", invocation, "error", err)
		return
	}
	p.log.Debug("Invocation state: failure recorded; next attempt eligible", "invocation", invocation, "next", next)
}

// MarkExhausted records that the invocation's attempts are exhausted, making
// it terminal. A write error is logged only: if the marker is lost, a later
// delivery may re-run the invocation once (at-least-once), which is safe.
func (p *invocationState) MarkExhausted(invocation string, attempts int) {
	if err := p.store.markExhausted(p.ctx, p.stream, p.group, p.msgID, invocation, attempts); err != nil {
		p.log.Warn("Invocation state: mark exhausted failed", "invocation", invocation, "error", err)
	}
}
