package stream

import (
	"context"
	"errors"
	"log"
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
//	"exhausted:<attempts>"         → attempts exhausted; terminal, never eligible
//	(absent)                      → eligible to execute
//
// "ok" stays bare because attempts are no longer needed after completion. Any
// value that does not parse under this grammar is treated as eligible.
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
// re-running. HSET + EXPIRE are pipelined.
func (p *invocationStore) markExhausted(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
	attempts int,
) error {
	key := invocationStateKey(stream, group, msgID)
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, exhaustedValue(attempts))
	pipe.Expire(ctx, key, invocationStateTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// clear deletes the message's invocation-state hash entirely. It is called
// eagerly on completion (successful ACK or DLQ routing) so the key does not
// linger.
func (p *invocationStore) clear(ctx context.Context, stream, group, msgID string) error {
	return p.client.Del(ctx, invocationStateKey(stream, group, msgID)).Err()
}

// completedSet reads the completion state of many invocations in one HMGET,
// returning a map keyed by invocation. It avoids per-invocation round trips when
// the caller knows the full set of invocations for a delivery up front.
func (p *invocationStore) completedSet(
	ctx context.Context,
	stream,
	group,
	msgID string,
	invocations []string,
) (map[string]bool, error) {
	if len(invocations) == 0 {
		return map[string]bool{}, nil
	}
	vals, err := p.client.HMGet(ctx, invocationStateKey(stream, group, msgID), invocations...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(invocations))
	for i, v := range vals {
		out[invocations[i]] = v == "ok"
	}
	return out, nil
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

// exhaustedValue encodes the terminal exhausted state as "exhausted:<attempts>".
func exhaustedValue(attempts int) string {
	return "exhausted:" + strconv.Itoa(attempts)
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
		n, err := strconv.Atoi(v[len("exhausted:"):])
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
// to the DLQ. The stream layer routes the whole message to the DLQ (per-invocation
// DLQ is not claimed; exhaustion of the last non-complete invocation routes the
// message).
var ErrInvocationExhausted = errors.New("invocation exhausted")

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
// when the runner is driven directly in tests or invocation tracking is
// disabled).
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
// stream layer injects. It binds an invocationStore to one (stream, group,
// msgID) and captures the delivery context so the runner's calls hit the right
// key. Reads are lazy per-invocation (one HGET per call), which is acceptable
// for v1; the batch completedSet is available for callers that know the full set
// up front.
func NewInvocationState(
	ctx context.Context,
	store *invocationStore,
	stream,
	group,
	msgID string,
	log *log.Logger,
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
	store  *invocationStore
	stream string
	group  string
	msgID  string
	log    *log.Logger
	now    func() time.Time
}

// IsComplete treats a Redis read error as not completed (fail-open): the runner
// re-runs the invocation, preserving at-least-once semantics.
func (p *invocationState) IsComplete(invocation string) bool {
	done, err := p.store.completed(p.ctx, p.stream, p.group, p.msgID, invocation)
	if err != nil {
		p.log.Printf("invocation state: read %q: %v; treating as not completed", invocation, err)
		return false
	}
	return done
}

// IsTerminal reports whether the invocation is terminal (complete or exhausted).
// A Redis read error fails open to false (not terminal), so the message is
// conservatively left pending rather than DLQ'd.
func (p *invocationState) IsTerminal(invocation string) bool {
	v, err := p.store.client.HGet(p.ctx, invocationStateKey(p.stream, p.group, p.msgID), invocation).Result()
	if err == redis.Nil {
		return false
	}
	if err != nil {
		p.log.Printf("invocation state: read %q: %v; treating as not terminal", invocation, err)
		return false
	}
	kind, _, _, ok := parseInvocationState(v)
	if !ok {
		return false
	}
	return kind == kindComplete || kind == kindExhausted
}

// MarkComplete logs but does not fail the handler on a write error: the message
// will simply be re-run later, preserving at-least-once semantics. State
// bookkeeping must never become a new failure source.
func (p *invocationState) MarkComplete(invocation string) {
	if err := p.store.markComplete(p.ctx, p.stream, p.group, p.msgID, invocation); err != nil {
		p.log.Printf("invocation state: mark %q: %v; message will be re-run later", invocation, err)
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
		p.log.Printf("invocation state: try-start %q: %v; failing open (running)", invocation, err)
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
		p.log.Printf("invocation state: record failure %q: %v; leaving field as-is (eligible immediately)", invocation, err)
		return
	}
	p.log.Printf("invocation state: %q failed; next attempt eligible at %s", invocation, next)
}

// MarkExhausted records that the invocation's attempts are exhausted, making
// it terminal. A write error is logged only: if the marker is lost, a later
// delivery may re-run the invocation once (at-least-once), which is safe.
func (p *invocationState) MarkExhausted(invocation string, attempts int) {
	if err := p.store.markExhausted(p.ctx, p.stream, p.group, p.msgID, invocation, attempts); err != nil {
		p.log.Printf("invocation state: mark exhausted %q: %v", invocation, err)
	}
}
