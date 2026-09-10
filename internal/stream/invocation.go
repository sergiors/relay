package stream

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// invocationStateTTL is how long a message's invocation-state hash survives
// after its last write. It is comfortably longer than the maximum pending
// lifetime (maxAttempts=5 × reclaim cadence ~2m + idle headroom), so a message
// that is abandoned or deleted from the stream is eventually cleaned up by
// Redis even if the eager clear on completion never runs. The eager clear (see
// processMessage/routeToDLQ) is the primary cleanup; the TTL is the safety net.
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
// The "relay:invocation:" prefix superseded an earlier "relay:progress:" one.
// State written by older binaries under the old prefix is simply not found
// (IsComplete reads miss), so pending messages from an upgrade re-run their
// handlers once — acceptable under at-least-once, where handlers must remain
// idempotent regardless. The old keys self-expire via their TTL; no migration
// or cleanup code is needed for them.
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

// invocationStore is a thin Redis-backed store for per-message invocation
// state. Each key is a HASH mapping an invocation ID ("<function>/<handler>")
// to a short value describing that invocation's lifecycle for this message:
//
//	"ok"              → the invocation completed on a previous delivery
//	"running:<nano>"  → an attempt is (or was) executing, protected until the
//	                    absolute Unix-nano deadline it carries
//	(absent)          → eligible to execute
//
// The value is deliberately a short string so the same field convention can
// later carry richer values (e.g. a "next_attempt_at:<nano>" retry gate) without
// changing the key layout or the read path. It is the stream layer's domain
// (Redis), but the runner decides which invocations match, so the store is
// exposed to the runner through the InvocationState interface carried in the
// delivery context.
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
// round trip. Writing "ok" overwrites any "running:<nano>" marker the same
// invocation carried, so a successful attempt atomically transitions the field
// from protected to complete.
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
//	"ok"            → already complete; not started
//	"running:<nano>" with now < deadline → a protected attempt is in flight
//	                  (this or another replica); not started
//	absent, expired, or unparseable → eligible: HSET "running:<deadline>" and
//	                  report started
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
) (started bool, err error) {
	key := invocationStateKey(stream, group, msgID)
	v, err := p.client.HGet(ctx, key, invocation).Result()
	if err == redis.Nil {
		// Field absent: eligible.
	} else if err != nil {
		return false, err
	} else if v == "ok" {
		return false, nil
	} else if dl, ok := parseRunning(v); ok && now.Add(-clockSkewTolerance).Before(dl) {
		// A protected attempt is still within its persisted deadline (minus the
		// clock-skew tolerance, so a slightly-fast replica's inflated deadline
		// does not block a slower replica beyond the intended window).
		return false, nil
	}
	// Absent, expired, or unparseable: start a new attempt. HSET + EXPIRE are
	// pipelined so the TTL is refreshed on the write without an extra round trip.
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, runningValue(deadline))
	pipe.Expire(ctx, key, invocationStateTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// endRunning removes the running marker for the invocation (normal failure or
// timeout cleanup), leaving the field absent so a later delivery is eligible
// again. It is a plain HDEL: if the field was already overwritten to "ok" by a
// concurrent MarkComplete, HDEL is a no-op and the completion stands.
func (p *invocationStore) endRunning(
	ctx context.Context,
	stream,
	group,
	msgID,
	invocation string,
) error {
	return p.client.HDel(ctx, invocationStateKey(stream, group, msgID), invocation).Err()
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

// runningValue encodes a protected attempt's absolute deadline as the field
// value "running:<unixnano>". The "running:" prefix distinguishes it from the
// "ok" completion sentinel; the Unix-nano suffix is the deadline at which the
// attempt is considered abandoned.
func runningValue(deadline time.Time) string {
	return "running:" + strconv.FormatInt(deadline.UnixNano(), 10)
}

// parseRunning decodes a "running:<unixnano>" field value into its deadline. It
// returns ok=false for any value that is not a well-formed running marker (e.g.
// "ok", a future richer value, or a corrupt marker), which the caller treats as
// eligible.
func parseRunning(v string) (time.Time, bool) {
	const prefix = "running:"
	if !strings.HasPrefix(v, prefix) {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(v[len(prefix):], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

// InvocationState is the read/write view of a single message's invocation
// state, carried in the delivery context so the runner can decide whether to
// execute an invocation. The invocation argument is the full
// "<function>/<handler>" ID; the stream layer never parses it.
//
// The lifecycle of a single invocation's field value:
//
//	attempt starts  → "running:<deadline>"   (TryStart)
//	completes       → "ok"                   (MarkComplete, overwrites running)
//	fails/times out → (absent)               (EndRunning, HDEL)
//
// A crash mid-attempt leaves "running:<deadline>", which self-expires at its
// deadline; recovery waits it out (bounded staleness of at most one timeout).
type InvocationState interface {
	IsComplete(invocation string) bool
	MarkComplete(invocation string)
	TryStart(invocation string, timeout time.Duration) bool
	EndRunning(invocation string)
}

// invocationStateContextKey is the context key carrying the per-message
// InvocationState into the Handler. It is unexported so the stream package owns
// the contract; the runner reads it best-effort via InvocationStateFrom.
type invocationStateContextKey struct{}

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
// true when the caller should execute, false when the invocation is already
// complete or protected by an active attempt deadline (this or another replica).
//
// On a Redis error it fails OPEN (returns true, writes no state): bookkeeping
// being down must not break at-least-once delivery, and running a duplicate is
// safe (handlers are idempotent) while never blocking recovery. The persisted
// deadline matches the local timer by construction: the runner passes the same
// capped timeout to TryStart and to context.WithTimeout.
func (p *invocationState) TryStart(invocation string, timeout time.Duration) bool {
	now := p.now()
	started, err := p.store.tryStart(p.ctx, p.stream, p.group, p.msgID, invocation, now, now.Add(timeout))
	if err != nil {
		p.log.Printf("invocation state: try-start %q: %v; failing open (running)", invocation, err)
		return true
	}
	return started
}

// EndRunning clears the running marker after a normal failure or timeout, so a
// later delivery is eligible again. A write error is logged only: if the marker
// is lost (e.g. a crash after the failure), it self-expires at its deadline
// anyway, bounding staleness to at most one timeout.
func (p *invocationState) EndRunning(invocation string) {
	if err := p.store.endRunning(p.ctx, p.stream, p.group, p.msgID, invocation); err != nil {
		p.log.Printf("invocation state: end-running %q: %v", invocation, err)
	}
}
