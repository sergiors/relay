package stream

import (
	"context"
	"log"
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
// to "ok" once that invocation has completed. It is the stream layer's domain
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
// round trip.
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

// InvocationState is the read/write view of a single message's invocation
// state, carried in the delivery context so the runner can skip invocations
// that already completed on a previous delivery. The invocation argument is the
// full "<function>/<handler>" ID; the stream layer never parses it.
type InvocationState interface {
	IsComplete(invocation string) bool
	MarkComplete(invocation string)
}

// invocationStateContextKey is the context key carrying the per-message
// InvocationState into the Handler. It is unexported so the stream package owns
// the contract; the runner reads it best-effort via InvocationStateFrom.
type invocationStateContextKey struct{}

// WithInvocationState returns a child of ctx carrying the per-message
// InvocationState. The stream layer sets this before invoking the Handler so
// the runner can skip already-completed invocations without changing the Handler
// signature.
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

// invocationState is the concrete per-message handle the stream layer injects.
// It binds an invocationStore to one (stream, group, msgID) and captures the
// delivery context so the runner's IsComplete/MarkComplete calls hit the right
// key. Reads are lazy per-invocation (one HGET per IsComplete call), which is
// acceptable for v1; the batch completedSet is available for callers that know
// the full set up front.
type invocationState struct {
	ctx    context.Context
	store  *invocationStore
	stream string
	group  string
	msgID  string
	log    *log.Logger
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
