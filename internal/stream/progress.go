package stream

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// progressTTL is how long a message's invocation-progress hash survives after
// its last write. It is comfortably longer than the maximum pending lifetime
// (maxAttempts=5 × reclaim cadence ~2m + idle headroom), so a message that is
// abandoned or deleted from the stream is eventually cleaned up by Redis even
// if the eager clear on completion never runs. The eager clear (see
// processMessage/routeToDLQ) is the primary cleanup; the TTL is the safety net.
const progressTTL = 7 * 24 * time.Hour

// progressKey returns the Redis key holding a message's invocation-progress
// hash. The key is scoped by stream and group so multiple groups/consumers
// reading the same message ID never collide, and by msgID so each message has
// its own hash. The "relay:" prefix keeps the namespace Relay-owned.
//
// WHY percent-encoding: Redis stream/group names are arbitrary strings and
// colons are legal in them, so raw concatenation could alias two distinct
// (stream, group) pairs onto one key (e.g. stream="a:b", group="c" vs
// stream="a", group="b:c"), cross-linking invocation progress between groups
// for the same msgID. Percent-encoding the components makes the encoding
// injective: the ":" separators are unambiguous because any literal ":" in a
// component is escaped as "%3A" (and "%" as "%25" so the escape itself cannot
// be forged by a name containing "%3A"). msgID is a Redis stream ID
// ("<ms>-<seq>", digits and dashes only, as reported verbatim by go-redis), so
// it is safe to leave raw; it is encoded too only for uniformity.
func progressKey(stream, group, msgID string) string {
	return "relay:progress:" + encodeComponent(stream) + ":" + encodeComponent(group) + ":" + encodeComponent(msgID)
}

// encodeComponent percent-encodes a single key component so the ":" separators
// in progressKey are unambiguous. Only "%" and ":" need escaping: "%" first so
// a name containing a literal "%3A" cannot be mistaken for an escaped ":".
func encodeComponent(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	return strings.ReplaceAll(s, ":", "%3A")
}

// progressStore is a thin Redis-backed store for per-message invocation
// progress. Each key is a HASH mapping an invocation ID ("<function>/<handler>")
// to "ok" once that invocation has succeeded. It is the stream layer's domain
// (Redis), but the runner decides which invocations match, so the store is
// exposed to the runner through the InvocationProgress interface carried in the
// delivery context.
type progressStore struct {
	client *redis.Client
}

// completed reports whether the invocation has already succeeded for this
// message. redis.Nil (field absent) means not completed.
func (p *progressStore) completed(ctx context.Context, stream, group, msgID, invocation string) (bool, error) {
	v, err := p.client.HGet(ctx, progressKey(stream, group, msgID), invocation).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v == "ok", nil
}

// markSuccess records that the invocation succeeded for this message. HSET and
// EXPIRE are pipelined so the TTL is refreshed on every write without an extra
// round trip.
func (p *progressStore) markSuccess(ctx context.Context, stream, group, msgID, invocation string) error {
	key := progressKey(stream, group, msgID)
	pipe := p.client.Pipeline()
	pipe.HSet(ctx, key, invocation, "ok")
	pipe.Expire(ctx, key, progressTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// clear deletes the message's progress hash entirely. It is called eagerly on
// completion (successful ACK or DLQ routing) so the key does not linger.
func (p *progressStore) clear(ctx context.Context, stream, group, msgID string) error {
	return p.client.Del(ctx, progressKey(stream, group, msgID)).Err()
}

// completedSet reads the completion state of many invocations in one HMGET,
// returning a map keyed by invocation. It avoids per-invocation round trips when
// the caller knows the full set of invocations for a delivery up front.
func (p *progressStore) completedSet(ctx context.Context, stream, group, msgID string, invocations []string) (map[string]bool, error) {
	if len(invocations) == 0 {
		return map[string]bool{}, nil
	}
	vals, err := p.client.HMGet(ctx, progressKey(stream, group, msgID), invocations...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(invocations))
	for i, v := range vals {
		out[invocations[i]] = v == "ok"
	}
	return out, nil
}

// InvocationProgress is the read/write view of a single message's invocation
// progress, carried in the delivery context so the runner can skip invocations
// that already succeeded on a previous delivery. The invocation argument is the
// full "<function>/<handler>" ID; the stream layer never parses it.
type InvocationProgress interface {
	// Done reports whether the invocation already succeeded for this message.
	Done(invocation string) bool
	// MarkSuccess records that the invocation succeeded for this message.
	MarkSuccess(invocation string)
}

// invocationProgressKey is the context key carrying the per-message
// InvocationProgress into the Handler. It is unexported so the stream package
// owns the contract; the runner reads it best-effort via InvocationProgressFrom.
type invocationProgressKey struct{}

// WithInvocationProgress returns a child of ctx carrying the per-message
// InvocationProgress. The stream layer sets this before invoking the Handler so
// the runner can skip already-completed invocations without changing the Handler
// signature.
func WithInvocationProgress(ctx context.Context, p InvocationProgress) context.Context {
	return context.WithValue(ctx, invocationProgressKey{}, p)
}

// InvocationProgressFrom returns the InvocationProgress carried in ctx, or
// (nil, false) if absent. Callers must treat the value as best-effort context:
// it never panics and returns false when no progress was injected (e.g. when the
// runner is driven directly in tests or progress is disabled).
func InvocationProgressFrom(ctx context.Context) (InvocationProgress, bool) {
	p, ok := ctx.Value(invocationProgressKey{}).(InvocationProgress)
	return p, ok
}

// invocationProgress is the concrete per-message handle the stream layer
// injects. It binds a progressStore to one (stream, group, msgID) and captures
// the delivery context so the runner's Done/MarkSuccess calls hit the right key.
// Reads are lazy per-invocation (one HGET per Done call), which is acceptable
// for v1; the batch completedSet is available for callers that know the full set
// up front.
type invocationProgress struct {
	ctx    context.Context
	store  *progressStore
	stream string
	group  string
	msgID  string
	log    *log.Logger
}

// Done reads the invocation's completion state. On a Redis read error it logs
// and treats the invocation as not completed (fail-open): the runner re-runs it,
// preserving at-least-once semantics.
func (p *invocationProgress) Done(invocation string) bool {
	done, err := p.store.completed(p.ctx, p.stream, p.group, p.msgID, invocation)
	if err != nil {
		p.log.Printf("progress: read %q: %v; treating as not completed", invocation, err)
		return false
	}
	return done
}

// MarkSuccess records the invocation as succeeded. On a write error it logs but
// does not fail the handler: the message will simply be re-run later, preserving
// at-least-once semantics. Progress bookkeeping must never become a new failure
// source.
func (p *invocationProgress) MarkSuccess(invocation string) {
	if err := p.store.markSuccess(p.ctx, p.stream, p.group, p.msgID, invocation); err != nil {
		p.log.Printf("progress: mark %q: %v; message will be re-run later", invocation, err)
	}
}
