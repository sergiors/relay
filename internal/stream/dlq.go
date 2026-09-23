package stream

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// DLQEntry is one decoded Relay DLQ stream entry in the CURRENT format: exactly
// one entry per exhausted invocation (or one placeholder entry for a message
// that never reached a handler), with the flat field set written by dlqPayload.
// It is the read model the `relay dlq` commands render and replay; it carries no
// Redis or worker concerns.
//
// There is deliberately no older-format variant, alias, or fallback: an entry
// that does not carry every current field is malformed and rejected (see
// ParseDLQEntry), never reinterpreted.
type DLQEntry struct {
	// ID is the DLQ stream entry ID (the "<ms>-<seq>" Redis stream ID). It is
	// the identity the `relay dlq inspect/replay/rm` commands take.
	ID string
	// OriginalStream and OriginalID identify the source message this entry was
	// dead-lettered from.
	OriginalStream string
	OriginalID     string
	// Group and Consumer are the consumer-group identity that routed the
	// original message.
	Group    string
	Consumer string
	// Event is the original payload string exactly as dead-lettered. It is
	// normally a JSON object; "-" is the placeholder for a malformed message.
	Event string
	// Reason is the human-readable exhaustion reason.
	Reason string
	// Function and Handler identify the exact exhausted invocation; both are
	// "-" for a malformed message that never reached a handler.
	Function string
	Handler  string
	// Deliveries is the Redis Stream/PEL delivery count (diagnostic only).
	Deliveries int64
	// HandlerAttempts is the handler execution attempt that exhausted the
	// invocation (0 for the malformed-message placeholder).
	HandlerAttempts int
	// Timestamp is the RFC3339 UTC time the entry was written.
	Timestamp string
}

// Replayable reports whether the entry attributes a real handler invocation
// that `relay dlq replay` can re-execute: both Function and Handler name the
// exact exhausted invocation recorded when it was dead-lettered. A
// malformed-message placeholder (a message routed to the DLQ before it reached a
// handler) carries dlqNoHandler ("-") for both and has no invocation to replay,
// so it is not replayable — the entry is retained for inspection or removal.
// There is deliberately no reinterpretation of the placeholder as a real
// invocation.
func (e DLQEntry) Replayable() bool {
	return e.Function != "" && e.Function != dlqNoHandler &&
		e.Handler != "" && e.Handler != dlqNoHandler
}

// ParseDLQEntry decodes one DLQ stream entry into the current format. id is the
// stream entry ID; values is the entry's field map as returned by Redis
// (XRANGE/XREAD), where every field is a bulk string. Every current field must
// be present; a missing field or an unparseable integer is an error naming the
// field, so a malformed entry is surfaced rather than silently coerced. There is
// no compatibility parsing.
func ParseDLQEntry(id string, values map[string]any) (DLQEntry, error) {
	e := DLQEntry{ID: id}
	var err error
	if e.OriginalStream, err = dlqString(values, "original_stream"); err != nil {
		return DLQEntry{}, err
	}
	if e.OriginalID, err = dlqString(values, "original_id"); err != nil {
		return DLQEntry{}, err
	}
	if e.Group, err = dlqString(values, "group"); err != nil {
		return DLQEntry{}, err
	}
	if e.Consumer, err = dlqString(values, "consumer"); err != nil {
		return DLQEntry{}, err
	}
	if e.Event, err = dlqString(values, "event"); err != nil {
		return DLQEntry{}, err
	}
	if e.Reason, err = dlqString(values, "reason"); err != nil {
		return DLQEntry{}, err
	}
	if e.Function, err = dlqString(values, "function"); err != nil {
		return DLQEntry{}, err
	}
	if e.Handler, err = dlqString(values, "handler"); err != nil {
		return DLQEntry{}, err
	}
	if e.Deliveries, err = dlqInt(values, "deliveries"); err != nil {
		return DLQEntry{}, err
	}
	attempts, err := dlqInt(values, "handler_attempts")
	if err != nil {
		return DLQEntry{}, err
	}
	e.HandlerAttempts = int(attempts)
	if e.Timestamp, err = dlqString(values, "timestamp"); err != nil {
		return DLQEntry{}, err
	}
	return e, nil
}

// dlqString reads one current field as a string, rejecting a missing field or a
// non-string value.
func dlqString(values map[string]any, field string) (string, error) {
	v, ok := values[field]
	if !ok {
		return "", fmt.Errorf("DLQ entry is missing the %q field", field)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("DLQ entry %q field is not a string", field)
	}
	return s, nil
}

// dlqInt reads one current field as an integer. The value is a bulk string on
// the wire; anything that does not parse is an error naming the field.
func dlqInt(values map[string]any, field string) (int64, error) {
	s, err := dlqString(values, field)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("DLQ entry %q field %q is not an integer", field, s)
	}
	return n, nil
}

// RedisDLQStore is the Redis-backed read/delete view of a Relay-owned DLQ
// stream. It owns the client it is constructed with and closes it via Close, so
// the caller's lifetime is explicit. It lives in this package because the DLQ
// stream naming and entry format are stream-layer concerns; the CLI never
// touches Redis directly.
type RedisDLQStore struct {
	client *redis.Client
	stream string
}

// NewRedisDLQStore builds the store for opts' Redis, reading the DLQ stream
// derived from sourceStream (see DLQStreamFor). It creates the client; Close
// closes it.
func NewRedisDLQStore(opts *redis.Options, sourceStream string) *RedisDLQStore {
	return &RedisDLQStore{
		client: redis.NewClient(opts),
		stream: DLQStreamFor(sourceStream),
	}
}

// List returns every entry in the DLQ stream in Redis stream order (ascending
// entry ID). A malformed entry fails the whole listing with an error naming it,
// rather than being silently skipped: a listing that hid data would be worse
// than one that surfaces the corruption.
func (s *RedisDLQStore) List(ctx context.Context) ([]DLQEntry, error) {
	msgs, err := s.client.XRange(ctx, s.stream, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("read DLQ stream %q: %w", s.stream, err)
	}
	entries := make([]DLQEntry, 0, len(msgs))
	for _, msg := range msgs {
		e, err := ParseDLQEntry(msg.ID, msg.Values)
		if err != nil {
			return nil, fmt.Errorf("DLQ entry %s: %w", msg.ID, err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Get returns the DLQ entry with the exact stream entry id. ok is false when no
// such entry exists. A malformed id is an error.
func (s *RedisDLQStore) Get(ctx context.Context, id string) (DLQEntry, bool, error) {
	msgs, err := s.client.XRange(ctx, s.stream, id, id).Result()
	if err != nil {
		return DLQEntry{}, false, fmt.Errorf("read DLQ entry %q: %w", id, err)
	}
	if len(msgs) == 0 {
		return DLQEntry{}, false, nil
	}
	e, err := ParseDLQEntry(msgs[0].ID, msgs[0].Values)
	if err != nil {
		return DLQEntry{}, false, fmt.Errorf("DLQ entry %s: %w", id, err)
	}
	return e, true, nil
}

// Delete removes exactly the one entry with id (XDEL with a single ID), leaving
// every other entry in place. deleted reports whether an entry with that exact
// ID existed (XDEL's deleted count is 1), so the caller can distinguish a real
// removal from a no-op on an unknown ID.
func (s *RedisDLQStore) Delete(ctx context.Context, id string) (deleted bool, err error) {
	n, err := s.client.XDel(ctx, s.stream, id).Result()
	if err != nil {
		return false, fmt.Errorf("delete DLQ entry %q: %w", id, err)
	}
	return n > 0, nil
}

// Close closes the store's Redis client. It is idempotent at the redis.Client
// level.
func (s *RedisDLQStore) Close() error {
	return s.client.Close()
}
