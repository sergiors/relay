// Package dlq defines Relay's broker-neutral dead-letter read/write model.
//
// A dead-letter entry records ONE terminal source delivery's exhaustion of ONE
// invocation (or one placeholder entry for a source delivery that never reached
// a handler). The source is identified by an opaque logical delivery identity —
// source, source group, and source message ID — plus the invocation identity
// ("<app>/<handler>"). The entry ID is derived from exactly those inputs, never
// from a broker-assigned dead-letter ID, so writing the same exhaustion again
// (after a crash, an ambiguous transaction, or a failed source acknowledgement)
// is idempotent by primary key.
//
// The package deliberately knows nothing about Redis: the processing layer
// validates a claim against its broker state before calling a Store, and the
// PostgreSQL implementation lives in internal/state. Rows for one logical source
// delivery share a stable DeliveryID and start unsettled; only a confirmed
// source acknowledgement (or broker-side reconciliation) may settle them, which
// is what makes admin removal and retention safe. Only operator-facing metadata
// that has no behavioural role (the consumer name, delivery count) is carried
// through.
package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"time"
)

// NoHandler is the app/handler placeholder for an entry that has no handler
// invocation to attribute (a malformed or oversized message routed before a
// handler ran). It mirrors the stream layer's "-" fallback for a missing event.
const NoHandler = "-"

// ErrEntryUnsettled is returned by an admin removal when the entry's source
// delivery has not been acknowledged yet. Such an entry is still needed by
// delivery recovery and must never be deleted administratively; retention
// likewise removes only settled entries.
var ErrEntryUnsettled = errors.New("DLQ entry is not settled")

// DeliveryRef identifies one logical source delivery whose DLQ rows exist but
// are not yet known to be settled. It is the bounded reconciliation input: the
// worker asks the source broker whether this delivery is still recoverable.
type DeliveryRef struct {
	DeliveryID      string
	Source          string
	SourceGroup     string
	SourceMessageID string
}

// Entry is one dead-letter record. It is the read model `relay dlq` renders and
// replays, and the write model the stream layer persists before acknowledging a
// source delivery.
type Entry struct {
	// ID is the stable entry identity derived from the logical source delivery
	// and invocation identity (see NewID). It is what `relay dlq inspect`,
	// `replay`, and `rm` take.
	ID string
	// Source, SourceGroup, and SourceMessageID are the logical source delivery
	// identity. SourceMessageID is the source broker's delivery ID, treated as
	// an opaque stable token.
	Source          string
	SourceGroup     string
	SourceMessageID string
	// DeliveryID is the stable identity of the source delivery as a whole
	// (source + group + message ID). Every exhausted invocation from that one
	// delivery shares it, which is what lets the stream mark the whole delivery
	// settled after its source ACK.
	DeliveryID string
	// Consumer is the source consumer that routed the delivery. It is
	// diagnostic only.
	Consumer string
	// Event is the original payload exactly as dead-lettered. It is normally a
	// JSON object; "-" is the placeholder for a malformed message, and an
	// oversized message carries only a bounded diagnostic summary (never the
	// oversized payload).
	Event string
	// Reason is the human-readable exhaustion reason.
	Reason string
	// App and Handler identify the exact exhausted invocation; both are
	// NoHandler for a message that never reached a handler.
	App     string
	Handler string
	// Deliveries is the source delivery count (diagnostic only).
	Deliveries int64
	// HandlerAttempts is the handler execution attempt that exhausted the
	// invocation (0 for the malformed-message placeholder).
	HandlerAttempts int
	// Trace is the OPTIONAL compact trace lineage of the final failed
	// invocation (traceparent[|tracestate], never baggage). It is empty for a
	// pre-tracing entry, the placeholder, or when tracing was disabled.
	Trace string
	// Timestamp is the RFC3339 UTC time the entry was written.
	Timestamp string
	// SettledAt is the RFC3339 UTC time the source delivery's ACK was confirmed
	// and every DLQ row for that delivery was marked settled. It is empty while
	// the source delivery may still be recoverable; such an entry cannot be
	// removed administratively or by retention.
	SettledAt string
}

// Replayable reports whether the entry attributes a real handler invocation that
// `relay dlq replay` can re-execute. A malformed-message or oversized-event
// placeholder carries NoHandler for both and has no invocation to replay.
func (e Entry) Replayable() bool {
	return e.App != "" && e.App != NoHandler &&
		e.Handler != "" && e.Handler != NoHandler
}

// Store is the dead-letter persistence boundary shared by the stream processor
// and the operator CLI. It is broker-neutral: callers have already validated
// their own claim state, and an implementation neither reads nor mutates broker
// state.
type Store interface {
	// SaveDLQEntry inserts entry unless an entry with the same ID already
	// exists, and reports whether a NEW row was inserted. The write is
	// idempotent by ID, so retrying an exhaustion after a crash or an ambiguous
	// transaction never duplicates an entry. A new row is UNSETTLED.
	SaveDLQEntry(ctx context.Context, entry Entry) (inserted bool, err error)
	// MarkDLQDeliverySettled marks every row for one logical source delivery
	// settled. It is called only AFTER the source ACK is confirmed (or by the
	// worker's reconciliation once the source broker reports the delivery no
	// longer pending). It only moves unsettled rows to settled, never the
	// reverse.
	MarkDLQDeliverySettled(ctx context.Context, deliveryID string, settledAt time.Time) error
	// ListUnsettledDLQDeliveries returns up to limit distinct source deliveries
	// that still have unsettled rows, strictly after afterDeliveryID in stable
	// delivery-ID order. The worker uses it for bounded ACK/settlement
	// reconciliation and owns wraparound.
	ListUnsettledDLQDeliveries(ctx context.Context, afterDeliveryID string, limit int) ([]DeliveryRef, error)
	// ListDLQEntries returns every entry in write order (oldest first).
	ListDLQEntries(ctx context.Context) ([]Entry, error)
	// GetDLQEntry returns the entry with the exact ID. ok is false when no such
	// entry exists.
	GetDLQEntry(ctx context.Context, id string) (entry Entry, ok bool, err error)
	// DeleteDLQEntry removes exactly the one settled entry with id, reporting
	// whether it existed. It returns ErrEntryUnsettled when the row exists but
	// its source delivery may still be recoverable. It never touches broker
	// invocation state and never causes execution.
	DeleteDLQEntry(ctx context.Context, id string) (deleted bool, err error)
}

// NewDeliveryID derives the stable identity of one logical source delivery from
// its source, group, and opaque message ID. Every exhausted invocation from that
// delivery shares this ID, so the stream can mark the whole delivery settled
// after the source ACK.
func NewDeliveryID(source, group, messageID string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("relay.dlq.delivery.v1"))
	writeField(h, source)
	writeField(h, group)
	writeField(h, messageID)
	return hex.EncodeToString(h.Sum(nil))
}

// NewID derives the stable entry ID from a logical source delivery identity and
// an invocation identity. invocation is empty for a message that never reached
// a handler; that placeholder is a distinct identity from any real invocation.
//
// For a schedule occurrence, messageID is the source delivery identity of that
// occurrence and the stored event remains the occurrence envelope
// (occurrence_id/scheduled_at), so its provenance is preserved without making
// the DLQ identity broker- or schedule-specific.
//
// The inputs are length-prefixed before hashing so arbitrary source names, group
// names, message IDs, and invocation IDs can never alias one another. The
// attempt count and claim token are deliberately NOT inputs: the exhausted
// marker is terminal, so the same logical delivery and invocation always map to
// the same entry, which is what makes the PostgreSQL write idempotent.
func NewID(source, group, messageID, invocation string) string {
	return NewIDForDelivery(NewDeliveryID(source, group, messageID), invocation)
}

// NewIDForDelivery derives the stable entry ID from an already-computed delivery
// identity and an invocation identity. It lets the stream compute the delivery
// identity once and reuse it for every row plus the post-ACK settlement mark.
func NewIDForDelivery(deliveryID, invocation string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("relay.dlq.entry.v1"))
	writeField(h, deliveryID)
	if invocation == "" {
		_, _ = h.Write([]byte{0})
	} else {
		_, _ = h.Write([]byte{1})
		writeField(h, invocation)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// writeField writes one component with an 8-byte big-endian length prefix.
func writeField(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write([]byte(value))
}
