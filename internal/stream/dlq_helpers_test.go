package stream

import (
	"context"
	"strconv"
	"testing"
	"time"

	"relay/internal/dlq"
)

// dlqEntriesFor returns every durable DLQ row whose source message ID is msgID,
// failing the test on a store error. It is the assertion seam for tests that
// read the PostgreSQL DLQ through the broker-neutral store.
func dlqEntriesFor(t *testing.T, store dlq.Store, msgID string) []dlq.Entry {
	t.Helper()
	entries, err := store.ListDLQEntries(context.Background())
	if err != nil {
		t.Fatalf("list DLQ entries: %v", err)
	}
	var out []dlq.Entry
	for _, entry := range entries {
		if entry.SourceMessageID == msgID {
			out = append(out, entry)
		}
	}
	return out
}

// exhaustedValue renders a persisted exhausted marker
// ("exhausted:<attempt>:<token>") for tests that pin the terminal grammar, plus
// the optional read-only legacy ":dlq" suffix an older Relay still parses as
// terminal. Production never writes the suffix; PostgreSQL persistence is
// idempotent by the entry's stable ID instead.
func exhaustedValue(claim InvocationClaim, persisted bool) string {
	value := "exhausted:" + strconv.Itoa(claim.Attempt) + ":" + claim.Token
	if persisted {
		value += ":dlq"
	}
	return value
}

// isExhaustedDLQValue reports whether v carries the legacy ":dlq" suffix
// (an exhausted marker parsed by parseExhaustedValue as terminal, but no longer
// written by production).
func isExhaustedDLQValue(v string) bool {
	_, persisted, ok := parseExhaustedValue(v)
	return ok && persisted
}

// fakeDLQStore is an in-memory broker-neutral dlq.Store for processMessage
// tests. It records inserted entries (insertion order preserved) and supports
// injected save/settle failures.
type fakeDLQStore struct {
	entries []dlq.Entry
	saveErr error
}

func (f *fakeDLQStore) SaveDLQEntry(_ context.Context, entry dlq.Entry) (bool, error) {
	if f.saveErr != nil {
		return false, f.saveErr
	}
	for _, existing := range f.entries {
		if existing.ID == entry.ID {
			return false, nil
		}
	}
	if entry.DeliveryID == "" {
		entry.DeliveryID = dlq.NewDeliveryID(entry.Source, entry.SourceGroup, entry.SourceMessageID)
	}
	f.entries = append(f.entries, entry)
	return true, nil
}

func (f *fakeDLQStore) MarkDLQDeliverySettled(_ context.Context, deliveryID string, settledAt time.Time) error {
	for i := range f.entries {
		if f.entries[i].DeliveryID == deliveryID {
			f.entries[i].SettledAt = settledAt.UTC().Format(time.RFC3339)
		}
	}
	return nil
}

func (f *fakeDLQStore) ListUnsettledDLQDeliveries(_ context.Context, afterDeliveryID string, limit int) ([]dlq.DeliveryRef, error) {
	if limit <= 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	var refs []dlq.DeliveryRef
	for _, entry := range f.entries {
		if entry.SettledAt != "" || entry.DeliveryID == "" || entry.DeliveryID <= afterDeliveryID || seen[entry.DeliveryID] {
			continue
		}
		seen[entry.DeliveryID] = true
		refs = append(refs, dlq.DeliveryRef{
			DeliveryID:      entry.DeliveryID,
			Source:          entry.Source,
			SourceGroup:     entry.SourceGroup,
			SourceMessageID: entry.SourceMessageID,
		})
		if len(refs) >= limit {
			break
		}
	}
	return refs, nil
}

func (f *fakeDLQStore) ListDLQEntries(context.Context) ([]dlq.Entry, error) {
	return append([]dlq.Entry(nil), f.entries...), nil
}

func (f *fakeDLQStore) GetDLQEntry(_ context.Context, id string) (dlq.Entry, bool, error) {
	for _, entry := range f.entries {
		if entry.ID == id {
			return entry, true, nil
		}
	}
	return dlq.Entry{}, false, nil
}

func (f *fakeDLQStore) DeleteDLQEntry(_ context.Context, id string) (bool, error) {
	for i, entry := range f.entries {
		if entry.ID == id {
			f.entries = append(f.entries[:i], f.entries[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}
