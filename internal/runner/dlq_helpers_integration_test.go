//go:build integration

package runner

import (
	"context"
	"sync/atomic"

	"relay/internal/dlq"
)

// failDLQStore decorates a dlq.Store and fails every SaveDLQEntry with err
// until ok is set, so a DLQ persistence failure (e.g. an unavailable
// PostgreSQL) is deterministic while every other operation still reaches the
// real store. The flip is an atomic bool: SaveDLQEntry runs on the consumer's
// goroutine while the test flips it.
type failDLQStore struct {
	dlq.Store
	err error
	ok  atomic.Bool
}

func (f *failDLQStore) SaveDLQEntry(ctx context.Context, entry dlq.Entry) (bool, error) {
	if !f.ok.Load() {
		return false, f.err
	}
	return f.Store.SaveDLQEntry(ctx, entry)
}

// listDLQEntriesFor is the assertion seam over the durable rows: it fails the
// test on a store error and returns the rows for one source message ID in
// write order.
func listDLQEntriesFor(t interface {
	Helper()
	Fatalf(string, ...any)
}, store dlq.Store, msgID string) []dlq.Entry {
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
