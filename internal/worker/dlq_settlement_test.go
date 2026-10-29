package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"relay/internal/dlq"
	"relay/internal/testutil"
)

// stubSettlementStore doubles dlqSettlementStore in the worker tests: it
// returns the configured unsettled deliveries (or a canned list error) and
// records every MarkDLQDeliverySettled call (delivery, instant, or mark error).
type stubSettlementStore struct {
	mu      sync.Mutex
	refs    []dlq.DeliveryRef
	listErr error
	markErr map[string]error // by delivery ID
	listed  []int            // limits seen per List call
	afters  []string         // cursor seen per List call
	marks   []string         // settled delivery IDs, in order
	settled map[string]time.Time
}

func (s *stubSettlementStore) ListUnsettledDLQDeliveries(_ context.Context, afterDeliveryID string, limit int) ([]dlq.DeliveryRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listed = append(s.listed, limit)
	s.afters = append(s.afters, afterDeliveryID)
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]dlq.DeliveryRef, 0, len(s.refs))
	for _, r := range s.refs {
		if r.DeliveryID > afterDeliveryID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeliveryID < out[j].DeliveryID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *stubSettlementStore) MarkDLQDeliverySettled(_ context.Context, deliveryID string, settledAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.markErr[deliveryID]; err != nil {
		return err
	}
	if s.settled == nil {
		s.settled = map[string]time.Time{}
	}
	s.settled[deliveryID] = settledAt
	s.marks = append(s.marks, deliveryID)
	return nil
}

func (s *stubSettlementStore) limitSeen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.listed) == 0 {
		return 0
	}
	return s.listed[len(s.listed)-1]
}

func (s *stubSettlementStore) lastAfter() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.afters) == 0 {
		return ""
	}
	return s.afters[len(s.afters)-1]
}

func (s *stubSettlementStore) afterSeen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.afters...)
}

func (s *stubSettlementStore) markedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.marks...)
}

func (s *stubSettlementStore) settledAt(deliveryID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.settled[deliveryID]
	return at, ok
}

// stubInspector doubles deliveryPendingInspector: one canned answer per source
// message ID, with an error injected for a matching identity.
type stubInspector struct {
	pending map[string]bool // by "stream/group/id"
	errBy   string
	groups  chan struct{} // optional block seam (unused in these tests)
}

func (s *stubInspector) Pending(_ context.Context, source, group, messageID string) (bool, error) {
	key := source + "/" + group + "/" + messageID
	if key == s.errBy {
		return false, errors.New("redis unavailable")
	}
	return s.pending[key], nil
}

func TestDLQSettlementTickSettlesOnlyConfirmedDeliveries(t *testing.T) {
	confirmed := dlq.DeliveryRef{
		DeliveryID: "d-confirmed", Source: "stream", SourceGroup: "group", SourceMessageID: "5-0",
	}
	stillPending := dlq.DeliveryRef{
		DeliveryID: "d-pending", Source: "stream", SourceGroup: "group", SourceMessageID: "6-0",
	}
	store := &stubSettlementStore{refs: []dlq.DeliveryRef{confirmed, stillPending}}
	// The broker reports the confirmed delivery is no longer in its group's PEL
	// (the XACK must already have succeeded or its reply was lost); the pending
	// delivery is still recoverable.
	inspector := &stubInspector{pending: map[string]bool{
		"stream/group/5-0": false,
		"stream/group/6-0": true,
	}}
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

	dlqSettlementTick(context.Background(), store, inspector, "", func() time.Time { return now }, testutil.DiscardLogger())

	// Only the confirmed delivery is marked, with the injected now.
	if got := store.markedIDs(); len(got) != 1 || got[0] != confirmed.DeliveryID {
		t.Fatalf("marked = %v, want exactly the confirmed delivery", got)
	}
	if at, ok := store.settledAt(confirmed.DeliveryID); !ok || at != now {
		t.Fatalf("settled instant = %v ok=%v, want %v", at, ok, now)
	}
	// The batch bound is passed through so a large backlog drains in bounded
	// passes.
	if got := store.limitSeen(); got != dlqSettlementBatch {
		t.Fatalf("list limit = %d, want the bounded batch %d", got, dlqSettlementBatch)
	}
}

func TestDLQSettlementTickPagesPastPermanentPendingRows(t *testing.T) {
	refs := make([]dlq.DeliveryRef, 0, dlqSettlementBatch+1)
	pending := make(map[string]bool, dlqSettlementBatch+1)
	for i := 0; i < dlqSettlementBatch; i++ {
		id := fmt.Sprintf("d-%03d", i)
		ref := dlq.DeliveryRef{
			DeliveryID: id, Source: "stream", SourceGroup: "group", SourceMessageID: id,
		}
		if i == dlqSettlementBatch/2 {
			// A malformed stored identity is preserved but must still advance the
			// pagination cursor.
			ref.Source = ""
		}
		refs = append(refs, ref)
		pending["stream/group/"+id] = true
	}
	later := dlq.DeliveryRef{
		DeliveryID: "d-999", Source: "stream", SourceGroup: "group", SourceMessageID: "999-0",
	}
	refs = append(refs, later)
	pending["stream/group/999-0"] = false

	store := &stubSettlementStore{refs: refs}
	inspector := &stubInspector{pending: pending}
	logger := testutil.DiscardLogger()

	cursor := dlqSettlementTick(context.Background(), store, inspector, "", time.Now, logger)
	if len(store.markedIDs()) != 0 {
		t.Fatalf("the first full page is all pending, marked = %v", store.markedIDs())
	}
	if cursor != "d-099" {
		t.Fatalf("cursor = %q, want the last returned delivery d-099", cursor)
	}

	cursor = dlqSettlementTick(context.Background(), store, inspector, cursor, time.Now, logger)
	marked := store.markedIDs()
	if len(marked) != 1 || marked[0] != later.DeliveryID {
		t.Fatalf("later delivery was not reached past a full pending page: marked = %v", marked)
	}
	if cursor != "" {
		t.Fatalf("cursor = %q after a short page, want wraparound to empty", cursor)
	}
	if _, ok := store.settledAt("d-000"); ok {
		t.Fatal("an earlier pending delivery must remain preserved")
	}
}

func TestDLQSettlementTickPreservesRowsOnAmbiguity(t *testing.T) {
	d := dlq.DeliveryRef{DeliveryID: "d-1", Source: "stream", SourceGroup: "group", SourceMessageID: "1-0"}

	// A broker error (NOGROUP, an unavailable Redis) is never proof of
	// settlement: the rows are preserved for a later pass.
	store := &stubSettlementStore{refs: []dlq.DeliveryRef{d}}
	dlqSettlementTick(context.Background(), store, &stubInspector{
		errBy: "stream/group/1-0",
	}, "", time.Now, testutil.DiscardLogger())
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("a broker error must settle nothing, marked = %v", got)
	}

	// A listing failure aborts the whole pass (no observation, no marks).
	store = &stubSettlementStore{listErr: errors.New("database down")}
	dlqSettlementTick(context.Background(), store, &stubInspector{}, "", time.Now, testutil.DiscardLogger())
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("a listing failure must settle nothing, marked = %v", got)
	}

	// An incomplete stored identity is preserved, never guessed into a broker
	// query.
	broken := dlq.DeliveryRef{DeliveryID: "d-broken", Source: "", SourceGroup: "group", SourceMessageID: "1-0"}
	store = &stubSettlementStore{refs: []dlq.DeliveryRef{broken, d}}
	dlqSettlementTick(context.Background(), store, &stubInspector{
		pending: map[string]bool{"stream/group/1-0": false},
	}, "", time.Now, testutil.DiscardLogger())
	if got := store.markedIDs(); len(got) != 1 || got[0] != d.DeliveryID {
		t.Fatalf("marked = %v, want only the complete identity", got)
	}

	// A settlement-write failure is retried later: the pass continues and the
	// failed delivery stays unsettled here.
	store = &stubSettlementStore{
		refs: []dlq.DeliveryRef{d, {DeliveryID: "d-2", Source: "stream", SourceGroup: "group", SourceMessageID: "2-0"}},
		markErr: map[string]error{
			"d-1": errors.New("postgres down"),
		},
	}
	dlqSettlementTick(context.Background(), store, &stubInspector{
		pending: map[string]bool{"stream/group/1-0": false, "stream/group/2-0": false},
	}, "", time.Now, testutil.DiscardLogger())
	if got := store.markedIDs(); len(got) != 1 || got[0] != "d-2" {
		t.Fatalf("after one failed mark, marked = %v, want the remaining delivery", got)
	}
	if _, ok := store.settledAt("d-1"); ok {
		t.Fatal("the failed mark must not settle its delivery")
	}
}

func TestDLQSettlementLoopStopsOnCancel(t *testing.T) {
	store := &stubSettlementStore{}
	inspector := &stubInspector{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dlqSettlementLoop(ctx, store, inspector, testutil.DiscardLogger())
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dlqSettlementLoop did not stop on cancel")
	}
}

func TestDLQSettlementLoopPerformsInitialReconcile(t *testing.T) {
	d := dlq.DeliveryRef{DeliveryID: "d-1", Source: "stream", SourceGroup: "group", SourceMessageID: "1-0"}
	store := &stubSettlementStore{refs: []dlq.DeliveryRef{d}}
	inspector := &stubInspector{pending: map[string]bool{"stream/group/1-0": false}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		dlqSettlementLoop(ctx, store, inspector, testutil.DiscardLogger())
	}()
	// Wait for the initial pass to settle the delivery, then cancel: a crash
	// between a successful ACK and its settlement mark is repaired promptly.
	deadline := time.After(2 * time.Second)
	for {
		if _, ok := store.settledAt(d.DeliveryID); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("dlqSettlementLoop did not perform the initial reconciliation pass")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dlqSettlementLoop did not stop on cancel")
	}
}
