//go:build integration

package state

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/dlq"
)

// dlqEntryFor builds a deterministic entry for the given invocation ("" for the
// malformed-message placeholder), with event/reason bytes that include NUL and
// invalid UTF-8 so the JSONB round-trip must be byte-exact.
func dlqEntryFor(deliveryID, invocation, event, reason string, timestamp time.Time) dlq.Entry {
	entry := dlq.Entry{
		ID:              dlq.NewIDForDelivery(deliveryID, invocation),
		Source:          "events-test",
		SourceGroup:     "relay-test",
		SourceMessageID: "1760000000000-0",
		DeliveryID:      deliveryID,
		Consumer:        "worker-test",
		Event:           event,
		Reason:          reason,
		Deliveries:      7,
		HandlerAttempts: 3,
		Trace:           "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01|vendor=x",
		Timestamp:       timestamp.UTC().Format(time.RFC3339),
	}
	if invocation == "" {
		entry.App, entry.Handler = dlq.NoHandler, dlq.NoHandler
	} else {
		parts := strings.SplitN(invocation, "/", 2)
		entry.App, entry.Handler = parts[0], parts[1]
	}
	return entry
}

// lossyBytes is a payload no JSONB can carry directly: NUL, invalid UTF-8, and
// newlines. The persisted representation must restore it byte-for-byte.
const lossyBytes = "\x00nul \xff\xfe invalid \n \t {\"k\":1}"

// TestDLQEntrySaveAndGetRoundTripsLosslessly pins the durable representation
// end to end: payload AND identity bytes (NUL, invalid UTF-8, newlines) survive
// a real JSONB write/read, since encoding/json base64-encodes the []byte
// fields. Identity metadata also flows through the reconciliation listing
// unchanged, so the broker query the worker performs points at the exact
// delivery.
func TestDLQEntrySaveAndGetRoundTripsLosslessly(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1760000000000-0")
	// Event and Reason carry NUL and invalid UTF-8: identity metadata stays
	// plain, while payload bytes survive only through the lossless encoding.
	// Event, Reason, and the broker diagnostic identity fields carry bytes no
	// JSONB text can hold directly: NUL and invalid UTF-8 must round-trip
	// byte-for-byte, so the private JSON representation base64-encodes them.
	entry := dlqEntryFor(deliveryID, "fn/index.run", lossyBytes, "exhausted: "+lossyBytes, at)
	entry.Source = lossyBytes
	entry.SourceGroup = "\xffgroup"
	entry.SourceMessageID = "\x00message"
	entry.Consumer = "\xffworker"
	entry.App = "fn"
	entry.Handler = "index.run"

	inserted, err := c.SaveDLQEntry(ctx, entry)
	if err != nil || !inserted {
		t.Fatalf("SaveDLQEntry = (%v,%v), want (true,nil)", inserted, err)
	}

	got, ok, err := c.GetDLQEntry(ctx, entry.ID)
	if err != nil || !ok {
		t.Fatalf("GetDLQEntry = (%+v,%v,%v)", got, ok, err)
	}
	if got.ID != entry.ID || got.DeliveryID != deliveryID {
		t.Fatalf("identity = (%q,%q), want (%q,%q)", got.ID, got.DeliveryID, entry.ID, deliveryID)
	}
	if got.Source != entry.Source || got.SourceMessageID != entry.SourceMessageID || got.SourceGroup != entry.SourceGroup || got.Consumer != entry.Consumer {
		t.Fatalf("source identity = %+v", got)
	}
	// The byte-exact payloads (including NUL and invalid UTF-8) decode
	// losslessly out of JSONB.
	if got.Event != entry.Event {
		t.Fatalf("Event round-trip mismatch: got %q want %q", got.Event, entry.Event)
	}
	if got.Reason != entry.Reason {
		t.Fatalf("Reason round-trip mismatch: got %q want %q", got.Reason, entry.Reason)
	}
	if got.App != "fn" || got.Handler != "index.run" {
		t.Fatalf("attribution = %q/%q", got.App, got.Handler)
	}
	if got.Deliveries != 7 || got.HandlerAttempts != 3 {
		t.Fatalf("counts = %d/%d, want 7/3", got.Deliveries, got.HandlerAttempts)
	}
	if got.Trace != entry.Trace {
		t.Fatalf("Trace = %q", got.Trace)
	}
	if got.Timestamp != entry.Timestamp {
		t.Fatalf("Timestamp = %q, want the caller's %q", got.Timestamp, entry.Timestamp)
	}
	// A fresh row is unsettled: only a confirmed source ACK settles it.
	if got.SettledAt != "" {
		t.Fatalf("fresh row must be unsettled, SettledAt = %q", got.SettledAt)
	}

	// The placeholder identity is distinct and replayable-free.
	placeholder := dlqEntryFor(deliveryID, "", "-", "decode event: missing 'event' field", at)
	if inserted, err := c.SaveDLQEntry(ctx, placeholder); err != nil || !inserted {
		t.Fatalf("SaveDLQEntry placeholder = (%v,%v)", inserted, err)
	}
	got2, ok, err := c.GetDLQEntry(ctx, placeholder.ID)
	if err != nil || !ok {
		t.Fatalf("GetDLQEntry placeholder = (%v,%v)", ok, err)
	}
	if got2.ID == entry.ID || got2.App != dlq.NoHandler || got2.HandlerAttempts != 3 {
		t.Fatalf("placeholder = %+v (must be a distinct identity)", got2)
	}
	if got2.Replayable() {
		t.Fatalf("placeholder must not be replayable: %+v", got2)
	}

	// The lossy identity also flows through the reconciliation listing, which
	// must hand the worker the EXACT stored source identity (the broker query
	// targets the delivery that actually dead-lettered, not a sanitized copy).
	refs, err := c.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil {
		t.Fatalf("list unsettled: %v", err)
	}
	for _, r := range refs {
		if r.DeliveryID == deliveryID {
			if r.Source != entry.Source || r.SourceGroup != entry.SourceGroup ||
				r.SourceMessageID != entry.SourceMessageID {
				t.Fatalf("reconciliation identity for %s = %+v, want the byte-exact stored identity",
					r.DeliveryID, r)
			}
			return
		}
	}
	t.Fatalf("delivery %s missing from the reconciliation listing: %+v", deliveryID, refs)
}

func TestDLQEntrySaveIsIdempotentByIDAndPreservesFirstPayload(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1760000000000-0")
	first := dlqEntryFor(deliveryID, "fn/index.run", `{"a":1}`, "first write", at)
	if inserted, err := c.SaveDLQEntry(ctx, first); err != nil || !inserted {
		t.Fatalf("first = (%v,%v), want (true,nil)", inserted, err)
	}

	// The retry (an ambiguous transaction replaying the same exhaustion after a
	// crash, or a failed source ACK) has identical identity but different bytes:
	// the committed row must win, untouched.
	second := first
	second.Event = `{"a":999}`
	second.Reason = "retry of the same exhaustion"
	second.Deliveries = 42
	inserted, err := c.SaveDLQEntry(ctx, second)
	if err != nil || inserted {
		t.Fatalf("confluent save = (%v,%v), want (false,nil)", inserted, err)
	}
	got, ok, err := c.GetDLQEntry(ctx, first.ID)
	if err != nil || !ok {
		t.Fatalf("GetDLQEntry = (%v,%v)", ok, err)
	}
	if got.Event != `{"a":1}` || got.Reason != "first write" || got.Deliveries != 7 {
		t.Fatalf("the first committed payload was overwritten: %+v", got)
	}
	if got.SettledAt != "" {
		t.Fatalf("a conflict no-op must not settle anything: %+v", got)
	}

	// Validation guards: an empty ID and a malformed timestamp are refused
	// rather than written.
	if _, err := c.SaveDLQEntry(ctx, dlq.Entry{ID: ""}); err == nil {
		t.Fatal("an empty entry ID must be refused")
	}
	badTime := first
	badTime.ID = dlq.NewIDForDelivery(deliveryID, "fn/other")
	badTime.Timestamp = "not-a-time"
	if _, err := c.SaveDLQEntry(ctx, badTime); err == nil {
		t.Fatal("a malformed timestamp must be refused")
	}
	if _, ok, err := c.GetDLQEntry(ctx, badTime.ID); err != nil || ok {
		t.Fatalf("refused entry must not exist (ok=%v err=%v)", ok, err)
	}
}

func TestDLQEntryConcurrentSameIdentityPersistsOneRow(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1760000000000-0")
	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	entry := dlqEntryFor(deliveryID, "fn/index.run", `{"a":1}`, "boom", at)

	const n = 16
	start := make(chan struct{})
	wrote := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			wrote[i], errs[i] = c.SaveDLQEntry(ctx, entry)
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if wrote[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly one", winners)
	}
	entries, err := c.ListDLQEntries(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].ID != entry.ID {
		t.Fatalf("rows = %+v, want exactly [%s]", entries, entry.ID)
	}
}

func TestDLQMarkSettledCoversWholeDeliveryOnlyAndIsMonotonic(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	deliveryA := dlq.NewDeliveryID("events-test", "relay-test", "1-0")
	deliveryB := dlq.NewDeliveryID("events-test", "relay-test", "2-0")

	// Delivery A exhausts two invocations; delivery B exhausts one.
	for _, inv := range []string{"fnA/h", "fnB/h"} {
		entry := dlqEntryFor(deliveryA, inv, `{"a":1}`, "boom", time.Now())
		if inserted, err := c.SaveDLQEntry(ctx, entry); err != nil || !inserted {
			t.Fatalf("save A/%s = (%v,%v)", inv, inserted, err)
		}
	}
	entryB := dlqEntryFor(deliveryB, "fnC/h", `{"b":1}`, "boom", time.Now())
	if inserted, err := c.SaveDLQEntry(ctx, entryB); err != nil || !inserted {
		t.Fatalf("save B = (%v,%v)", inserted, err)
	}

	ackTime := time.Date(2026, 10, 26, 9, 1, 0, 0, time.UTC)
	if err := c.MarkDLQDeliverySettled(ctx, deliveryA, ackTime); err != nil {
		t.Fatalf("mark settled A: %v", err)
	}

	// Every row of delivery A is settled with the recorded instant; delivery B
	// is untouched.
	for _, inv := range []string{"fnA/h", "fnB/h"} {
		id := dlq.NewIDForDelivery(deliveryA, inv)
		got, ok, err := c.GetDLQEntry(ctx, id)
		if err != nil || !ok {
			t.Fatalf("get A/%s = (%v,%v)", inv, ok, err)
		}
		if got.SettledAt == "" {
			t.Fatalf("A/%s must be settled: %+v", inv, got)
		}
		if parsed, err := time.Parse(time.RFC3339, got.SettledAt); err != nil || !parsed.Equal(ackTime.UTC()) {
			t.Fatalf("A/%s SettledAt = %q, want %q", inv, got.SettledAt, ackTime.UTC().Format(time.RFC3339))
		}
	}
	if kept, ok, _ := c.GetDLQEntry(ctx, entryB.ID); !ok || kept.SettledAt != "" {
		t.Fatalf("delivery B row changed by A's settlement: %+v (ok %v)", kept, ok)
	}

	// Settlement is monotonic: a later mark (e.g. from reconciliation after a
	// crash replay) cannot rewrite the confirmed instant or re-settle anything.
	later := time.Date(2026, 10, 26, 9, 5, 0, 0, time.UTC)
	if err := c.MarkDLQDeliverySettled(ctx, deliveryA, later); err != nil {
		t.Fatalf("re-mark settled A: %v", err)
	}
	if got, _, _ := c.GetDLQEntry(ctx, dlq.NewIDForDelivery(deliveryA, "fnA/h")); got.SettledAt != ackTime.UTC().Format(time.RFC3339) {
		t.Fatalf("SettledAt changed by a re-mark: %q", got.SettledAt)
	}

	// A settlement mark for delivery B makes only B deletable.
	if err := c.MarkDLQDeliverySettled(ctx, deliveryB, later); err != nil {
		t.Fatalf("mark settled B: %v", err)
	}
	refs, err := c.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil {
		t.Fatalf("list unsettled: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("unsettled deliveries = %v, want none", refs)
	}
}

func TestDLQListUnsettledReturnsStableOrderAndPreservesCorruptRows(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	// Three distinct deliveries, plus a settled delivery that must not appear.
	deliveries := []struct{ group, msg string }{
		{"relay-test", "3-0"},
		{"relay-test", "1-0"},
		{"other-group", "2-0"},
	}
	for _, d := range deliveries {
		entry := dlqEntryFor(dlq.NewDeliveryID("events-test", d.group, d.msg), "fn/h", `{"a":1}`, "boom", time.Now())
		if inserted, err := c.SaveDLQEntry(ctx, entry); err != nil || !inserted {
			t.Fatalf("save %s/%s = (%v,%v)", d.group, d.msg, inserted, err)
		}
	}
	settled := dlqEntryFor(dlq.NewDeliveryID("events-test", "relay-test", "9-9"), "fn/h", `{"a":1}`, "boom", time.Now())
	if inserted, err := c.SaveDLQEntry(ctx, settled); err != nil || !inserted {
		t.Fatalf("save settled = (%v,%v)", inserted, err)
	}
	ack := time.Date(2026, 10, 26, 9, 1, 0, 0, time.UTC)
	if err := c.MarkDLQDeliverySettled(ctx, settled.DeliveryID, ack); err != nil {
		t.Fatalf("mark settled: %v", err)
	}

	// The listing answers bounded DISTINCT deliveries in stable delivery-ID
	// order with the decoded source identity, never the settled delivery.
	refs, err := c.ListUnsettledDLQDeliveries(ctx, "", 2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("refs = %+v, want exactly 2 (bounded)", refs)
	}
	seen := map[string]bool{}
	for _, r := range refs {
		if seen[r.DeliveryID] {
			t.Fatalf("duplicate delivery %s in the list", r.DeliveryID)
		}
		seen[r.DeliveryID] = true
		if r.Source == "" || r.SourceGroup == "" || r.SourceMessageID == "" {
			t.Fatalf("ref = %+v, want a complete source identity", r)
		}
	}
	if seen[settled.DeliveryID] {
		t.Fatal("the settled delivery must not be listed")
	}

	all, err := c.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("unsettled = %d, want 3", len(all))
	}

	// A corrupt stored payload (a future/incompatible row shape: structurally
	// valid JSONB but not decodable into the DLQ entry model) must never be
	// answered with a guessed identity nor deleted: its delivery is skipped
	// (preserved unsettled) while the remaining deliveries still reconcile.
	target := all[0].DeliveryID
	if _, err := c.db.ExecContext(ctx, `UPDATE dlq SET data = '{"event": 1}' WHERE delivery_id = $1`, target); err != nil {
		t.Fatalf("corrupt payload: %v", err)
	}
	refs, err = c.ListUnsettledDLQDeliveries(ctx, "", 10)
	if err != nil {
		t.Fatalf("list with corrupt row: %v", err)
	}
	var corruptRef *dlq.DeliveryRef
	for i := range refs {
		if refs[i].DeliveryID == target {
			corruptRef = &refs[i]
		}
	}
	if corruptRef == nil {
		t.Fatalf("the corrupt delivery must still advance the reconciliation cursor: %+v", refs)
	}
	if corruptRef.Source != "" || corruptRef.SourceGroup != "" || corruptRef.SourceMessageID != "" {
		t.Fatalf("the corrupt delivery must not be answered with a guessed identity: %+v", corruptRef)
	}
	if len(refs) != 3 {
		t.Fatalf("refs after corruption = %d, want all 3 cursor-bearing deliveries", len(refs))
	}
	// The corrupt row is preserved, not deleted, and still unsettled.
	var n int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dlq WHERE delivery_id = $1 AND settled_at_ms IS NULL`, target).Scan(&n); err != nil || n != 1 {
		t.Fatalf("corrupt row preserved = (%d,%v), want 1 unsettled", n, err)
	}
}

func TestDLQDeleteOnlySettledAndPrunePreservesUnsettled(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	deliveryOld := dlq.NewDeliveryID("events-test", "relay-test", "1-0")
	deliveryFresh := dlq.NewDeliveryID("events-test", "relay-test", "2-0")
	deliveryUnsettled := dlq.NewDeliveryID("events-test", "relay-test", "3-0")
	oldAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	freshAt := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)

	old := dlqEntryFor(deliveryOld, "fn/h", `{"a":1}`, "boom", oldAt)
	fresh := dlqEntryFor(deliveryFresh, "fn/h", `{"b":2}`, "boom", freshAt)
	unsettled := dlqEntryFor(deliveryUnsettled, "fn/h", `{"c":3}`, "boom", oldAt)
	for _, e := range []dlq.Entry{old, fresh, unsettled} {
		if inserted, err := c.SaveDLQEntry(ctx, e); err != nil || !inserted {
			t.Fatalf("save %s = (%v,%v)", e.ID, inserted, err)
		}
	}
	ackTime := time.Date(2026, 10, 26, 9, 1, 0, 0, time.UTC)
	for _, deliveryID := range []string{deliveryOld, deliveryFresh} {
		if err := c.MarkDLQDeliverySettled(ctx, deliveryID, ackTime); err != nil {
			t.Fatalf("mark %s settled: %v", deliveryID, err)
		}
	}

	// Admin removal refuses an UNSETTLED row: it may still be needed by
	// delivery recovery.
	deleted, err := c.DeleteDLQEntry(ctx, unsettled.ID)
	if err == nil {
		t.Fatalf("deleting an unsettled entry must fail with ErrEntryUnsettled, got deleted=%v err=%v", deleted, err)
	}
	if !strings.Contains(err.Error(), "not settled") {
		t.Fatalf("delete error = %v, want ErrEntryUnsettled", err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("the unsettled row must remain after the refused delete")
	}

	// A settled row is removed exactly once; a second remove is a no-op.
	if deleted, err := c.DeleteDLQEntry(ctx, fresh.ID); err != nil || !deleted {
		t.Fatalf("delete settled = (%v,%v), want (true,nil)", deleted, err)
	}
	if deleted, err := c.DeleteDLQEntry(ctx, fresh.ID); err != nil || deleted {
		t.Fatalf("delete gone = (%v,%v), want (false,nil)", deleted, err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, fresh.ID); ok {
		t.Fatal("the settled row must be gone")
	}

	// An unknown ID is a no-op, never an error.
	if deleted, err := c.DeleteDLQEntry(ctx, "no such id"); err != nil || deleted {
		t.Fatalf("delete unknown = (%v,%v), want (false,nil)", deleted, err)
	}

	// Retention removes at most limit settled entries at or before the cutoff,
	// oldest first, and NEVER an unsettled row regardless of age.
	cutoff := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	removed, err := c.PruneDLQEntries(ctx, cutoff, 0)
	if err != nil || removed != 0 {
		t.Fatalf("prune with a non-positive limit = (%d,%v), want (0,nil)", removed, err)
	}
	// Both settled rows are at or before the cutoff; the unsettled row (older
	// than both) is not eligible. The bounded batch removes exactly 1 (the
	// oldest settled row first).
	removed, err = c.PruneDLQEntries(ctx, cutoff, 1)
	if err != nil || removed != 1 {
		t.Fatalf("bounded prune = (%d,%v), want 1 (only the oldest settled row)", removed, err)
	}
	if _, ok, _ := c.GetDLQEntry(ctx, old.ID); ok {
		t.Fatal("the old settled row must be pruned")
	}
	if _, ok, _ := c.GetDLQEntry(ctx, unsettled.ID); !ok {
		t.Fatal("the unsettled row is never removed by retention")
	}
	// A later batch removes the remaining settled row; the unsettled row is
	// still there and is the only depth left.
	removed, err = c.PruneDLQEntries(ctx, cutoff, 10)
	if err != nil {
		t.Fatalf("prune 10: %v", err)
	}
	var depth int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dlq`).Scan(&depth); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if depth != 1 {
		t.Fatalf("rows = %d, want 1 (only the never-deletable unsettled row)", depth)
	}
	got, ok, _ := c.GetDLQEntry(ctx, unsettled.ID)
	if !ok || got.SettledAt != "" {
		t.Fatalf("surviving row = %+v (ok %v)", got, ok)
	}
}

// TestDLQListUnsettledPaginationIsStrictAfterCursor pins the fair-pagination
// contract the reconciliation loop owns: rows come back strictly AFTER the
// cursor in stable delivery-ID order, a full page exposes the cursor to
// continue from (the returned page's last delivery), and the pages joined
// together cover every unsettled delivery exactly once without reopening
// earlier ones (so a permanently pending delivery cannot starve later rows).
func TestDLQListUnsettledPaginationIsStrictAfterCursor(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	const pageCount = 5
	byID := map[string]dlq.Entry{}
	for i := 0; i < pageCount; i++ {
		deliveryID := dlq.NewDeliveryID("events-test", "relay-test", fmt.Sprintf("%d-0", i+1))
		entry := dlqEntryFor(deliveryID, "fn/h", `{"a":1}`, "boom", time.Now())
		if inserted, err := c.SaveDLQEntry(ctx, entry); err != nil || !inserted {
			t.Fatalf("save %d = (%v,%v)", i, inserted, err)
		}
		byID[deliveryID] = entry
	}

	// Page through with limit 2: every page starts strictly after the cursor
	// (the previous page's last delivery), so a full page of 5 takes 3 calls
	// and never repeats a delivery. The delivery IDs are hashes, so nothing in
	// the test may assume the seeded order matches the delivery-ID order.
	const limit = 2
	cursor := ""
	seen := map[string]bool{}
	ordered := []string{} // page order == stable delivery-ID order
	calls := 0
	for {
		refs, err := c.ListUnsettledDLQDeliveries(ctx, cursor, limit)
		if err != nil {
			t.Fatalf("list after %q: %v", cursor, err)
		}
		calls++
		for _, r := range refs {
			if seen[r.DeliveryID] {
				t.Fatalf("delivery %s repeated across pages", r.DeliveryID)
			}
			if r.DeliveryID <= cursor {
				t.Fatalf("delivery %s is not strictly after cursor %q", r.DeliveryID, cursor)
			}
			if _, ok := byID[r.DeliveryID]; !ok {
				t.Fatalf("unknown delivery %s returned", r.DeliveryID)
			}
			if r.Source != byID[r.DeliveryID].Source || r.SourceMessageID != byID[r.DeliveryID].SourceMessageID {
				t.Fatalf("ref identity = %+v, want the stored source identity", r)
			}
			seen[r.DeliveryID] = true
			ordered = append(ordered, r.DeliveryID)
		}
		if len(refs) < limit {
			break // short page: everything past the cursor was returned
		}
		cursor = refs[len(refs)-1].DeliveryID
	}
	if calls < 3 {
		t.Fatalf("pagination took %d calls, want the 3 pages 5 rows with limit 2 needs", calls)
	}
	if len(seen) != pageCount {
		t.Fatalf("seen = %d deliveries, want all %d across the pages", len(seen), pageCount)
	}

	// The empty cursor restarts the listing from the beginning (the smallest
	// delivery ID again, in the same stable order).
	restart, err := c.ListUnsettledDLQDeliveries(ctx, "", 1)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if len(restart) != 1 || restart[0].DeliveryID != ordered[0] {
		t.Fatalf("first delivery after restart = %+v, want %s", restart, ordered[0])
	}

	// A settled delivery leaves the listing entirely (pagination never returns
	// settled rows, and settling does not corrupt the cursor: the page after
	// the second-to-last delivery becomes empty, so the loop wraps around and
	// the settled one is never visited again). The delivery IDs are hashes, so
	// the test is deliberately agnostic to which fixture is first/last.
	if err := c.MarkDLQDeliverySettled(ctx, ordered[len(ordered)-1], time.Now()); err != nil {
		t.Fatalf("settle: %v", err)
	}
	tail, err := c.ListUnsettledDLQDeliveries(ctx, ordered[len(ordered)-2], 10)
	if err != nil {
		t.Fatalf("list after second-to-last: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("refs = %+v, want none (the last delivery is settled; the page after it is empty)", tail)
	}
}

// TestDLQListOrderIsWriteOrderWithStableTieBreak pins the operator listing
// order: write (created_at) order, then the stable ID as the tie-breaker
// among same-millisecond writes.
func TestDLQListOrderIsWriteOrderWithStableTieBreak(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)

	deliveryID := dlq.NewDeliveryID("events-test", "relay-test", "1-0")
	// Same-millisecond writes: the ID breaks the tie deterministically.
	sameMs := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	a := dlqEntryFor(deliveryID, "fnB/index.run", "{}", "second id", sameMs)
	b := dlqEntryFor(deliveryID, "fnA/index.run", "{}", "first id", sameMs)
	for _, e := range []dlq.Entry{a, b} {
		if inserted, err := c.SaveDLQEntry(ctx, e); err != nil || !inserted {
			t.Fatalf("save %s = (%v,%v)", e.ID, inserted, err)
		}
	}
	later := dlqEntryFor(dlq.NewDeliveryID("events-test", "relay-test", "2-0"), "fnC/h", "{}", "later", time.Date(2026, 10, 26, 9, 0, 1, 0, time.UTC))
	if inserted, err := c.SaveDLQEntry(ctx, later); err != nil || !inserted {
		t.Fatalf("save later = (%v,%v)", inserted, err)
	}

	entries, err := c.ListDLQEntries(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("rows = %d, want 3", len(entries))
	}
	// Write (created_at) order, then id as the tie-breaker.
	want := []string{a.ID, b.ID, later.ID}
	minID := a.ID
	if b.ID < minID {
		want[0], want[1] = b.ID, a.ID
	}
	if entries[0].ID != want[0] || entries[1].ID != want[1] || entries[2].ID != want[2] {
		t.Fatalf("order = [%s %s %s], want [%s %s %s]",
			entries[0].ID, entries[1].ID, entries[2].ID, want[0], want[1], want[2])
	}
}
