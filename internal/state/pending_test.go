package state

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"relay/internal/testutil"
)

// pendingOcc builds a deterministic pending record with the given id.
func pendingOcc(id string, due time.Time) PendingOccurrence {
	return PendingOccurrence{
		ID:          id,
		App:         "app-" + id,
		Schedule:    "sched-" + id,
		Handler:     "jobs." + id,
		ScheduledAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
		NextAttempt: due,
	}
}

// TestPendingOccurrencePersistAndResolve pins the outbox lifecycle: an insert
// creates the row with zero attempts, a re-insert of the same ID is a no-op that
// preserves progress, and a delete removes it idempotently.
func TestPendingOccurrencePersistAndResolve(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	inserted, err := c.SavePendingOccurrence(ctx, pendingOcc("a", now))
	if err != nil || !inserted {
		t.Fatalf("first save = (%v,%v), want (true,nil)", inserted, err)
	}
	// Re-saving the same occurrence is idempotent: no new row, no error.
	inserted, err = c.SavePendingOccurrence(ctx, pendingOcc("a", now.Add(time.Hour)))
	if err != nil || inserted {
		t.Fatalf("second save = (%v,%v), want (false,nil)", inserted, err)
	}

	// Progress written after the re-save must not have been rewound.
	if err := c.ReschedulePendingOccurrence(ctx, "a", now.Add(2*time.Minute)); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	claimed, err := c.ClaimPendingOccurrences(ctx, now.Add(3*time.Minute), now.Add(4*time.Minute), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	if got := claimed[0]; got.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (reschedule increments)", got.Attempts)
	}
	if !claimed[0].NextAttempt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("next = %v, want %v", claimed[0].NextAttempt, now.Add(2*time.Minute))
	}
	// Intent fields round-trip exactly.
	if claimed[0].App != "app-a" || claimed[0].Schedule != "sched-a" || claimed[0].Handler != "jobs.a" ||
		!claimed[0].ScheduledAt.Equal(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("intent fields not preserved: %+v", claimed[0])
	}

	if err := c.DeletePendingOccurrence(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Delete is idempotent (already gone is not an error).
	if err := c.DeletePendingOccurrence(ctx, "a"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if _, ok, err := c.NextPendingDue(ctx, now.Add(4*time.Minute)); err != nil || ok {
		t.Fatalf("NextPendingDue after delete = (ok %v, err %v), want (false,nil)", ok, err)
	}
}

// TestPendingOccurrenceClaimRespectsDueAndLease pins that a claim takes only due,
// unleased records and that a lease hides a record from a second claim until it
// expires.
func TestPendingOccurrenceClaimRespectsDueAndLease(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	must := func(id string, due time.Time) {
		t.Helper()
		if _, err := c.SavePendingOccurrence(ctx, pendingOcc(id, due)); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}
	must("due", now.Add(-time.Minute))
	must("future", now.Add(time.Hour))

	claimed, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "due" {
		t.Fatalf("claim = %v, want only the due record", ids(claimed))
	}
	// The first claim holds a lease: a second claim before expiry takes nothing.
	again, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("second claim = %v, want none (leased)", ids(again))
	}
	// After the lease expires the still-unresolved record is reclaimable.
	later := now.Add(2 * time.Minute)
	reclaimed, err := c.ClaimPendingOccurrences(ctx, later, later.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != "due" {
		t.Fatalf("reclaim = %v, want the expired-lease record", ids(reclaimed))
	}
}

// TestNextPendingDue pins the wake computation: the earliest MAX(next, lease)
// across rows, and ok=false on an empty outbox.
func TestNextPendingDue(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return now }
	if _, ok, err := c.NextPendingDue(ctx, now); err != nil || ok {
		t.Fatalf("empty NextPendingDue = (ok %v, err %v), want (false,nil)", ok, err)
	}
	for _, p := range []PendingOccurrence{
		pendingOcc("a", now.Add(10*time.Minute)),
		pendingOcc("b", now.Add(2*time.Minute)),
		pendingOcc("c", now.Add(5*time.Minute)),
	} {
		if _, err := c.SavePendingOccurrence(ctx, p); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	due, ok, err := c.NextPendingDue(ctx, now)
	if err != nil || !ok {
		t.Fatalf("NextPendingDue = (ok %v, err %v), want (true,nil)", ok, err)
	}
	if !due.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("NextPendingDue = %v, want the minimum %v", due, now.Add(2*time.Minute))
	}
}

// TestNextPendingDueReturnsExpiryWhenBackoffOutlivesRetention pins the
// expiration-cleanup wakeup: a record whose next retry (backoff) is scheduled
// after its retention deadline must yield the deadline as its wake instant, so
// the worker wakes at expiry and removes the row promptly instead of sleeping
// past the exact 7-day limit. At the deadline the row is excluded from the
// wake computation and the bounded cleanup deletes it.
func TestNextPendingDueReturnsExpiryWhenBackoffOutlivesRetention(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c.nowFn = func() time.Time { return now }
	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("a", now.Add(-time.Minute))); err != nil {
		t.Fatalf("save: %v", err)
	}
	// The retention deadline expires shortly; the retry is rescheduled well past
	// it (a backoff that would outlive retention).
	expiry := now.Add(2 * time.Minute)
	if _, err := c.db.ExecContext(ctx,
		`UPDATE schedule_pending SET expires_at_ms = ?, next_attempt_ms = ? WHERE id = 'a'`,
		expiry.UnixMilli(), now.Add(30*time.Minute).UnixMilli()); err != nil {
		t.Fatalf("stage row: %v", err)
	}

	// Wake at the earlier of the two: the retention deadline, not the retry.
	due, ok, err := c.NextPendingDue(ctx, now)
	if err != nil || !ok {
		t.Fatalf("NextPendingDue = (ok %v, err %v), want (true,nil)", ok, err)
	}
	if !due.Equal(expiry) {
		t.Fatalf("NextPendingDue = %v, want the expiry wakeup %v", due, expiry)
	}

	// At the deadline the row is expired: excluded from the wake computation and
	// removed by the bounded cleanup.
	if _, ok, err := c.NextPendingDue(ctx, expiry); err != nil || ok {
		t.Fatalf("NextPendingDue at expiry = (ok %v, err %v), want (false,nil)", ok, err)
	}
	n, err := c.ExpirePendingOccurrences(ctx, expiry, 10)
	if err != nil || n != 1 {
		t.Fatalf("ExpirePendingOccurrences = (%d,%v), want (1,nil)", n, err)
	}
	var remaining int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schedule_pending WHERE id = 'a'`).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("rows after cleanup = %d, want 0 (removed at expiry)", remaining)
	}
}

// TestPendingOccurrenceConcurrentClaimsAreDisjoint hammers the claim path from
// many goroutines and proves each record is claimed at most once (the DB lease,
// not a process mutex, is the coordination) and no SQLite lock/busy error
// surfaces.
func TestPendingOccurrenceConcurrentClaimsAreDisjoint(t *testing.T) {
	ctx := context.Background()
	c, buf := captureLogger(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	const n = 40
	for i := 0; i < n; i++ {
		p := pendingOcc("id-"+string(rune('a'+i%26))+string(rune('0'+i/26)), now.Add(-time.Minute))
		if _, err := c.SavePendingOccurrence(ctx, p); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	const workers = 8
	var (
		mu     sync.Mutex
		seen   = map[string]int{}
		wg     sync.WaitGroup
		errs   = make(chan error, workers)
		claims int
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 3)
				if err != nil {
					errs <- err
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, p := range got {
					seen[p.ID]++
					claims++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent claim: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if claims != n {
		t.Fatalf("total claims = %d, want exactly %d (no record claimed twice)", claims, n)
	}
	for id, k := range seen {
		if k != 1 {
			t.Fatalf("record %q claimed %d times, want 1", id, k)
		}
	}
	assertNoBusy(t, buf)
}

// TestPendingOccurrenceSurvivesReopen pins the crash/reopen contract: a record
// persisted before Close is still claimable and retriable after reopening the
// same file.
func TestPendingOccurrenceSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := c1.SavePendingOccurrence(ctx, pendingOcc("crash", now.Add(-time.Minute))); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	claimed, err := c2.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim after reopen: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "crash" {
		t.Fatalf("after reopen claim = %v, want the persisted record", ids(claimed))
	}
	// The immutable intent survived the restart.
	if claimed[0].Handler != "jobs.crash" || !claimed[0].ScheduledAt.Equal(time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("intent not preserved across reopen: %+v", claimed[0])
	}
}

// TestPendingOccurrenceSchemaAndNoHistoryTable pins the EXACT schedule_pending
// schema (id/data/attempts/next_attempt_ms/lease_until_ms/expires_at_ms with the
// due index), that the outbox is its own table, and that no execution/history
// table is introduced: the state DB stays a view, not an execution source.
func TestPendingOccurrenceSchemaAndNoHistoryTable(t *testing.T) {
	c := openTestState(t)
	if !tableExists(t, c, "schedule_pending") {
		t.Fatal("schedule_pending table must exist")
	}
	cols := tableColumnSet(t, c, "schedule_pending")
	for _, want := range []string{"id", "data", "attempts", "next_attempt_ms", "lease_until_ms", "expires_at_ms"} {
		if !cols[want] {
			t.Errorf("schedule_pending missing column %q: %v", want, cols)
		}
	}
	if len(cols) != 6 {
		t.Errorf("schedule_pending must have exactly id/data/attempts/next_attempt_ms/lease_until_ms/expires_at_ms, got %v", cols)
	}
	// The occurrence intent is a BLOB payload, not dedicated columns.
	for _, gone := range []string{"app", "schedule", "handler", "scheduled_at"} {
		if cols[gone] {
			t.Errorf("schedule_pending must not have per-intent column %q (intent lives in data)", gone)
		}
	}
	// The due index lives in sqlite_master under type 'index' (not 'table').
	var idx int
	if err := c.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'schedule_pending_due'`).Scan(&idx); err != nil {
		t.Fatalf("check index: %v", err)
	}
	if idx == 0 {
		t.Error("schedule_pending_due index must exist")
	}
	for _, table := range []string{"schedule_history", "schedule_outbox_events", "executions"} {
		if tableExists(t, c, table) {
			t.Fatalf("unexpected history/execution table %q must not exist", table)
		}
	}
}

// TestPendingOccurrenceDataColumnHoldsIntent pins that the intent is actually
// stored in the data BLOB as JSONB (not duplicated into columns) and that it is
// valid JSON rendering back the exact occurrence.
func TestPendingOccurrenceDataColumnHoldsIntent(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	o := pendingOcc("a", time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC))
	if _, err := c.SavePendingOccurrence(ctx, o); err != nil {
		t.Fatalf("save: %v", err)
	}
	var (
		payload string
		typ     string
	)
	if err := c.db.QueryRowContext(ctx,
		`SELECT json(data), typeof(data) FROM schedule_pending WHERE id = ?`, o.ID).Scan(&payload, &typ); err != nil {
		t.Fatalf("read data: %v", err)
	}
	for _, want := range []string{`"app":"app-a"`, `"schedule":"sched-a"`, `"handler":"jobs.a"`, `"scheduled_at":"2026-07-01T08:00:00Z"`} {
		if !strings.Contains(payload, want) {
			t.Errorf("data payload %s missing %s", payload, want)
		}
	}
}

// TestPendingOccurrenceCorruptPayloadRetained pins the safe corrupt-BLOB
// contract: a payload that is not valid JSON, and one that is valid JSON but
// incomplete, are BOTH returned with DecodeErr set (empty intent, so the caller
// cannot publish the wrong occurrence) and are RETAINED — never deleted. A valid
// sibling in the same batch is returned normally.
func TestPendingOccurrenceCorruptPayloadRetained(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	// One valid record and two corrupt ones, all due now.
	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("good", now.Add(-time.Minute))); err != nil {
		t.Fatalf("save good: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO schedule_pending (id, data, attempts, next_attempt_ms, lease_until_ms, expires_at_ms)
		 VALUES ('garbage', ?, 0, 0, 0, ?)`, "not json", now.Add(PendingRetention).UnixMilli()); err != nil {
		t.Fatalf("insert garbage: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO schedule_pending (id, data, attempts, next_attempt_ms, lease_until_ms, expires_at_ms)
		 VALUES ('incomplete', jsonb('{"app":"x"}'), 0, 0, 0, ?)`, now.Add(PendingRetention).UnixMilli()); err != nil {
		t.Fatalf("insert incomplete: %v", err)
	}

	claimed, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed = %d, want 3 (valid + two corrupt surfaced)", len(claimed))
	}
	byID := map[string]PendingOccurrence{}
	for _, p := range claimed {
		byID[p.ID] = p
	}
	if p, ok := byID["good"]; !ok || p.DecodeErr != nil || p.Handler != "jobs.good" {
		t.Fatalf("valid record = %+v (ok=%v), want a decodable occurrence", p, ok)
	}
	for _, id := range []string{"garbage", "incomplete"} {
		p, ok := byID[id]
		if !ok {
			t.Fatalf("corrupt record %q was skipped instead of surfaced for retention", id)
		}
		if p.DecodeErr == nil {
			t.Fatalf("corrupt record %q returned without DecodeErr (caller could publish the wrong occurrence)", id)
		}
		// The empty intent must never be mistaken for a real occurrence.
		if p.App != "" || p.Schedule != "" || p.Handler != "" || !p.ScheduledAt.IsZero() {
			t.Fatalf("corrupt record %q carries intent fields: %+v", id, p)
		}
	}
	// The corrupt rows are retained (still present).
	var n int
	if err := c.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schedule_pending WHERE id IN ('garbage','incomplete')`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("corrupt rows retained = %d, want 2 (never deleted on decode failure)", n)
	}
}

// TestPendingOccurrenceCorruptPayloadNeverDeletedByReschedule confirms the
// failure path is also safe for a corrupt row: it is surfaced with DecodeErr and
// keeps its scheduling state (attempts untouched until the caller reschedules),
// so a decode failure never mutates or deletes the durable row.
func TestPendingOccurrenceCorruptPayloadNeverDeletedByReschedule(t *testing.T) {
	ctx := context.Background()
	c := openTestState(t)
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	if _, err := c.db.ExecContext(ctx,
		`INSERT INTO schedule_pending (id, data, attempts, next_attempt_ms, lease_until_ms, expires_at_ms)
		 VALUES ('bad', ?, 3, 0, 0, ?)`, "corrupt", now.Add(PendingRetention).UnixMilli()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// A claim surfaces it with DecodeErr and does not reset its attempts.
	claimed, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].DecodeErr == nil {
		t.Fatalf("claim = %+v, want one surfaced corrupt record", claimed)
	}
	// The caller's reschedule (the safe policy) advances it, retaining the row.
	if err := c.ReschedulePendingOccurrence(ctx, "bad", now.Add(time.Minute)); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	var attempts int
	if err := c.db.QueryRowContext(ctx,
		`SELECT attempts FROM schedule_pending WHERE id = 'bad'`).Scan(&attempts); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attempts != 4 {
		t.Fatalf("attempts = %d, want 4 (reschedule increments, row retained)", attempts)
	}
}

// TestPendingOccurrenceWriteErrorIsNonFatal pins the state package's non-fatal
// contract: a closed DB surfaces an error from the outbox write rather than
// panicking, so the caller can log and continue.
func TestPendingOccurrenceWriteErrorIsNonFatal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	c, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c.SetLogger(slog.New(slog.NewTextHandler(&testutil.SyncBuffer{}, nil)))
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("x", time.Now())); err == nil {
		t.Fatal("save on a closed DB must return an error, not panic")
	}
	if err := c.DeletePendingOccurrence(ctx, "x"); err == nil {
		t.Fatal("delete on a closed DB must return an error, not panic")
	}
}

// ids extracts the IDs of claimed records for readable failures.
func ids(ps []PendingOccurrence) string {
	parts := make([]string, len(ps))
	for i, p := range ps {
		parts[i] = p.ID
	}
	return strings.Join(parts, ",")
}

// TestPendingOccurrenceConcurrentPersistIsUnique is requirement E's deterministic
// state test: multiple goroutines concurrently persist the SAME occurrence ID,
// split across TWO separately opened State handles to the same database file (so
// the uniqueness is enforced by the DB primary key across handles, not by a
// single in-process pool). A start barrier releases all writers at once, and the
// test asserts:
//   - every SavePendingOccurrence returns no error;
//   - exactly ONE call reports inserted=true;
//   - exactly one row exists, carrying the complete immutable intent;
//   - no SQLITE_BUSY / lock error surfaces.
//
// No sleeps: the writes are released by closing the barrier channel, and the
// result is fully joined via a WaitGroup.
func TestPendingOccurrenceConcurrentPersistIsUnique(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	// Two independent handles to the SAME file, modeling two processes/retriers
	// racing the same occurrence.
	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open c1: %v", err)
	}
	defer c1.Close()
	c2, err := Open(path)
	if err != nil {
		t.Fatalf("open c2: %v", err)
	}
	defer c2.Close()
	buf := &testutil.SyncBuffer{}
	c1.SetLogger(slog.New(slog.NewTextHandler(buf, nil)))
	c2.SetLogger(slog.New(slog.NewTextHandler(buf, nil)))

	const writers = 16
	// The occurrence persisted by every writer is identical: the same ID with
	// the same immutable intent. (The due instant is irrelevant to uniqueness,
	// but keeping it identical makes the "one valid intent" assertion exact.)
	occ := pendingOcc("dup", now)

	start := make(chan struct{})
	type result struct {
		handle   int
		inserted bool
		err      error
	}
	results := make(chan result, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := c1
			if i%2 == 1 {
				c = c2
			}
			<-start // release all writers together
			inserted, err := c.SavePendingOccurrence(ctx, occ)
			results <- result{handle: i, inserted: inserted, err: err}
		}(i)
	}
	close(start) // barrier: all writers proceed at once
	wg.Wait()
	close(results)

	insertedCount := 0
	for r := range results {
		if r.err != nil {
			t.Fatalf("SavePendingOccurrence (handle %d) returned error: %v", r.handle, r.err)
		}
		if r.inserted {
			insertedCount++
		}
	}
	if insertedCount != 1 {
		t.Fatalf("inserted=true count = %d, want exactly 1 (unique persistence under concurrency)", insertedCount)
	}

	// Exactly one row exists and carries the complete, immutable intent.
	var rows int
	if err := c1.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM schedule_pending WHERE id = ?`, occ.ID).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("schedule_pending rows for id = %d, want exactly 1", rows)
	}
	claimed, err := c1.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	got := claimed[0]
	if got.DecodeErr != nil {
		t.Fatalf("persisted row is not a valid intent: %v", got.DecodeErr)
	}
	if got.ID != occ.ID || got.App != occ.App || got.Schedule != occ.Schedule ||
		got.Handler != occ.Handler || !got.ScheduledAt.Equal(occ.ScheduledAt) {
		t.Fatalf("persisted intent = %+v, want the immutable occurrence %+v", got, occ)
	}
	if got.Attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (a losing writer must not rewind/advance it)", got.Attempts)
	}

	// Cross-handle race: no SQLite lock/busy error may surface.
	if s := buf.String(); strings.Contains(s, "locked") || strings.Contains(s, "SQLITE_BUSY") {
		t.Fatalf("SQLite lock/busy error surfaced across concurrent handles:\n%s", s)
	}
}

// TestPendingOccurrenceExpiryExactAndNotRefreshed pins the retention contract:
// the first insert stamps expires_at_ms = now + PendingRetention, a re-save of
// the same ID does NOT refresh it, and a reschedule does NOT change it.
func TestPendingOccurrenceExpiryExactAndNotRefreshed(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c := openTestState(t)
	c.nowFn = func() time.Time { return now }

	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("a", now)); err != nil {
		t.Fatalf("save: %v", err)
	}
	wantExpiry := now.Add(PendingRetention)

	var stored int64
	if err := c.db.QueryRowContext(ctx,
		`SELECT expires_at_ms FROM schedule_pending WHERE id = 'a'`).Scan(&stored); err != nil {
		t.Fatalf("read expiry: %v", err)
	}
	if stored != wantExpiry.UnixMilli() {
		t.Fatalf("expires_at_ms = %d, want %d (first insert + 7d)", stored, wantExpiry.UnixMilli())
	}

	// Re-saving under a LATER clock must not extend the deadline: the row exists,
	// so ON CONFLICT DO NOTHING leaves expires_at_ms untouched.
	c.nowFn = func() time.Time { return now.Add(3 * 24 * time.Hour) }
	inserted, err := c.SavePendingOccurrence(ctx, pendingOcc("a", now))
	if err != nil || inserted {
		t.Fatalf("re-save = (%v,%v), want (false,nil)", inserted, err)
	}
	if err := c.db.QueryRowContext(ctx,
		`SELECT expires_at_ms FROM schedule_pending WHERE id = 'a'`).Scan(&stored); err != nil {
		t.Fatalf("read expiry: %v", err)
	}
	if stored != wantExpiry.UnixMilli() {
		t.Fatalf("expires_at_ms after re-save = %d, want %d (never refreshed)", stored, wantExpiry.UnixMilli())
	}

	// A reschedule (the failure path) advances attempts/next_attempt only.
	if err := c.ReschedulePendingOccurrence(ctx, "a", now.Add(time.Minute)); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	if err := c.db.QueryRowContext(ctx,
		`SELECT expires_at_ms FROM schedule_pending WHERE id = 'a'`).Scan(&stored); err != nil {
		t.Fatalf("read expiry: %v", err)
	}
	if stored != wantExpiry.UnixMilli() {
		t.Fatalf("expires_at_ms after reschedule = %d, want %d (reschedule never changes expiry)", stored, wantExpiry.UnixMilli())
	}
}

// TestPendingOccurrenceExpiryBoundaryAndClaimExclusion pins the expiry boundary:
// a record is expired once now >= expires_at_ms (so at exactly the deadline it is
// expired, and one millisecond before it is not), and an expired record is never
// claimed. A fresh record is used per boundary so a prior claim's lease cannot
// mask the expiry decision.
func TestPendingOccurrenceExpiryBoundaryAndClaimExclusion(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c := openTestState(t)
	c.nowFn = func() time.Time { return now }
	save := func() {
		t.Helper()
		if _, err := c.SavePendingOccurrence(ctx, pendingOcc("a", now.Add(-time.Minute))); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	expiry := now.Add(PendingRetention)

	// One millisecond before the deadline the record is claimable.
	save()
	before := expiry.Add(-time.Millisecond)
	if claimed, err := c.ClaimPendingOccurrences(ctx, before, before.Add(time.Minute), 10); err != nil || len(claimed) != 1 {
		t.Fatalf("claim before expiry = (%v,%v), want the record", ids(claimed), err)
	}
	// Recreate the record unleased, then claim at exactly the deadline: it is
	// expired and must not be claimed.
	if err := c.DeletePendingOccurrence(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	save()
	at := expiry
	claimed, err := c.ClaimPendingOccurrences(ctx, at, at.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim at expiry: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claim at expiry = %v, want none (expired)", ids(claimed))
	}
	// NextPendingDue also excludes the expired record.
	if _, ok, err := c.NextPendingDue(ctx, at); err != nil || ok {
		t.Fatalf("NextPendingDue with only an expired row = (ok %v, err %v), want (false,nil)", ok, err)
	}
}

// TestExpirePendingOccurrencesBoundedAndExcludesLive pins the bounded cleanup: it
// deletes only records whose retention has passed, is limited to `limit` per
// call, and leaves unexpired records untouched.
func TestExpirePendingOccurrencesBoundedAndExcludesLive(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	c := openTestState(t)
	// Three already-expired rows and one live row. The expired rows are inserted
	// with an explicit already-past expiry.
	for _, id := range []string{"e1", "e2", "e3"} {
		row := pendingOcc(id, now.Add(-time.Hour))
		if _, err := c.SavePendingOccurrence(ctx, row); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}
	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("live", now.Add(-time.Hour))); err != nil {
		t.Fatalf("save live: %v", err)
	}
	// Force the three to be already expired (the insert stamped +7d).
	if _, err := c.db.ExecContext(ctx,
		`UPDATE schedule_pending SET expires_at_ms = ? WHERE id IN ('e1','e2','e3')`,
		now.Add(-time.Millisecond).UnixMilli()); err != nil {
		t.Fatalf("expire rows: %v", err)
	}

	n, err := c.ExpirePendingOccurrences(ctx, now, 2)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 2 {
		t.Fatalf("first expire = %d, want 2 (bounded by limit)", n)
	}
	n, err = c.ExpirePendingOccurrences(ctx, now, 2)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 1 {
		t.Fatalf("second expire = %d, want 1 (remaining expired)", n)
	}
	n, err = c.ExpirePendingOccurrences(ctx, now, 2)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	if n != 0 {
		t.Fatalf("third expire = %d, want 0 (only live remains)", n)
	}
	var live int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schedule_pending WHERE id = 'live'`).Scan(&live); err != nil {
		t.Fatalf("count live: %v", err)
	}
	if live != 1 {
		t.Fatalf("live rows = %d, want 1 (unexpired record never removed)", live)
	}
}

// TestPendingOccurrenceExpirySurvivesReopen pins the crash/restart contract: a
// record inside its retention window is still claimable after reopening the same
// file, and its deadline is preserved; a record whose deadline passed while the
// process was down is excluded from claims and removed by cleanup after reopen.
func TestPendingOccurrenceExpirySurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db.sqlite3")
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	c1, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c1.nowFn = func() time.Time { return now }
	if _, err := c1.SavePendingOccurrence(ctx, pendingOcc("inside", now.Add(-time.Minute))); err != nil {
		t.Fatalf("save inside: %v", err)
	}
	if _, err := c1.SavePendingOccurrence(ctx, pendingOcc("outside", now.Add(-time.Minute))); err != nil {
		t.Fatalf("save outside: %v", err)
	}
	// "outside" expires just after the restart instant; "inside" stays live for
	// the whole 7-day window.
	if _, err := c1.db.ExecContext(ctx,
		`UPDATE schedule_pending SET expires_at_ms = ? WHERE id = 'outside'`,
		now.Add(2*time.Hour).UnixMilli()); err != nil {
		t.Fatalf("set outside expiry: %v", err)
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	c2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer c2.Close()
	// At now, "inside" is claimable and "outside" is not yet due-expired.
	claimed, err := c2.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim after reopen: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claim after reopen = %v, want both unexpired records", ids(claimed))
	}

	// Later, past "outside"'s deadline: it is unclaimable and cleaned; "inside"
	// remains.
	later := now.Add(3 * time.Hour)
	claimed, err = c2.ClaimPendingOccurrences(ctx, later, later.Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("claim later: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "inside" {
		t.Fatalf("claim later = %v, want only the still-live record", ids(claimed))
	}
	n, err := c2.ExpirePendingOccurrences(ctx, later, 10)
	if err != nil || n != 1 {
		t.Fatalf("expire after reopen = (%d,%v), want (1,nil)", n, err)
	}
}
