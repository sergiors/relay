//go:build integration

// This file exercises the PostgreSQL-server-specific concurrency guarantees that
// an in-memory fake cannot: schema initialization serialized across concurrent
// openers, row-level locking under real concurrent connections
// (FOR UPDATE SKIP LOCKED), claim fencing across a superseded lease, and
// idempotent concurrent insertion.
//
// It is excluded from the default suite by the integration build tag and
// REQUIRES a reachable PostgreSQL at TEST_DATABASE_URL (default localhost:5432
// with user/password/database relay, the address the CI service container
// exposes); point TEST_DATABASE_URL at any reachable server. A missing
// dependency FAILS the tests rather than skipping them, matching the
// Redis/Docker integration convention.
package state

import (
	"context"
	"sync"
	"testing"
	"time"
)

// requireTestDB fails immediately when PostgreSQL is not reachable, so the
// integration-tagged tests below never silently skip.
func requireTestDB(t *testing.T) {
	t.Helper()
	if !testDatabaseAvailable(t) {
		t.Fatalf("state integration test requires a reachable PostgreSQL at TEST_DATABASE_URL (%s)", testDatabaseURL())
	}
}

// TestIntegrationClaimSkipLockedDisjoint launches concurrent claim cycles
// against a real database and proves no due record is ever leased twice: the
// UPDATE ... FOR UPDATE SKIP LOCKED claims across two independent handles (and
// two goroutines each) partition the outbox without overlap.
func TestIntegrationClaimSkipLockedDisjoint(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	c1 := testOpenOrFail(t, dsn)
	c2 := testOpenOrFail(t, dsn)

	ctx := context.Background()
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
	const total = 40
	for i := 0; i < total; i++ {
		id := pendingID(i)
		if _, err := c1.SavePendingOccurrence(ctx, pendingOcc(id, now.Add(-time.Minute))); err != nil {
			t.Fatalf("save %s: %v", id, err)
		}
	}

	claims := func(c *State) []PendingOccurrence {
		var got []PendingOccurrence
		for {
			batch, err := c.ClaimPendingOccurrences(ctx, now, now.Add(time.Minute), 5)
			if err != nil {
				t.Errorf("claim: %v", err)
				return got
			}
			if len(batch) == 0 {
				return got
			}
			got = append(got, batch...)
		}
	}

	var (
		wg sync.WaitGroup
		mu sync.Mutex
		by = map[string]int{}
	)
	for _, c := range []*State{c1, c2, c1, c2} {
		wg.Add(1)
		go func(c *State) {
			defer wg.Done()
			for _, p := range claims(c) {
				mu.Lock()
				by[p.ID]++
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()

	if len(by) != total {
		t.Fatalf("distinct claimed rows = %d, want %d", len(by), total)
	}
	for id, n := range by {
		if n != 1 {
			t.Fatalf("row %s claimed %d times, want exactly once (no double lease)", id, n)
		}
	}
}

// TestIntegrationStaleClaimantIsFenced proves the claim_token fence: a claimant
// whose lease already expired and was re-claimed by a newer claimant can neither
// reschedule nor delete the row, so the newer claim's state is never clobbered.
func TestIntegrationStaleClaimantIsFenced(t *testing.T) {
	requireTestDB(t)
	c := testOpenOrFail(t, newTestDSN(t))
	ctx := context.Background()
	t0 := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	if _, err := c.SavePendingOccurrence(ctx, pendingOcc("fenced", t0.Add(-time.Minute))); err != nil {
		t.Fatalf("save: %v", err)
	}

	// First claimant: a short lease.
	first, err := c.ClaimPendingOccurrences(ctx, t0, t0.Add(time.Millisecond), 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim = (%v, %v), want one row", first, err)
	}
	if first[0].ClaimToken == 0 {
		t.Fatal("first claim must carry a nonzero fencing token")
	}

	// The lease expires; a newer claimant takes over with a fresh token.
	later := t0.Add(time.Second)
	second, err := c.ClaimPendingOccurrences(ctx, later, later.Add(time.Minute), 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("second claim = (%v, %v), want one row", second, err)
	}
	if second[0].ClaimToken <= first[0].ClaimToken {
		t.Fatalf("second claim token = %d, want > first token %d", second[0].ClaimToken, first[0].ClaimToken)
	}

	// The superseded first claimant's writes are no-ops.
	if err := c.ReschedulePendingOccurrence(ctx, "fenced", first[0].ClaimToken, later.Add(time.Hour)); err != nil {
		t.Fatalf("stale reschedule: %v", err)
	}
	var (
		attempts int
		lease    int64
	)
	if err := c.db.QueryRowContext(ctx,
		`SELECT attempts, lease_until_ms FROM schedule_pending WHERE id = 'fenced'`).Scan(&attempts, &lease); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want 0 (stale reschedule must not apply)", attempts)
	}
	if lease != later.Add(time.Minute).UnixMilli() {
		t.Fatalf("lease = %d, want the newer claimant's lease (%d)", lease, later.Add(time.Minute).UnixMilli())
	}
	if deleted, err := c.DeletePendingOccurrenceClaimed(ctx, "fenced", first[0].ClaimToken); err != nil || deleted {
		t.Fatalf("stale delete = (%v, %v), want (false, nil)", deleted, err)
	}
	if _, ok, err := c.NextPendingDue(ctx, later); err != nil || !ok {
		t.Fatalf("row must still exist after a stale delete: ok=%v err=%v", ok, err)
	}

	// The current claimant's fenced delete resolves it.
	if deleted, err := c.DeletePendingOccurrenceClaimed(ctx, "fenced", second[0].ClaimToken); err != nil || !deleted {
		t.Fatalf("current delete = (%v, %v), want (true, nil)", deleted, err)
	}
	if _, ok, err := c.NextPendingDue(ctx, later); err != nil || ok {
		t.Fatalf("row must be gone after the current claimant deletes it: ok=%v err=%v", ok, err)
	}
}

// TestIntegrationConcurrentSaveIsIdempotent proves SavePendingOccurrence is
// unique under real concurrency: many goroutines racing the same occurrence ID
// insert exactly one row.
func TestIntegrationConcurrentSaveIsIdempotent(t *testing.T) {
	requireTestDB(t)
	c := testOpenOrFail(t, newTestDSN(t))
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	const racers = 16
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		insert int
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.SavePendingOccurrence(ctx, pendingOcc("race", now))
			if err != nil {
				t.Errorf("save: %v", err)
				return
			}
			if ok {
				mu.Lock()
				insert++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if insert != 1 {
		t.Fatalf("inserted = %d, want exactly 1 (unique under concurrency)", insert)
	}
	var rows int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schedule_pending WHERE id = 'race'`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
}

// TestIntegrationConcurrentSchemaInit proves the schema-init advisory lock under
// real concurrency: many handles opening the same FRESH database at once must all
// succeed. Without the transaction-scoped advisory lock, the concurrent
// CREATE TABLE IF NOT EXISTS statements race and can surface as a duplicate
// pg_type/relation error.
func TestIntegrationConcurrentSchemaInit(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)

	const openers = 8
	start := make(chan struct{})
	errs := make([]error, openers)
	var wg sync.WaitGroup
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			st, err := Open(context.Background(), Options{DSN: dsn})
			errs[i] = err
			if err == nil {
				_ = st.Close()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent open %d: %v", i, err)
		}
	}
}

// TestIntegrationOpenRejectsUnreachableDSN proves the required-database startup
// contract: Open returns a clear error (never a degraded handle) when the
// database is unreachable, and a working DSN succeeds.
func TestIntegrationOpenRejectsUnreachableDSN(t *testing.T) {
	requireTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := Open(ctx, Options{
		DSN:            "postgres://relay:relay@127.0.0.1:1/relay?sslmode=disable",
		ConnectTimeout: time.Second,
	})
	if err == nil {
		_ = st.Close()
		t.Fatal("Open must fail for an unreachable database")
	}
}

// TestIntegrationPerWorkerStatsOwnership proves two workers sharing one database
// never overwrite each other's stats: each owner has its own row, a worker-scoped
// read sees only its own, and the aggregate read sums the counters.
func TestIntegrationPerWorkerStatsOwnership(t *testing.T) {
	requireTestDB(t)
	dsn := newTestDSN(t)
	a := testOpenOrFail(t, dsn)
	b := testOpenOrFail(t, dsn)
	a.owner = "worker-a"
	b.owner = "worker-b"

	a.RecordStats(Stats{EventsMatchedTotal: 10})
	b.RecordStats(Stats{EventsMatchedTotal: 25})

	if got, ok := a.OwnerStats(); !ok || got.EventsMatchedTotal != 10 {
		t.Fatalf("worker-a own stats = (%+v, %v), want events 10", got, ok)
	}
	if got, ok := b.OwnerStats(); !ok || got.EventsMatchedTotal != 25 {
		t.Fatalf("worker-b own stats = (%+v, %v), want events 25", got, ok)
	}
	if agg, ok := a.Stats(); !ok || agg.EventsMatchedTotal != 35 {
		t.Fatalf("aggregate stats = (%+v, %v), want events 35 (10+25)", agg, ok)
	}
}

func pendingID(i int) string {
	return "occurrence-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
