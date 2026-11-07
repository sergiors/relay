//go:build integration

package state

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Runtime image selection state — the startup-only runtime base-image pin table.
// A missing row means UNPINNED: the worker keeps its historical
// configured base-image behavior. These tests pin the read/compare-and-swap
// contract the worker's startup read and a future writer rely on against real
// PostgreSQL, including the schema's own CHECK/PK enforcement and durability
// across State close/reopen.

// openSelectionState is an ownerless handle on a fresh isolated schema with the
// clock pinned so SelectedAt assertions are deterministic. The clock is the
// single timestamp source every State write uses, as in production.
func openSelectionState(t *testing.T) *State {
	t.Helper()
	st := openTestState(t)
	base := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	st.nowFn = func() time.Time { return base }
	return st
}

func selectionFixture(runtimeID, digest string) RuntimeImageSelection {
	return RuntimeImageSelection{
		RuntimeID:   runtimeID,
		SourceImage: "python:3.14-slim",
		Digest:      digest,
	}
}

func validRuntimeDigest(seed byte) string {
	// A valid "sha256:<64 lowercase hex>" index digest derived from seed so the
	// tests can distinguish rows without inventing magic strings.
	const hexChars = "0123456789abcdef"
	d := "sha256:"
	for i := 0; i < 64; i++ {
		d += string(hexChars[int((seed+byte(i))%16)])
	}
	return d
}

// An unpinned runtime is a MISSING row: the startup read on a fresh database is
// an empty result, never an error.
func TestReadRuntimeImageSelectionsEmptyMeansUnpinned(t *testing.T) {
	st := openSelectionState(t)

	// When: the worker's startup read runs before any selection was written.
	selections, err := st.ReadRuntimeImageSelections(context.Background())

	// Then: no selections and no error — every runtime is unpinned.
	if err != nil {
		t.Fatalf("read on a fresh database: %v", err)
	}
	if len(selections) != 0 {
		t.Fatalf("fresh database returned %d selections, want 0 (every runtime unpinned)", len(selections))
	}
}

// A compare-and-swap against expected generation 0 inserts the FIRST row at
// generation 1 and stamps the injected clock's instant as SelectedAt.
func TestCASInsertsGenerationOneIntoUnpinnedRuntime(t *testing.T) {
	st := openSelectionState(t)
	st.nowFn = func() time.Time { return time.Date(2026, 11, 6, 12, 30, 0, 0, time.UTC) }

	// When: the runtime is unpinned and a writer selects a pin (expected 0 = absent).
	got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(1)), 0)

	// Then: the write wins at generation 1 with the injected clock's timestamp.
	if err != nil {
		t.Fatalf("insert at expected generation 0: %v", err)
	}
	if got.Generation != 1 {
		t.Fatalf("returned generation = %d, want 1 (first selection)", got.Generation)
	}
	if got.SelectedAt != "2026-11-06T12:30:00Z" {
		t.Fatalf("SelectedAt = %q, want the injected clock's RFC3339 instant", got.SelectedAt)
	}
	if got.RuntimeID != "python3.14" || got.SourceImage != "python:3.14-slim" {
		t.Fatalf("returned identity = %+v, want the written selection", got)
	}

	// And: the persisted row round-trips every field.
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("read back %d rows, want exactly the inserted one", len(rows))
	}
	if rows[0] != got {
		t.Fatalf("persisted row = %+v, want %+v (field-for-field)", rows[0], got)
	}
}

// A writer racing against an existing row with expected generation 0 loses its
// swap: the row is already pinned, and the loss is reported as the distinct
// ErrRuntimeImageSelectionConflict, not a database error.
func TestCASAtExpectedZeroConflictsWhenRuntimeAlreadyPinned(t *testing.T) {
	st := openSelectionState(t)
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(1)), 0); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}

	// When: another writer also treats the runtime as unpinned.
	got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(2)), 0)

	// Then: the swap conflicts and the original pin is untouched.
	if !errors.Is(err, ErrRuntimeImageSelectionConflict) {
		t.Fatalf("second insert at expected 0 = (%+v, %v), want ErrRuntimeImageSelectionConflict", got, err)
	}
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back after conflict: (%d rows, %v), want the unmodified row", len(rows), err)
	}
	if rows[0].Generation != 1 || rows[0].Digest != validRuntimeDigest(1) {
		t.Fatalf("conflicting writer mutated the pinned row: %+v", rows[0])
	}
}

// A positive expected generation updates the row only when it still matches,
// advancing generation by one and replacing source/digest/selected_at in place.
func TestCASUpdatesExpectedGenerationAndAdvancesByOne(t *testing.T) {
	st := openSelectionState(t)
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(1)), 0); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}
	st.nowFn = func() time.Time { return time.Date(2026, 11, 6, 13, 45, 0, 0, time.UTC) }

	// When: the writer swaps the current generation 1 to a new pin.
	got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.15-slim", Digest: validRuntimeDigest(9)}, 1)

	// Then: the row advances to generation 2 and carries the new values.
	if err != nil {
		t.Fatalf("update at expected generation 1: %v", err)
	}
	if got.Generation != 2 {
		t.Fatalf("returned generation = %d, want 2", got.Generation)
	}
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back: (%d rows, %v)", len(rows), err)
	}
	if rows[0] != got {
		t.Fatalf("persisted row = %+v, want %+v", rows[0], got)
	}
	if rows[0].SelectedAt != "2026-11-06T13:45:00Z" {
		t.Fatalf("SelectedAt after update = %q, want the new write instant", rows[0].SelectedAt)
	}
}

// A stale writer whose expected generation no longer matches loses the swap
// without touching the newer pin.
func TestCASWithStaleGenerationConflictsWithoutMutatingRow(t *testing.T) {
	st := openSelectionState(t)
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(1)), 0); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}
	// Advance the row to generation 2 behind the stale writer's back.
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(2)), 1); err != nil {
		t.Fatalf("fixture advance: %v", err)
	}

	// When: a writer still expecting generation 1 arrives.
	got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(3)), 1)

	// Then: the swap conflicts and the generation-2 row is unchanged.
	if !errors.Is(err, ErrRuntimeImageSelectionConflict) {
		t.Fatalf("stale swap = (%+v, %v), want ErrRuntimeImageSelectionConflict", got, err)
	}
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatalf("read back: (%d rows, %v)", len(rows), err)
	}
	if rows[0].Generation != 2 || rows[0].Digest != validRuntimeDigest(2) {
		t.Fatalf("stale writer mutated the row: %+v", rows[0])
	}
}

// The persisted shape is validated BEFORE any database work: only non-empty
// identifiers and a "sha256:<64 lowercase hex>" digest are accepted, and a
// rejected selection leaves no row behind.
func TestCASRejectsInvalidSelectionShapeWithoutPersisting(t *testing.T) {
	st := openSelectionState(t)
	valid := validRuntimeDigest(4)

	cases := []struct {
		name      string
		selection RuntimeImageSelection
		expected  int64
	}{
		{"empty runtime ID", RuntimeImageSelection{SourceImage: "python:3.14-slim", Digest: valid}, 0},
		{"empty source image", RuntimeImageSelection{RuntimeID: "python3.14", Digest: valid}, 0},
		{"digest without sha256: prefix", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: valid[len("sha256:"):]}, 0},
		{"short digest", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: "sha256:abcd"}, 0},
		{"uppercase digest", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: "sha256:" + strings.ToUpper(valid[len("sha256:"):])}, 0},
		{"non-hex digest", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: valid[:len(valid)-1] + "g"}, 0},
		{"empty digest", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim"}, 0},
		{"negative expected generation", RuntimeImageSelection{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: valid}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), tc.selection, tc.expected)
			if err == nil {
				t.Fatalf("accepted invalid selection: %+v", got)
			}
			if errors.Is(err, ErrRuntimeImageSelectionConflict) {
				t.Fatalf("a shape rejection must not be reported as a swap conflict: %v", err)
			}
		})
	}

	// Then: no row was ever inserted by the rejected calls.
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(rows) != 0 {
		t.Fatalf("rejected selections persisted %d rows (%v), want 0", len(rows), err)
	}
}

// The table itself is the last line of defense: its CHECK constraints reject a
// digest outside the persisted index-digest shape and a non-positive
// generation, and the runtime_id primary key rejects a second row for one
// runtime.
func TestRuntimeImageSelectionsTableEnforcesShape(t *testing.T) {
	st := openSelectionState(t)
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.14", validRuntimeDigest(5)), 0); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}

	cases := []struct {
		name string
		dml  string
	}{
		{"uppercase digest", `INSERT INTO runtime_image_selections
			(runtime_id, source_image, digest, generation, selected_at)
			VALUES ('nodejs24.x', 'node:24-alpine', '` + "sha256:" + strings.ToUpper(validRuntimeDigest(6)[len("sha256"):]) + `', 1, 'now')`},
		{"zero generation", `INSERT INTO runtime_image_selections
			(runtime_id, source_image, digest, generation, selected_at)
			VALUES ('nodejs24.x', 'node:24-alpine', '` + validRuntimeDigest(6) + `', 0, 'now')`},
		{"duplicate runtime ID", `INSERT INTO runtime_image_selections
			(runtime_id, source_image, digest, generation, selected_at)
			VALUES ('python3.14', 'python:3.14-slim', '` + validRuntimeDigest(7) + `', 1, 'now')`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := st.db.Exec(tc.dml); err == nil {
				t.Fatal("the table accepted a row violating its constraints")
			}
		})
	}
}

// The startup read survives the handle lifecycle: selections persisted by one
// State are still there, field-for-field and ordered by runtime ID, after the
// handle closes and a new State opens on the same database.
func TestRuntimeImageSelectionsReadSurvivesCloseAndReopen(t *testing.T) {
	dsn := newTestDSN(t)
	st := testOpenOrFail(t, dsn)
	st.nowFn = func() time.Time { return time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC) }
	// Insert deliberately out of runtime-ID order: the read must sort.
	for _, sel := range []RuntimeImageSelection{
		{RuntimeID: "python3.14", SourceImage: "python:3.14-slim", Digest: validRuntimeDigest(1)},
		{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine", Digest: validRuntimeDigest(2)},
	} {
		if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), sel, 0); err != nil {
			t.Fatalf("insert %s: %v", sel.RuntimeID, err)
		}
	}
	st.Close()

	// When: a fresh State (a worker restart) reads the selections.
	reopened, err := testOpen(t, dsn)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	rows, err := reopened.ReadRuntimeImageSelections(context.Background())

	// Then: both pins survive with their values, in runtime-ID order.
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read after reopen returned %d rows, want 2", len(rows))
	}
	if rows[0].RuntimeID != "nodejs24.x" || rows[1].RuntimeID != "python3.14" {
		t.Fatalf("selections = %q..%q, want runtime-ID order", rows[0].RuntimeID, rows[1].RuntimeID)
	}
	if rows[0].SourceImage != "node:24-alpine" || rows[0].Digest != validRuntimeDigest(2) || rows[0].Generation != 1 {
		t.Fatalf("node selection did not round-trip: %+v", rows[0])
	}
	if rows[1].SourceImage != "python:3.14-slim" || rows[1].Digest != validRuntimeDigest(1) || rows[1].Generation != 1 {
		t.Fatalf("python selection did not round-trip: %+v", rows[1])
	}
}

// Concurrent writers can never both win the same expected generation: out of N
// writers treating the runtime as unpinned exactly one inserts generation 1,
// and out of N concurrent swaps of that generation exactly one advances it.
func TestConcurrentCASAllowsExactlyOneWinnerPerGeneration(t *testing.T) {
	st := openSelectionState(t)
	ctx := context.Background()

	const writers = 8
	raceInsert := func(expected int64) (wins int, conflicts int, generation int64) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				sel := selectionFixture("python3.14", validRuntimeDigest(byte(i+1)))
				got, err := st.CompareAndSwapRuntimeImageSelection(ctx, sel, expected)
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					wins++
					generation = got.Generation
				case errors.Is(err, ErrRuntimeImageSelectionConflict):
					conflicts++
				default:
					t.Errorf("unexpected database failure: %v", err)
				}
			}(i)
		}
		wg.Wait()
		return wins, conflicts, generation
	}

	// When: 8 writers race the absent-row insert (expected 0).
	wins, conflicts, generation := raceInsert(0)
	// Then: exactly one inserts, the others conflict, and the row is at
	// generation 1.
	if wins != 1 || conflicts != writers-1 {
		t.Fatalf("insert race: %d wins / %d conflicts, want 1 / %d", wins, conflicts, writers-1)
	}
	if generation != 1 {
		t.Fatalf("race winner generation = %d, want 1", generation)
	}

	// When: 8 writers race the swap of generation 1.
	wins, conflicts, generation = raceInsert(1)
	if wins != 1 || conflicts != writers-1 {
		t.Fatalf("swap race: %d wins / %d conflicts, want 1 / %d", wins, conflicts, writers-1)
	}
	if generation != 2 {
		t.Fatalf("race winner generation = %d, want 2", generation)
	}

	// Then: the final row is at generation 2.
	rows, err := st.ReadRuntimeImageSelections(ctx)
	if err != nil || len(rows) != 1 || rows[0].Generation != 2 {
		t.Fatalf("final row = (%d rows, %v), want one row at generation 2", len(rows), err)
	}
}

// A selection read must not mutate persisted rows (no implicit write, generation
// bump, or timestamp change).
func TestReadRuntimeImageSelectionsNeverWrites(t *testing.T) {
	st := openSelectionState(t)
	ctx := context.Background()
	inserted, err := st.CompareAndSwapRuntimeImageSelection(ctx,
		selectionFixture("python3.14", validRuntimeDigest(3)), 0)
	if err != nil {
		t.Fatalf("fixture insert: %v", err)
	}

	// When: the startup read runs repeatedly (a worker may restart several times).
	for range 3 {
		if _, err := st.ReadRuntimeImageSelections(ctx); err != nil {
			t.Fatalf("read: %v", err)
		}
	}

	// Then: the row is byte-for-byte the one the CAS wrote.
	rows, err := st.ReadRuntimeImageSelections(ctx)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if len(rows) != 1 || rows[0] != inserted {
		t.Fatalf("readings mutated the selection: want %+v, got %+v", inserted, rows)
	}
}

// Publication checks use the same lock as CAS for both a missing row (the
// unpinned identity) and a present row. The callback receives the authoritative
// value while that lock is held; absence is not confused with a read failure.
func TestWithRuntimeImageSelectionLockReportsAbsentAndCurrentSelection(t *testing.T) {
	st := openSelectionState(t)
	ctx := context.Background()
	called := false
	if err := st.WithRuntimeImageSelectionLock(ctx, "python3.14", func(_ context.Context, current *RuntimeImageSelection) error {
		called = true
		if current != nil {
			t.Fatalf("absent runtime selection = %+v, want nil", current)
		}
		return nil
	}); err != nil {
		t.Fatalf("lock absent selection: %v", err)
	}
	if !called {
		t.Fatal("absent selection callback was not invoked")
	}

	written, err := st.CompareAndSwapRuntimeImageSelection(ctx,
		selectionFixture("python3.14", validRuntimeDigest(11)), 0)
	if err != nil {
		t.Fatalf("insert selection: %v", err)
	}
	called = false
	if err := st.WithRuntimeImageSelectionLock(ctx, "python3.14", func(_ context.Context, current *RuntimeImageSelection) error {
		called = true
		if current == nil || *current != written {
			t.Fatalf("locked selection = %+v, want persisted row %+v", current, written)
		}
		return nil
	}); err != nil {
		t.Fatalf("lock current selection: %v", err)
	}
	if !called {
		t.Fatal("current selection callback was not invoked")
	}
}

// The publication lock also fences a first pin write when no row exists. This
// test observes PostgreSQL's ungranted advisory lock rather than relying on a
// sleep: after the CAS is demonstrably waiting on the same key, releasing the
// publication callback lets it insert generation 1.
func TestRuntimeImageSelectionLockFencesCASForAbsentRow(t *testing.T) {
	st := openSelectionState(t)
	ctx := context.Background()
	const runtimeID = "python3.14"
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	publicationDone := make(chan error, 1)
	go func() {
		publicationDone <- st.WithRuntimeImageSelectionLock(ctx, runtimeID,
			func(_ context.Context, current *RuntimeImageSelection) error {
				if current != nil {
					return errors.New("expected absent selection")
				}
				close(callbackEntered)
				<-releaseCallback
				return nil
			})
	}()
	<-callbackEntered

	casStarted := make(chan struct{})
	casDone := make(chan error, 1)
	go func() {
		close(casStarted)
		_, err := st.CompareAndSwapRuntimeImageSelection(ctx,
			selectionFixture(runtimeID, validRuntimeDigest(12)), 0)
		casDone <- err
	}()
	<-casStarted

	key := uint32(runtimeSelectionLockKey(runtimeID))
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		err := st.db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_locks
				WHERE locktype = 'advisory' AND NOT granted
			  AND classid = 3 AND objid = $1 AND objsubid = 2
			)`, key).Scan(&waiting)
		if err != nil {
			close(releaseCallback)
			<-publicationDone
			<-casDone
			t.Fatalf("inspect advisory lock wait: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			close(releaseCallback)
			<-publicationDone
			<-casDone
			t.Fatal("CAS never waited on the publication's advisory lock")
		}
		runtime.Gosched()
	}
	select {
	case err := <-casDone:
		close(releaseCallback)
		<-publicationDone
		t.Fatalf("CAS completed while publication held the selection lock: %v", err)
	default:
	}

	close(releaseCallback)
	if err := <-publicationDone; err != nil {
		t.Fatalf("publication lock callback: %v", err)
	}
	if err := <-casDone; err != nil {
		t.Fatalf("CAS after publication released lock: %v", err)
	}
	rows, err := st.ReadRuntimeImageSelections(ctx)
	if err != nil || len(rows) != 1 || rows[0].Generation != 1 {
		t.Fatalf("CAS result = (%+v, %v), want one generation-1 row", rows, err)
	}
}

// The database schema init is idempotent: re-running it on an existing database
// does not disturb the runtime_image_selections table (no error, rows kept).
func TestRuntimeImageSelectionsSchemaInitIsIdempotentWithRows(t *testing.T) {
	dsn := newTestDSN(t)
	st := testOpenOrFail(t, dsn)
	st.nowFn = func() time.Time { return time.Date(2026, 11, 6, 9, 0, 0, 0, time.UTC) }
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(),
		selectionFixture("python3.15", validRuntimeDigest(8)), 0); err != nil {
		t.Fatalf("fixture insert: %v", err)
	}
	if err := st.initSchema(context.Background()); err != nil {
		t.Fatalf("idempotent initSchema: %v", err)
	}
	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil {
		t.Fatalf("read after re-init: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("re-init disturbed the table: %d rows, want 1", len(rows))
	}
}
