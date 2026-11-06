//go:build integration

package worker

import (
	"context"
	"strings"
	"testing"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/runtime"
	"relay/internal/state"
	"relay/internal/testutil"
)

// Worker startup reads persisted selection rows after opening the state handle
// and provides the immutable snapshot to the runtime Manager via
// openRuntimeManager. The worker does not notify, poll, or live-refresh
// selections, so opening the manager must not mutate persisted rows.

// workerSHA256Digest is a shape-valid "sha256:<64 lowercase hex>" index digest
// derived from c, good for fixtures but NOT resolvable — which is irrelevant
// here: construction never resolves a digest.
func workerSHA256Digest(c byte) string {
	const hexChars = "0123456789abcdef"
	d := "sha256:"
	for i := 0; i < 64; i++ {
		d += string(hexChars[int((c+byte(i))%16)])
	}
	return d
}

// openWorkerManagerWithSelections mirrors the worker's startup wiring: read the
// selections from the just-opened state handle, convert, and open the runtime
// manager through openRuntimeManager. The shutdown registry is run at cleanup,
// so the manager (and its Docker client) is always torn down.
func openWorkerManagerWithSelections(t *testing.T, st *state.State) (*runtime.Manager, error) {
	t.Helper()
	startupCtx := context.Background()
	selections, readErr := st.ReadRuntimeImageSelections(startupCtx)
	if readErr != nil {
		t.Fatalf("startup selection read: %v", readErr)
	}
	reg := &shutdownRegistry{}
	t.Cleanup(func() { reg.run(discardLogger()) })
	return openRuntimeManager(
		context.Background(), discardLogger(), metrics.New(),
		config.Config{NodeID: "test-selection-node"}, st, selections, reg,
	)
}

// No selected rows means the startup wiring is a pure pass-through: the manager
// opens exactly as it did before selections existed (unpinned behavior).
func TestIntegrationStartupSelectionsNoRowsOpensManagerUnpinned(t *testing.T) {
	st := testutil.NewState(t)

	mgr, err := openWorkerManagerWithSelections(t, st)
	if err != nil {
		t.Fatalf("openRuntimeManager with no selection rows: %v", err)
	}
	if mgr == nil {
		t.Fatal("openRuntimeManager returned a nil manager without an error")
	}
}

// A valid pinned row is provided to the manager and construction performs NO
// writes: generation, digest, source image, and selection time are byte-for-byte
// what was persisted — the read/provide-only startup contract.
func TestIntegrationStartupSelectionsProvideRowWithoutMutatingIt(t *testing.T) {
	st := testutil.NewState(t)
	selection := state.RuntimeImageSelection{
		RuntimeID:   "python3.14",
		SourceImage: "python:3.14-slim",
		Digest:      workerSHA256Digest(1),
	}
	got, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), selection, 0)
	if err != nil {
		t.Fatalf("seed selection row: %v", err)
	}
	before, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(before) != 1 || before[0] != got {
		t.Fatalf("seeded row = (%+v, %v), want exactly one row %+v", before, err, got)
	}

	// When: the worker's startup wiring opens the manager from that row.
	mgr, openErr := openWorkerManagerWithSelections(t, st)
	if openErr != nil {
		t.Fatalf("openRuntimeManager with a valid selection row: %v", openErr)
	}
	if mgr == nil {
		t.Fatal("openRuntimeManager returned a nil manager without an error (after selections load)")
	}

	// Then: the persisted row is untouched — no implicit write, no generation
	// bump, no timestamp change.
	after, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil {
		t.Fatalf("read back after open: %v", err)
	}
	if len(after) != 1 || after[0] != got {
		t.Fatalf("startup wiring mutated the selection row: want %+v, got %+v", got, after)
	}
}

// A row whose source image does not match the runtime's configured base image
// fails startup: the runtime layer validates the binding the state package
// deliberately does not, so a bad row can never produce a silently unpinned
// build from a moving tag.
func TestIntegrationStartupSelectionsRejectsSourceImageMismatch(t *testing.T) {
	st := testutil.NewState(t)
	selection := state.RuntimeImageSelection{
		RuntimeID:   "python3.14",
		SourceImage: "python:not-the-configured-base",
		Digest:      workerSHA256Digest(2),
	}
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), selection, 0); err != nil {
		t.Fatalf("seed mismatched selection row: %v", err)
	}

	mgr, err := openWorkerManagerWithSelections(t, st)
	if err == nil {
		_ = mgr.Close()
		t.Fatal("openRuntimeManager accepted a selection whose source image is not the configured base")
	}
	if !strings.Contains(err.Error(), "does not match configured base image") {
		t.Fatalf("openRuntimeManager error = %v, want the source-image binding failure", err)
	}
	if !strings.Contains(err.Error(), "runtime image selection") {
		t.Fatalf("openRuntimeManager error = %v, want the runtime image selection context", err)
	}

	// The row itself was still not written by the failed startup.
	after, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil || len(after) != 1 {
		t.Fatalf("read back after failed open: (%d rows, %v)", len(after), err)
	}
	if after[0].Generation != 1 || after[0].SourceImage != selection.SourceImage {
		t.Fatalf("failed startup mutated the row: %+v", after[0])
	}
}

// The read can never be confused with a write: ReadRuntimeImageSelections on a
// database a concurrent writer is using stays conflict-free and returns the
// current rows unchanged; selection reads never write state.
func TestIntegrationStartupSelectionsReadDoesNotConflictWithWriter(t *testing.T) {
	st := testutil.NewState(t)
	first := state.RuntimeImageSelection{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine",
		Digest: workerSHA256Digest(3)}
	if _, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), first, 0); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// A state writer advances the pin behind a startup read.
	if _, err := st.ReadRuntimeImageSelections(context.Background()); err != nil {
		t.Fatalf("startup read: %v", err)
	}
	second := state.RuntimeImageSelection{RuntimeID: "nodejs24.x", SourceImage: "node:24-alpine",
		Digest: workerSHA256Digest(4)}
	advanced, err := st.CompareAndSwapRuntimeImageSelection(context.Background(), second, 1)
	if err != nil {
		t.Fatalf("advance row: %v", err)
	}
	if advanced.Generation != 2 {
		t.Fatalf("advanced generation = %d, want 2", advanced.Generation)
	}

	rows, err := st.ReadRuntimeImageSelections(context.Background())
	if err != nil {
		t.Fatalf("startup read after the writer advanced: %v", err)
	}
	if len(rows) != 1 || rows[0].Generation != 2 || rows[0].Digest != second.Digest {
		t.Fatalf("startup read saw a stale row: %+v, want %+v", rows, advanced)
	}
}
