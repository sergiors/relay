package reconciler

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/state"
	"relay/internal/testutil"
)

// seedCurrent fingerprints fn as it currently exists on disk and seeds the
// reconciler with that value, mirroring the worker's startup wiring (the
// fingerprint is computed once and supplied to Seed). Tests that need a
// deliberately stale seed use r.Seed directly.
func seedCurrent(r *Reconciler, fn app.App) {
	fp, _ := app.FingerprintApp(fn.Dir, fn.Template)
	r.Seed(fn, fp)
}

// newTestReconciler builds a reconciler over a fresh registry seeded from
// initial, with a tiny debounce and a long interval so tests drive reconciles
// explicitly. cfgHook (nil-safe) may mutate the Config to wire optional hooks
// (Retire, RemoveApp, Audit, UpdateSchedules, UpdateServices, RemoveServices,
// State). When cfg.State is set, each initial app is recorded as discovered
// first, mirroring production startup wiring so reconcile hooks always find an
// existing state row.
func newTestReconciler(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedApp,
	cfgHook func(*Config),
) (*Reconciler, *runner.Registry) {
	t.Helper()
	reg := &runner.Registry{}
	reg.Set(initial)
	cfg := Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour}
	if cfgHook != nil {
		cfgHook(&cfg)
	}
	r := New(cfg, reg, builder, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	for _, pf := range initial {
		seedCurrent(r, pf.App())
		if cfg.State != nil {
			// Seed the cross-worker fence from the SAME discovery write, never a
			// later read of apps.fencing_token (which could claim another writer's
			// generation).
			fencingToken, err := cfg.State.RecordDiscovered(context.Background(), pf.App())
			if err != nil {
				t.Fatalf("record initial app %q: %v", pf.App().Name, err)
			}
			r.SeedFencingToken(pf.App().Name, fencingToken)
		}
	}
	return r, reg
}

// recordSuccess gives name a usable persisted generation without reading the
// fencing token back from the database: the desired write returns the exact
// token and the success is conditioned on it. A test that then drives the
// reconciler through a fencing-token-guarded callback seeds this token with
// Reconciler.SeedFencingToken so the local fence matches the persisted row.
func recordSuccess(
	t *testing.T,
	st *state.State,
	name, image, fingerprint string,
	preparedAt time.Time,
	fn app.App,
) state.FencingToken {
	t.Helper()
	fencingToken, err := st.RecordDiscoveredWithFingerprint(context.Background(), fn, fingerprint)
	if err != nil {
		t.Fatalf("RecordDiscoveredWithFingerprint(%q): %v", name, err)
	}
	if fencingToken <= 0 {
		t.Fatalf("RecordDiscoveredWithFingerprint(%q) returned fencing token %d, want positive",
			name, fencingToken)
	}
	landed, err := st.RecordReconcileSuccess(context.Background(), name, image, fingerprint, preparedAt, fn, fencingToken)
	if err != nil {
		t.Fatalf("RecordReconcileSuccess(%q): %v", name, err)
	}
	if !landed {
		t.Fatalf("RecordReconcileSuccess(%q) did not persist with fencing token %d",
			name, fencingToken)
	}
	return fencingToken
}

// newTestStateReconciler is newTestReconciler with a temp state DB wired, and
// returns the state handle so tests can assert persisted rows.
func newTestStateReconciler(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedApp,
	cfgHook func(*Config, *state.State),
) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	st := testutil.NewState(t)
	r, reg := newTestReconciler(t, root, builder, initial, func(cfg *Config) {
		cfg.State = st
		if cfgHook != nil {
			cfgHook(cfg, st)
		}
	})
	return r, reg, st
}
