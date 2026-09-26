package reconciler

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/state"
)

// seedCurrent fingerprints fn as it currently exists on disk and seeds the
// reconciler with that value, mirroring the worker's startup wiring (the
// fingerprint is computed once and supplied to Seed). Tests that need a
// deliberately stale seed use r.Seed directly.
func seedCurrent(r *Reconciler, fn function.Function) {
	fp, _ := function.FingerprintFunction(fn.Dir, fn.Template)
	r.Seed(fn, fp)
}

// newTestReconciler builds a reconciler over a fresh registry seeded from
// initial, with a tiny debounce and a long interval so tests drive reconciles
// explicitly. cfgHook (nil-safe) may mutate the Config to wire optional hooks
// (Retire, RemoveFunction, UpdateSchedules, UpdateServices, RemoveServices,
// State). When cfg.State is set, each initial function is recorded as discovered
// first, mirroring production startup wiring so reconcile hooks always find an
// existing state row.
func newTestReconciler(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedFunction,
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
		seedCurrent(r, pf.Function())
		if cfg.State != nil {
			cfg.State.RecordDiscovered(pf.Function())
		}
	}
	return r, reg
}

// newTestStateReconciler is newTestReconciler with a temp state DB wired, and
// returns the state handle so tests can assert persisted rows.
func newTestStateReconciler(
	t *testing.T,
	root string,
	builder Builder,
	initial []*runner.PreparedFunction,
	cfgHook func(*Config, *state.State),
) (*Reconciler, *runner.Registry, *state.State) {
	t.Helper()
	st, err := state.Open(filepath.Join(t.TempDir(), "db.sqlite3"))
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	r, reg := newTestReconciler(t, root, builder, initial, func(cfg *Config) {
		cfg.State = st
		if cfgHook != nil {
			cfgHook(cfg, st)
		}
	})
	return r, reg, st
}
