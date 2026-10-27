package reconciler

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
)

// skipWithoutPosixPermissions guards tests that inject an operational
// filesystem failure with permissions: they only define behavior where read
// permission is actually enforced (a non-root unix user).
func skipWithoutPosixPermissions(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("permission-based failure injection is not portable on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as uid 0: read permission is not enforced")
	}
}

// seedCurrent fingerprints fn as it currently exists on disk and seeds the
// reconciler with that value, mirroring the worker's startup wiring (the
// fingerprint is computed once and supplied to Seed). Tests that need a
// deliberately stale seed use r.Seed directly.
func seedCurrent(r *Reconciler, fn app.App) {
	fp, _ := app.FingerprintApp(fn.Dir, fn.Template)
	r.Seed(fn, fp)
}

// launchSkeletonPump starts the pump goroutine for a skeleton reconciler whose
// production Start never ran, mirroring Start's loop lifecycle: the wait-group
// counter is registered BEFORE the launch (each loop defers loops.Done, so an
// unregistered manual launch can never balance it) and cleanup repeats Start's
// teardown ordering — close done first, stop any still-pending debounce timers,
// then join the pump. Use it at most once per Reconciler, and never on one
// whose Start already ran or whose done is otherwise closed
// (shutdownSkeletonReconciler owns that exact teardown for pump-less tests).
func launchSkeletonPump(t *testing.T, r *Reconciler) {
	t.Helper()
	r.loops.Add(1)
	go r.pump()
	t.Cleanup(func() {
		close(r.done)
		// A still-pending debounce timer must not fire a stale dispatch after
		// the pump was joined; Start stops them between closing done and the
		// join, so the skeleton teardown keeps that ordering.
		r.mu.Lock()
		for name, timer := range r.timers {
			timer.Stop()
			delete(r.timers, name)
		}
		r.mu.Unlock()
		r.loops.Wait()
	})
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
