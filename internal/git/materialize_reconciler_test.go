package git

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// reconcileProbeBuilder reports the generation Prepare actually observed, so a
// test can wait on the real reconcile pump instead of polling a counter.
type reconcileProbeBuilder struct {
	observed chan string
}

func (b *reconcileProbeBuilder) Prepare(
	_ context.Context, fn app.App,
) (*runtime.Prepared, error) {
	generation := "old"
	if _, err := os.Stat(filepath.Join(fn.Dir, "new-only.txt")); err == nil {
		generation = "new"
	}
	select {
	case b.observed <- generation:
	default:
	}
	return &runtime.Prepared{
		Name:  fn.Name,
		Image: "image-" + generation,
	}, nil
}

func (b *reconcileProbeBuilder) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

// startMaterializeReconciler starts a real Reconciler/watch over apps with one
// already-active app seeded to its current fingerprint, mirroring the worker's
// startup wiring. It returns the registry and a done channel closed after all
// loops join; cleanup guarantees shutdown even on test failure.
func startMaterializeReconciler(
	t *testing.T,
	apps string,
	active app.App,
	builder *reconcileProbeBuilder,
	removeApp func(string),
) (*runner.Registry, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	fingerprint, err := app.FingerprintApp(active.Dir, active.Template)
	if err != nil {
		t.Fatalf("fingerprint active app: %v", err)
	}
	registry := &runner.Registry{}
	registry.Set([]*runner.PreparedApp{runner.NewPrepared(
		active,
		&runtime.Prepared{
			Name:        active.Name,
			Image:       "image-old",
			Fingerprint: fingerprint,
		},
		builder,
	)})
	r := reconciler.New(reconciler.Config{
		Root:      apps,
		Debounce:  5 * time.Millisecond,
		Interval:  time.Hour,
		RemoveApp: removeApp,
	}, registry, builder, testLog())
	r.Seed(active, fingerprint)

	ctx, cancel := context.WithCancel(context.Background())
	if err := r.PrepareWatch(ctx); err != nil {
		cancel()
		t.Fatalf("prepare watcher: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("reconciler did not stop")
		}
	})
	return registry, cancel, done
}

// TestReconcilerMaterializeUpdateDoesNotRemove exercises the real watcher and
// reconciliation lifecycle around a materialize update. The app starts active
// and seeded to its old fingerprint, the update is committed by materialize's
// atomic exchange while the watcher is installed, and the real reconcile pump
// must prepare the new generation without ever invoking RemoveApp.
func TestReconcilerMaterializeUpdateDoesNotRemove(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("new-alpha"))
	writeFile(t, filepath.Join(src, "alpha", "new-only.txt"), "new only\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		hardeningTemplate("old-alpha"))
	writeFile(t, filepath.Join(apps, "alpha", "old-only.txt"), "old only\n")

	oldApp, err := app.LoadSingle(apps, "alpha")
	if err != nil {
		t.Fatalf("load old app: %v", err)
	}
	builder := &reconcileProbeBuilder{observed: make(chan string, 4)}
	var removals atomic.Int32
	registry, cancel, done := startMaterializeReconciler(t, apps, oldApp,
		builder, func(string) { removals.Add(1) })

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("materialize update: %v", err)
	}
	if len(mat) != 1 || mat[0] != "alpha" || len(rem) != 0 {
		t.Fatalf("materialize = (%v, %v), want ([alpha], [])", mat, rem)
	}

	select {
	case generation := <-builder.observed:
		if generation != "new" {
			t.Fatalf("reconciler prepared %q generation, want new", generation)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher never drove a reconcile of the materialized update")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}

	if got := removals.Load(); got != 0 {
		t.Fatalf("RemoveApp calls after update = %d, want 0", got)
	}
	if pf := registry.GetByName("alpha"); pf == nil ||
		pf.Prepared() == nil || pf.Prepared().Image != "image-new" {
		t.Fatalf("registry alpha = %+v, want prepared image-new", pf)
	}
}

// TestReconcilerMaterializeSourceRemovalCallsRemoveApp pins the normal
// lifecycle beside the update case: when materialize removes an app because the
// source no longer contains it, the real watcher/reconciler path invokes
// RemoveApp for that name and drops the registry entry.
func TestReconcilerMaterializeSourceRemovalCallsRemoveApp(t *testing.T) {
	src := t.TempDir()
	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "gone", "template.yaml"),
		hardeningTemplate("gone"))
	writeFile(t, filepath.Join(apps, "gone", "old-only.txt"), "old only\n")

	oldApp, err := app.LoadSingle(apps, "gone")
	if err != nil {
		t.Fatalf("load old app: %v", err)
	}
	builder := &reconcileProbeBuilder{observed: make(chan string, 4)}
	removals := make(chan string, 4)
	registry, cancel, done := startMaterializeReconciler(t, apps, oldApp,
		builder, func(name string) { removals <- name })

	mat, rem, err := materialize(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("materialize source removal: %v", err)
	}
	if len(mat) != 0 || len(rem) != 1 || rem[0] != "gone" {
		t.Fatalf("materialize = (%v, %v), want ([], [gone])", mat, rem)
	}

	select {
	case name := <-removals:
		if name != "gone" {
			t.Fatalf("RemoveApp name = %q, want gone", name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher never drove the removal lifecycle")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}
	if pf := registry.GetByName("gone"); pf != nil {
		t.Fatalf("registry still holds removed app: %+v", pf)
	}
}
