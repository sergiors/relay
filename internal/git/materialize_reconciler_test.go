package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

// nestedContentProbeBuilder reports the content of one nested file in the tree
// Prepare actually observed. It makes the recursive-watch regression observable:
// a reconcile that cannot see the newly installed nested subtree reports
// "missing", and a later edit under that subtree only produces a second
// observation if its directory was watched.
type nestedContentProbeBuilder struct {
	probe    string
	observed chan string
}

func (b *nestedContentProbeBuilder) Prepare(
	_ context.Context, fn app.App,
) (*runtime.Prepared, error) {
	content := "missing"
	if data, err := os.ReadFile(filepath.Join(fn.Dir, b.probe)); err == nil {
		content = strings.TrimSpace(string(data))
	}
	select {
	case b.observed <- content:
	default:
	}
	return &runtime.Prepared{Name: fn.Name, Image: "image-" + content}, nil
}

func (b *nestedContentProbeBuilder) Execute(
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
	builder reconciler.Builder,
	removeApp func(string),
) (*runner.Registry, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startMaterializeReconcilerWithDebounce(t, apps, active, builder,
		removeApp, 5*time.Millisecond)
}

// startMaterializeReconcilerWithDebounce is startMaterializeReconciler with an
// explicit debounce, for tests that must let an exchange's event storm settle
// before acting on the newly installed tree.
func startMaterializeReconcilerWithDebounce(
	t *testing.T,
	apps string,
	active app.App,
	builder reconciler.Builder,
	removeApp func(string),
	debounce time.Duration,
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
		Debounce:  debounce,
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

// waitProbeObservation waits for the builder to observe want. Values from
// earlier/duplicate reconciles are ignored: an atomic exchange may emit several
// events for the same app, and the assertion is only that a reconcile observing
// the target content eventually runs.
func waitProbeObservation(t *testing.T, observed <-chan string, want string) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case got := <-observed:
			if got == want {
				return
			}
		case <-timeout:
			t.Fatalf("reconciler never observed nested content %q", want)
		}
	}
}

// TestReconcilerMaterializeAtomicExchangeWatchesNestedNewTree extends the real
// materialize+fsnotify+reconciler path to the recursive-watch regression after an
// atomic exchange. Live revision A has no "nested" subtree; revision B (materialized
// while the watcher is live) adds nested/deep/data.txt. The reconciler must
// reconcile revision B, then a write to that nested file in the NEWLY installed
// tree must produce a second reconcile that observes the edited content.
//
// The periodic ticker is one hour, so the second observation can only come from
// fsnotify having registered a recursive watch on the exchanged-in subtree,
// rather than from the backstop or a timing-only sleep.
func TestReconcilerMaterializeAtomicExchangeWatchesNestedNewTree(t *testing.T) {
	requireAtomicExchange(t)

	src := t.TempDir()
	writeFile(t, filepath.Join(src, "alpha", "template.yaml"),
		hardeningTemplate("revision-b"))
	writeFile(t, filepath.Join(src, "alpha", "nested", "deep", "data.txt"),
		"revision-b\n")

	apps := t.TempDir()
	writeFile(t, filepath.Join(apps, "alpha", "template.yaml"),
		hardeningTemplate("revision-a"))
	writeFile(t, filepath.Join(apps, "alpha", "old", "deep.txt"), "revision-a\n")

	oldApp, err := app.LoadSingle(apps, "alpha")
	if err != nil {
		t.Fatalf("load old app: %v", err)
	}
	builder := &nestedContentProbeBuilder{
		probe:    filepath.Join("nested", "deep", "data.txt"),
		observed: make(chan string, 64),
	}
	// A longer debounce lets any duplicate Create/Rename events from the exchange
	// settle (and the watch refresh finish) before the first reconcile fires.
	registry, cancel, done := startMaterializeReconcilerWithDebounce(t, apps,
		oldApp, builder, func(string) {}, 50*time.Millisecond)

	result, err := materializeApps(mustSelect(t, src, src), apps)
	if err != nil {
		t.Fatalf("materialize revision B: %v", err)
	}
	if len(result.materialized) != 1 || result.materialized[0] != "alpha" {
		t.Fatalf("materialized = %v, want [alpha]", result.materialized)
	}

	waitProbeObservation(t, builder.observed, "revision-b")

	nested := filepath.Join(apps, "alpha", "nested", "deep", "data.txt")
	if err := os.WriteFile(nested, []byte("nested-edit\n"), 0o644); err != nil {
		t.Fatalf("edit newly installed nested file: %v", err)
	}
	waitProbeObservation(t, builder.observed, "nested-edit")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not stop")
	}
	if pf := registry.GetByName("alpha"); pf == nil ||
		pf.Prepared() == nil || pf.Prepared().Image != "image-nested-edit" {
		t.Fatalf("registry alpha = %+v, want prepared image-nested-edit", pf)
	}
}
