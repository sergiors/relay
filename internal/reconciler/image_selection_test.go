package reconciler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/source"
)

// liveSelectionBuilder models the Manager's live-selection extensions while
// keeping preparation and publication observable without Docker.
type liveSelectionBuilder struct {
	mu          sync.Mutex
	selections  map[string]string
	refreshErr  error
	refreshes   int
	prepares    []string
	failPrepare bool
}

func (b *liveSelectionBuilder) Prepare(ctx context.Context, fn app.App) (*runtime.Prepared, error) {
	return b.PrepareWithFingerprintAndSelection(ctx, fn, "", nil)
}

func (b *liveSelectionBuilder) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn app.App,
	fingerprint string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepares = append(b.prepares, fn.Name)
	if b.failPrepare {
		return nil, errors.New("scripted build failure")
	}
	runtimeID := fn.Template.Runtime
	return &runtime.Prepared{
		Name: fn.Name, Image: "img-" + fn.Name + "-" + b.selections[runtimeID],
		Fingerprint: fingerprint, Runtime: runtimeID, ImageSelection: b.selections[runtimeID],
	}, nil
}

func (b *liveSelectionBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

func (b *liveSelectionBuilder) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refreshes++
	if b.refreshErr != nil {
		return nil, b.refreshErr
	}
	return nil, nil
}

func (b *liveSelectionBuilder) PublishPrepared(
	_ context.Context,
	prepared *runtime.Prepared,
	publish func() error,
) error {
	b.mu.Lock()
	want := b.selections[prepared.Runtime]
	b.mu.Unlock()
	if prepared.ImageSelection != want {
		return runtime.ErrRuntimeImageSelectionStale
	}
	return publish()
}

func (b *liveSelectionBuilder) RuntimeImageSelectionIdentity(runtimeID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.selections[runtimeID]
}

func (b *liveSelectionBuilder) setSelection(runtimeID, identity string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.selections[runtimeID] = identity
}

func (b *liveSelectionBuilder) prepareNames() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.prepares...)
}

func (b *liveSelectionBuilder) refreshCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.refreshes
}

func writeRuntimeApp(t *testing.T, root, name, runtimeID string) app.App {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir app: %v", err)
	}
	text := "runtime: " + runtimeID + "\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(text), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi() {}\n"), 0o644); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	tmpl, err := app.ParseTemplate([]byte(text))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return app.App{Name: name, Dir: dir, Template: tmpl}
}

func preparedApp(t *testing.T, fn app.App, builder Builder, selection string) *runner.PreparedApp {
	t.Helper()
	fingerprint, err := app.FingerprintApp(fn.Dir, fn.Template)
	if err != nil {
		t.Fatalf("fingerprint %s: %v", fn.Name, err)
	}
	return runner.NewPrepared(fn, &runtime.Prepared{
		Name: fn.Name, Image: "old-" + fn.Name, Fingerprint: fingerprint,
		Runtime: fn.Template.Runtime, ImageSelection: selection,
	}, builder)
}

func TestReconcileAllRefreshesOnceAndRepreparesOnlyChangedRuntime(t *testing.T) {
	root := t.TempDir()
	nodeApp := writeRuntimeApp(t, root, "a-node", "nodejs24.x")
	pythonApp := writeRuntimeApp(t, root, "z-python", "python3.14")
	nodeA, nodeB := "node:24-alpine@sha256:node-a", "node:24-alpine@sha256:node-b"
	pythonA := "python:3.14-slim@sha256:python-a"
	b := &liveSelectionBuilder{selections: map[string]string{
		"nodejs24.x": nodeA, "python3.14": pythonA,
	}}
	nodeInitial := preparedApp(t, nodeApp, b, nodeA)
	pythonInitial := preparedApp(t, pythonApp, b, pythonA)
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{nodeInitial, pythonInitial}, nil)
	r.SeedImageSelection("a-node", nodeA)
	r.SeedImageSelection("z-python", pythonA)
	b.setSelection("nodejs24.x", nodeB)

	// reconcileAll refreshes the full selection snapshot once, then dispatches
	// the discovered candidates. Read the queued names and drive the same app
	// reconciler synchronously to avoid timer-based test coordination.
	r.reconcileAll()
	queued := map[string]bool{}
	for range 2 {
		select {
		case name := <-r.incoming:
			queued[name] = true
		default:
			t.Fatal("reconcileAll did not enqueue every app candidate")
		}
	}
	for name := range queued {
		r.reconcileApp(name)
	}

	if got := b.refreshCount(); got != 1 {
		t.Fatalf("selection refreshes in one pass = %d, want 1", got)
	}
	if got := b.prepareNames(); len(got) != 1 || got[0] != "a-node" {
		t.Fatalf("prepared apps = %v, want only the app on changed runtime", got)
	}
	if got := reg.GetByName("a-node"); got == nodeInitial || got.Prepared().ImageSelection != nodeB {
		t.Fatalf("changed runtime app was not replaced with digest B: %+v", got)
	}
	if got := reg.GetByName("z-python"); got != pythonInitial {
		t.Fatalf("unrelated runtime app changed: got %p, want original %p", got, pythonInitial)
	}
}

func TestReconcileRuntimeSelectionBuildFailureKeepsOldApp(t *testing.T) {
	root := t.TempDir()
	fn := writeRuntimeApp(t, root, "kept", "nodejs24.x")
	selectionA, selectionB := "node:24-alpine@sha256:old", "node:24-alpine@sha256:new"
	b := &liveSelectionBuilder{selections: map[string]string{"nodejs24.x": selectionA}, failPrepare: true}
	initial := preparedApp(t, fn, b, selectionA)
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)
	r.SeedImageSelection("kept", selectionA)
	b.setSelection("nodejs24.x", selectionB)

	r.reconcileApp("kept")

	if got := reg.GetByName("kept"); got != initial {
		t.Fatalf("failed selection rebuild replaced old app: got %p, want %p", got, initial)
	}
	if got := b.prepareNames(); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("preparations = %v, want one retry for changed selection", got)
	}
	r.mu.Lock()
	gotFingerprint := r.fingerprints["kept"]
	gotSelection := r.imageSelections["kept"]
	r.mu.Unlock()
	if gotFingerprint != initial.Prepared().Fingerprint || gotSelection != selectionA {
		t.Fatalf("failed build overwrote last-known-good identity: fingerprint=%q selection=%q", gotFingerprint, gotSelection)
	}
}

type stalePublisherBuilder struct {
	current      string
	prepared     string
	publishCalls int
	refreshCalls int
}

func (b *stalePublisherBuilder) Prepare(_ context.Context, fn app.App) (*runtime.Prepared, error) {
	return b.PrepareWithFingerprintAndSelection(context.Background(), fn, "", nil)
}

func (b *stalePublisherBuilder) PrepareWithFingerprintAndSelection(
	_ context.Context,
	fn app.App,
	fingerprint string,
	_ *source.Selection,
) (*runtime.Prepared, error) {
	return &runtime.Prepared{
		Name: fn.Name, Image: "img-stale-test-v1", Fingerprint: fingerprint,
		Runtime: fn.Template.Runtime, ImageSelection: b.prepared, // captured before the authoritative row advanced
	}, nil
}

func (b *stalePublisherBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	return nil
}

func (b *stalePublisherBuilder) RefreshRuntimeImageSelections(context.Context) ([]string, error) {
	b.refreshCalls++
	return nil, nil
}

func (b *stalePublisherBuilder) PublishPrepared(_ context.Context, prepared *runtime.Prepared, _ func() error) error {
	b.publishCalls++
	if prepared.ImageSelection != b.current {
		return runtime.ErrRuntimeImageSelectionStale
	}
	return errors.New("unexpected current selection")
}

func (b *stalePublisherBuilder) RuntimeImageSelectionIdentity(string) string { return b.current }

func TestReconcileStalePublicationKeepsActiveAndSkipsSideEffects(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stale-publish")
	initial := initialFn("stale-publish", dir)
	selectionA, selectionB := "node:24-alpine@old", "node:24-alpine@new"
	b := &stalePublisherBuilder{prepared: selectionA, current: selectionB}
	var scheduleCalls, serviceCalls int
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, func(cfg *Config) {
		cfg.UpdateSchedules = func(string, *app.Template) { scheduleCalls++ }
		cfg.UpdateServices = func(string, *app.Template, string) { serviceCalls++ }
	})
	shutdownSkeletonReconciler(t, r)
	r.SeedImageSelection("stale-publish", selectionA)

	r.reconcileApp("stale-publish")

	if got := reg.GetByName("stale-publish"); got != initial {
		t.Fatalf("stale publication replaced active app: got %p, want %p", got, initial)
	}
	if b.publishCalls != 1 || b.refreshCalls != 1 {
		t.Fatalf("publication/refresh calls = %d/%d, want one each", b.publishCalls, b.refreshCalls)
	}
	if scheduleCalls != 0 || serviceCalls != 0 {
		t.Fatalf("stale generation triggered schedule/service side effects: %d/%d", scheduleCalls, serviceCalls)
	}
}
