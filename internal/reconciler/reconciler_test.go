package reconciler

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runner"
	"relay/internal/runtime"
)

var errBoom = contextCanceledSentinel{}

type contextCanceledSentinel struct{}

func (contextCanceledSentinel) Error() string { return "boom" }

// Records Prepare calls and can be configured to fail or return a prepared
// handle, without touching Docker or Redis.
type fakeBuilder struct {
	mu      sync.Mutex
	prepCnt int
	fail    bool
}

func (f *fakeBuilder) Prepare(ctx context.Context, fn function.Function) (*runtime.Prepared, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepCnt++
	if f.fail {
		return nil, errBoom
	}
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (f *fakeBuilder) Execute(ctx context.Context, prepared *runtime.Prepared, handler string, _ []byte) error {
	return nil
}

func (f *fakeBuilder) prepares() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prepCnt
}

const template = "runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n      event_name: [INSERT]\n"

func writeFnDir(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(template), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	return dir
}

func initialFn(name, dir string) *runner.PreparedFunction {
	tmpl := mustParse(template)
	return runner.NewPrepared(
		function.Function{Name: name, Dir: dir, Template: tmpl},
		&runtime.Prepared{Name: name, Image: "img-" + name},
		&fakeBuilder{},
	)
}

func mustParse(s string) *function.Template {
	t, err := function.ParseTemplate([]byte(s))
	if err != nil {
		panic(err)
	}
	return t
}

func newTestReconciler(t *testing.T, root string, builder Builder, initial []*runner.PreparedFunction) (*Reconciler, *runner.Registry) {
	t.Helper()
	reg := &runner.Registry{}
	reg.Set(initial)
	r := New(Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour}, reg, builder, log.New(os.Stderr, "test: ", 0))
	for _, pf := range initial {
		r.Seed(pf.Function())
	}
	return r, reg
}

// Launches just the debounce pump (the consumer of the incoming queue) without
// the fsnotify watcher, so debounce semantics can be tested deterministically.
func (r *Reconciler) startDebounceForTest() {
	go r.pump()
}
