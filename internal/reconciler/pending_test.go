package reconciler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// barrierBuilder is a Builder whose Prepare can be gated on a synchronization
// barrier: a test arms a gate, starts a reconcile, and — while the first
// Prepare call is blocked inside the gate — asserts what the registry exposes.
// It never sleeps and never touches Docker; releasing the gate lets the build
// complete. fail makes a released build fail.
type barrierBuilder struct {
	mu       sync.Mutex
	prepares int
	executes int
	fail     bool

	// entered is closed by the first Prepare call that reaches the armed gate;
	// release unblocks it. Both are nil when no gate is armed.
	entered chan struct{}
	release chan struct{}
}

// arm installs a fresh gate and returns its entered/release channels. The gate
// is consumed by the next Prepare call.
func (b *barrierBuilder) arm() (entered, release chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entered = make(chan struct{})
	b.release = make(chan struct{})
	return b.entered, b.release
}

// setFail toggles whether a released build fails.
func (b *barrierBuilder) setFail(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fail = v
}

func (b *barrierBuilder) preparesCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.prepares
}

func (b *barrierBuilder) executesCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.executes
}

func (b *barrierBuilder) Prepare(ctx context.Context, fn app.App) (*runtime.Prepared, error) {
	b.mu.Lock()
	b.prepares++
	fail := b.fail
	entered, release := b.entered, b.release
	b.entered, b.release = nil, nil
	b.mu.Unlock()

	if entered != nil {
		close(entered)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errBoom
	}
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (b *barrierBuilder) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.executes++
	return nil
}

// runReconcile runs r.reconcileApp(name) on a goroutine and returns a channel
// closed when it returns, so a test can assert registry state while a gated
// build is in flight.
func runReconcile(r *Reconciler, name string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.reconcileApp(name)
	}()
	return done
}

// waitGate waits for the builder's gate to be entered, failing the test on
// timeout.
func waitGate(t *testing.T, entered chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the build gate")
	}
}

// waitDone waits for a reconcile goroutine to return.
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the reconcile to finish")
	}
}

// templateWithEvents builds a minimal valid runtime template with the given
// `events:` body (already indented under the events key).
func templateWithEvents(body string) string {
	return "runtime: node24\nevents:\n" + body
}

// writeTemplateDirFor writes a directory under root with the given template and
// a source file, returning the directory.
func writeTemplateDirFor(t *testing.T, root, name, tmpl string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(tmpl), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){}\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	return dir
}

// initialFnTemplate builds a prepared app named name over dir with a template
// parsed from tmpl.
func initialFnTemplate(t *testing.T, name, dir, tmpl string) *runner.PreparedApp {
	t.Helper()
	return runner.NewPrepared(
		app.App{Name: name, Dir: dir, Template: mustParse(tmpl)},
		&runtime.Prepared{Name: name, Image: "img-" + name},
		&fakeBuilder{},
	)
}

// countingExec is a minimal runner.Executor that counts handler executions, so
// the discovery-window unit test can prove an active v1 invocation actually ran
// while v2 is being prepared. It never touches Docker or Redis.
type countingExec struct {
	mu    sync.Mutex
	calls int
}

func (e *countingExec) Execute(context.Context, *runtime.Prepared, string, []byte, []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	return nil
}

func (e *countingExec) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// eventHandlers lists an app's active event handlers in declaration order.
func eventHandlers(pf *runner.PreparedApp) []string {
	if pf == nil || pf.App().Template == nil {
		return nil
	}
	var out []string
	for _, r := range pf.App().Template.Events {
		out = append(out, r.Handler)
	}
	return out
}

// TestReconcileNewAppPendingDuringBlockedFirstBuild pins the new-app discovery
// window: while the FIRST build of a brand-new app is blocked, the registry must
// expose its desired rules as pending/unavailable (never as active), so a
// delivery matching those rules is classified matched-but-unavailable rather
// than ACKed as unmatched. After the build succeeds the app is published active
// and the pending entry is cleared atomically.
func TestReconcileNewAppPendingDuringBlockedFirstBuild(t *testing.T) {
	root := t.TempDir()
	tmpl := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	writeTemplateDirFor(t, root, "brand-new", tmpl)

	b := &barrierBuilder{}
	r, reg := newTestReconciler(t, root, b, nil, nil)

	entered, release := b.arm()
	done := runReconcile(r, "brand-new")
	waitGate(t, entered)

	// While the build is blocked: no ACTIVE entry, but the desired rules are
	// exposed as pending.
	if pf := reg.GetByName("brand-new"); pf != nil {
		t.Fatalf("brand-new must not be published active before its build succeeds, got %v", pf)
	}
	if !reg.HasPending("brand-new") {
		t.Fatal("brand-new must be exposed as pending while its first build is in flight")
	}
	if names := reg.PendingNames(); len(names) != 1 || names[0] != "brand-new" {
		t.Fatalf("PendingNames = %v, want [brand-new]", names)
	}

	close(release)
	waitDone(t, done)

	pf := reg.GetByName("brand-new")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("brand-new must be published active after a successful build")
	}
	if reg.HasPending("brand-new") {
		t.Fatal("a successful build must clear the pending entry")
	}
}

// TestReconcileNewAppFirstBuildFailureRetainsPending pins that a brand-new app
// whose first build FAILS keeps its pending desired rules (so matching events
// stay pending rather than being ACKed as unmatched), and that a later
// successful reconcile publishes it runnable and clears the pending entry.
func TestReconcileNewAppFirstBuildFailureRetainsPending(t *testing.T) {
	root := t.TempDir()
	tmpl := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	writeTemplateDirFor(t, root, "flaky", tmpl)

	b := &barrierBuilder{fail: true}
	r, reg := newTestReconciler(t, root, b, nil, nil)

	entered, release := b.arm()
	done := runReconcile(r, "flaky")
	waitGate(t, entered)
	if !reg.HasPending("flaky") {
		t.Fatal("pending must be exposed while the first build is in flight")
	}
	close(release)
	waitDone(t, done)

	// The build failed: no active entry, but the desired rules survive as
	// pending.
	if pf := reg.GetByName("flaky"); pf != nil {
		t.Fatalf("failed first build must not publish an active entry, got %v", pf)
	}
	if !reg.HasPending("flaky") {
		t.Fatal("a failed first build must retain the pending desired rules")
	}

	// A later successful reconcile publishes the app and clears pending.
	b.setFail(false)
	r.reconcileApp("flaky")
	if pf := reg.GetByName("flaky"); pf == nil || pf.Prepared() == nil {
		t.Fatal("recovered app must be published active")
	}
	if reg.HasPending("flaky") {
		t.Fatal("a successful build must clear the pending entry")
	}
}

// TestReconcileActiveAppV2BlockedKeepsV1ExecutablePendingV2Held pins the
// existing-app rebuild: while v2 is blocked, v1 stays active and executable,
// v2-only rules are pending, and a build failure retains v1 AND the v2 pending
// rules. A later success installs v2 and clears pending.
func TestReconcileActiveAppV2BlockedKeepsV1ExecutablePendingV2Held(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "alpha", v1)

	initial := initialFnTemplate(t, "alpha", dir, v1)
	b := &barrierBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)

	// Change to v2 (adds a deleted rule) AND the source so the fingerprint
	// changes and a rebuild is warranted.
	v2 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n  - handler: handler.deleted\n    pattern:\n      event_name: [deleted]\n")
	writeTemplateDirFor(t, root, "alpha", v2)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	entered, release := b.arm()
	done := runReconcile(r, "alpha")
	waitGate(t, entered)

	// v1 remains ACTIVE and executable (only the created rule).
	pf := reg.GetByName("alpha")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("v1 must remain active while v2 is being prepared")
	}
	if got := eventHandlers(pf); len(got) != 1 || got[0] != "handler.created" {
		t.Fatalf("active rules during v2 prepare = %v, want only [handler.created]", got)
	}
	// v2's desired rules are pending.
	if !reg.HasPending("alpha") {
		t.Fatal("v2 desired rules must be pending while v2 is being prepared")
	}

	close(release)
	waitDone(t, done)

	// Success installs v2 and clears pending.
	pf = reg.GetByName("alpha")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("alpha must be active after a successful v2 build")
	}
	if got := eventHandlers(pf); len(got) != 2 {
		t.Fatalf("active rules after v2 success = %v, want both created and deleted", got)
	}
	if reg.HasPending("alpha") {
		t.Fatal("a successful v2 build must clear the pending entry")
	}
}

// TestReconcileActiveAppV2FailureRetainsV1AndPending pins that a FAILED v2
// build retains v1 exactly as today AND keeps v2's desired-only rules pending so
// v2-only events hold rather than being ACKed as unmatched.
func TestReconcileActiveAppV2FailureRetainsV1AndPending(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "alpha", v1)

	initial := initialFnTemplate(t, "alpha", dir, v1)
	b := &barrierBuilder{fail: true}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)

	v2 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n  - handler: handler.deleted\n    pattern:\n      event_name: [deleted]\n")
	writeTemplateDirFor(t, root, "alpha", v2)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	entered, release := b.arm()
	done := runReconcile(r, "alpha")
	waitGate(t, entered)
	if !reg.HasPending("alpha") {
		t.Fatal("v2 desired rules must be pending while the rebuild is in flight")
	}
	close(release)
	waitDone(t, done)

	pf := reg.GetByName("alpha")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("v1 must be retained after a failed v2 build")
	}
	if got := eventHandlers(pf); len(got) != 1 || got[0] != "handler.created" {
		t.Fatalf("active rules after failed v2 = %v, want v1 [handler.created]", got)
	}
	if !reg.HasPending("alpha") {
		t.Fatal("v2 desired-only rules must be retained as pending after a failed build")
	}
}

// TestReconcileRuleAdditionPublishesNewRuleAsPending pins the concrete
// users.created -> users.created + users.deleted example: during preparation the
// old rule is live, the new-only rule is pending, and after success both rules
// are active and no pending state remains.
func TestReconcileRuleAdditionPublishesNewRuleAsPending(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "users", v1)

	initial := initialFnTemplate(t, "users", dir, v1)
	b := &barrierBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)

	v2 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n  - handler: handler.deleted\n    pattern:\n      event_name: [deleted]\n")
	writeTemplateDirFor(t, root, "users", v2)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	entered, release := b.arm()
	done := runReconcile(r, "users")
	waitGate(t, entered)

	if pf := reg.GetByName("users"); pf == nil || pf.Prepared() == nil {
		t.Fatal("the old generation must stay active during preparation")
	}
	if !reg.HasPending("users") {
		t.Fatal("the added rule must be pending during preparation")
	}
	close(release)
	waitDone(t, done)

	pf := reg.GetByName("users")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("users must be active after the successful rebuild")
	}
	if got := eventHandlers(pf); len(got) != 2 {
		t.Fatalf("active rules = %v, want created and deleted", got)
	}
	if reg.HasPending("users") {
		t.Fatal("no pending state may remain after a successful rebuild")
	}
}

// TestReconcileRuleRemovalClearsPending pins the removal semantics: a rebuild
// that removes a rule installs the new generation and clears pending, so the
// removed rule neither stays active nor keeps gating events through a stale
// pending entry.
func TestReconcileRuleRemovalClearsPending(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n  - handler: handler.deleted\n    pattern:\n      event_name: [deleted]\n")
	dir := writeTemplateDirFor(t, root, "shrink", v1)

	initial := initialFnTemplate(t, "shrink", dir, v1)
	b := &barrierBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)

	// Remove the deleted rule and change the source so a rebuild happens.
	v2 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	writeTemplateDirFor(t, root, "shrink", v2)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	r.reconcileApp("shrink")

	pf := reg.GetByName("shrink")
	if pf == nil || pf.Prepared() == nil {
		t.Fatal("shrink must be active after the successful rebuild")
	}
	if got := eventHandlers(pf); len(got) != 1 || got[0] != "handler.created" {
		t.Fatalf("active rules = %v, want only [handler.created]", got)
	}
	if reg.HasPending("shrink") {
		t.Fatal("a successful rebuild must clear pending (no permanent pending for a removed rule)")
	}
}

// TestReconcileRemovalClearsPending pins that removing an app clears any pending
// desired entry as part of removal, so a removed app leaves no pending state
// gating events.
func TestReconcileRemovalClearsPending(t *testing.T) {
	root := t.TempDir()
	tmpl := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "gone", tmpl)

	r, reg := newTestReconciler(t, root, &fakeBuilder{}, nil, nil)
	// Simulate a brand-new app mid-preparation: pending only, no active entry.
	reg.SetPending("gone", app.App{Name: "gone", Dir: dir, Template: mustParse(tmpl)})
	if !reg.HasPending("gone") {
		t.Fatal("precondition: pending must be set")
	}

	r.remove("gone")

	if reg.HasPending("gone") {
		t.Fatal("removal must clear the pending entry")
	}
	if reg.GetByName("gone") != nil {
		t.Fatal("removal must drop any active entry")
	}
}

// TestReconcileInvalidTemplateClearsStalePending pins that an invalid desired
// template clears a stale pending entry (no indefinitely pending removed rules)
// while retaining the active generation.
func TestReconcileInvalidTemplateClearsStalePending(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "guarded", v1)

	initial := initialFnTemplate(t, "guarded", dir, v1)
	r, reg := newTestReconciler(t, root, &fakeBuilder{}, []*runner.PreparedApp{initial}, nil)

	// A stale pending entry is present (e.g. from an interrupted earlier
	// prepare), then the template becomes invalid.
	reg.SetPending("guarded", initial.App())
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("break template: %v", err)
	}

	r.reconcileApp("guarded")

	if reg.HasPending("guarded") {
		t.Fatal("an invalid desired template must clear stale pending state")
	}
	if pf := reg.GetByName("guarded"); pf == nil {
		t.Fatal("an invalid template must retain the active generation")
	}
}

// TestReconcileUnchangedClearsPending pins the no-transition path: an unchanged
// available app clears any transient pending entry and performs no build.
func TestReconcileUnchangedClearsPending(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "stable", v1)

	initial := initialFnTemplate(t, "stable", dir, v1)
	b := &barrierBuilder{}
	r, reg := newTestReconciler(t, root, b, []*runner.PreparedApp{initial}, nil)
	reg.SetPending("stable", initial.App())

	r.reconcileApp("stable")

	if reg.HasPending("stable") {
		t.Fatal("an unchanged skip path must clear stale pending state")
	}
	if b.preparesCount() != 0 {
		t.Fatalf("unchanged app must not rebuild: prepares = %d, want 0", b.preparesCount())
	}
}

// TestReconcileUnavailablePlaceholderUpdatedInPlace pins the startup-unavailable
// path: an app registered as unavailable (no image) has its unavailable rules
// updated COHERENTLY to the current desired template on reconcile, replacing the
// stale placeholder rather than leaving a removed rule gating events. No pending
// entry is created — the active unavailable placeholder itself carries the
// desired rules.
func TestReconcileUnavailablePlaceholderUpdatedInPlace(t *testing.T) {
	root := t.TempDir()
	tmpl := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "recover", tmpl)

	// Startup build failed: the app is a registered unavailable placeholder
	// carrying its OLD (stale) rules.
	stale := app.App{Name: "recover", Dir: dir, Template: mustParse(templateWithEvents("  - handler: handler.removed\n    pattern:\n      event_name: [removed]\n"))}
	unavail := runner.NewUnavailable(stale)

	reg := &runner.Registry{}
	reg.Set([]*runner.PreparedApp{unavail})
	// The build keeps failing, so the app stays an unavailable placeholder whose
	// rules must be updated in place to the current desired template.
	r := New(Config{Root: root, Debounce: time.Millisecond, Interval: time.Hour}, reg, &fakeBuilder{fail: true}, testutil.DiscardLogger())
	seedCurrent(r, stale)

	r.reconcileApp("recover")

	pf := reg.GetByName("recover")
	if pf == nil || pf.Prepared() != nil {
		t.Fatalf("the placeholder must remain unavailable while its build fails, got %v", pf)
	}
	if got := eventHandlers(pf); len(got) != 1 || got[0] != "handler.created" {
		t.Fatalf("unavailable placeholder rules = %v, want the current desired [handler.created]", got)
	}
	if reg.HasPending("recover") {
		t.Fatal("an unavailable placeholder carries its desired rules itself; no separate pending entry is needed")
	}
}

// TestReconcileSharedRegistryV2BlockedV1ExecutableV2Held ties the reconciler's
// ACTUAL pending publication to the runner's delivery semantics by sharing ONE
// registry between them, exactly as the worker wires them (the reconciler
// publishes into the registry the runner's Handle matches against). While v2 is
// blocked in its build:
//
//   - an event matching the active v1 rule is delivered by the runner and
//     invokes v1's handler;
//   - a v2-only event is held as runner.ErrAppUnavailable (matched but not
//     runnable) rather than ACKed as unmatched;
//   - the active generation remains v1 (v2 is not partially installed).
//
// After the build succeeds the same registry serves v2 and clears pending. The
// runner's own tests cover the matching semantics in isolation; this pins that
// reconcileApp is what publishes the pending state they depend on.
func TestReconcileSharedRegistryV2BlockedV1ExecutableV2Held(t *testing.T) {
	root := t.TempDir()
	v1 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n")
	dir := writeTemplateDirFor(t, root, "alpha", v1)

	// ONE registry, created by the runner and handed to the reconciler.
	runWorker := runner.NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	reg := runWorker.Registry()

	exec := &countingExec{}
	activeV1 := runner.NewPrepared(
		app.App{Name: "alpha", Dir: dir, Template: mustParse(v1)},
		&runtime.Prepared{Name: "alpha", Image: "img-alpha"},
		exec,
	)
	reg.Set([]*runner.PreparedApp{activeV1})

	b := &barrierBuilder{}
	r := New(
		Config{Root: root, Debounce: 10 * time.Millisecond, Interval: time.Hour},
		reg,
		b,
		testutil.DiscardLogger(),
	)
	seedCurrent(r, activeV1.App())

	// v2 adds a deleted rule AND the source changes, so a rebuild is warranted.
	v2 := templateWithEvents("  - handler: handler.created\n    pattern:\n      event_name: [created]\n  - handler: handler.deleted\n    pattern:\n      event_name: [deleted]\n")
	writeTemplateDirFor(t, root, "alpha", v2)
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2 source: %v", err)
	}

	entered, release := b.arm()
	done := runReconcile(r, "alpha")
	waitGate(t, entered)

	// The reconciler published v2 as pending while v1 stays active.
	if !reg.HasPending("alpha") {
		t.Fatal("v2 desired rules must be pending while v2 is being prepared")
	}
	if got := eventHandlers(reg.GetByName("alpha")); len(got) != 1 || got[0] != "handler.created" {
		t.Fatalf("active rules during v2 prepare = %v, want only [handler.created]", got)
	}

	// The runner over the SAME registry executes the active v1 rule.
	if err := runWorker.Handle(context.Background(), "m1", map[string]any{"event_name": "created"}); err != nil {
		t.Fatalf("active v1 event error = %v, want nil (executes and ACKs)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("active v1 handler executions = %d, want 1", exec.count())
	}

	// A v2-only event is matched but unavailable: never executed, never ACKed.
	if err := runWorker.Handle(context.Background(), "m2", map[string]any{"event_name": "deleted"}); !errors.Is(err, runner.ErrAppUnavailable) {
		t.Fatalf("v2-only event error = %v, want runner.ErrAppUnavailable", err)
	}
	if exec.count() != 1 {
		t.Fatalf("v2-only event must not execute a handler: executions = %d, want 1", exec.count())
	}

	// Success installs v2 and clears pending in one atomic step.
	close(release)
	waitDone(t, done)
	if got := eventHandlers(reg.GetByName("alpha")); len(got) != 2 {
		t.Fatalf("active rules after v2 success = %v, want both created and deleted", got)
	}
	if reg.HasPending("alpha") {
		t.Fatal("a successful v2 build must clear the pending entry")
	}
}
