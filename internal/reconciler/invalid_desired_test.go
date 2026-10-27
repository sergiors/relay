//go:build integration

package reconciler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/runner"
	"relay/internal/source"
	"relay/internal/state"
)

// TestReconcileInvalidDesiredWritesFailureAndKeepsRegistry pins the live
// invalid-desired seam end to end: an app whose template becomes invalid is
// recorded as a failed desired state (degraded with its retained active
// generation) while the runtime registry keeps serving the previous version
// untouched and no rebuild is attempted. Removal is reserved for a genuinely
// missing directory.
func TestReconcileInvalidDesiredWritesFailureAndKeepsRegistry(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	// Seed a usable active generation (the registry already serves img-guarded;
	// the skip path deliberately records no outcome, so record it explicitly as
	// startup/reconcile would after a real success).
	fencingToken := recordSuccess(t, st, "guarded", "img-guarded", "fp-guarded", time.Now(), fn.App())
	r.SeedFencingToken("guarded", fencingToken)
	before, ok := st.GetApp("guarded")
	if !ok || before.Status != state.StatusReady {
		t.Fatalf("precondition: %+v ok=%v, want ready", before, ok)
	}

	// Break the template: the desired definition is present but invalid.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("guarded")

	after, ok := st.GetApp("guarded")
	if !ok {
		t.Fatal("guarded row must survive an invalid desired definition")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", after.Status)
	}
	if after.LastReconcileStatus != state.ReconcileFailed || after.LastError == "" {
		t.Fatalf("outcome = status=%q error=%q, want failed with an error", after.LastReconcileStatus, after.LastError)
	}
	if after.Image != before.Image || after.Fingerprint != before.Fingerprint {
		t.Fatalf("active generation = %q/%q, want preserved %q/%q",
			after.Image, after.Fingerprint, before.Image, before.Fingerprint)
	}
	if after.DesiredFingerprint != "" {
		t.Fatalf("desired_fingerprint = %q, want cleared for an invalid desired definition", after.DesiredFingerprint)
	}
	// The live registry is untouched and no rebuild was attempted: an invalid
	// template never replaces the previously-loaded version.
	if pf := reg.GetByName("guarded"); pf == nil || pf.Prepared() == nil {
		t.Fatal("invalid template must not modify the live registry")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (an invalid template never builds)", b.prepares())
	}
}

// TestReconcileOverDeepPatternInvalidRetainsActive pins that the fixed
// per-pattern depth cap is enforced when a reconcile reads an UPDATED template:
// an over-deep pattern is an invalid desired definition, so it follows the
// existing invalid-desired path — recorded failed/degraded with its active
// generation retained, the live registry untouched, and no rebuild attempted.
func TestReconcileOverDeepPatternInvalidRetainsActive(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "deep")

	fn := initialFn("deep", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	fencingToken := recordSuccess(t, st, "deep", "img-deep", "fp-deep", time.Now(), fn.App())
	r.SeedFencingToken("deep", fencingToken)
	before, ok := st.GetApp("deep")
	if !ok || before.Status != state.StatusReady {
		t.Fatalf("precondition: %+v ok=%v, want ready", before, ok)
	}

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte(overDeepPatternTemplate()), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("deep")

	after, ok := st.GetApp("deep")
	if !ok {
		t.Fatal("row must survive an over-deep desired definition")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", after.Status)
	}
	if after.LastReconcileStatus != state.ReconcileFailed || after.LastError == "" {
		t.Fatalf("outcome = %q/%q, want failed with an error", after.LastReconcileStatus, after.LastError)
	}
	if !strings.Contains(after.LastError, "depth") {
		t.Fatalf("last_error = %q, want it to mention depth", after.LastError)
	}
	if after.Image != before.Image || after.Fingerprint != before.Fingerprint {
		t.Fatalf("active generation changed: %q/%q -> %q/%q",
			before.Image, before.Fingerprint, after.Image, after.Fingerprint)
	}
	if pf := reg.GetByName("deep"); pf == nil || pf.Prepared() == nil {
		t.Fatal("an over-deep pattern must not modify the live registry")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (an over-deep pattern never builds)", b.prepares())
	}
}

// overDeepPatternTemplate builds a template whose single event pattern is one
// field level over app.MaxPatternDepth.
func overDeepPatternTemplate() string {
	var b strings.Builder
	b.WriteString("runtime: node24\nevents:\n  - handler: index.hi\n    pattern:\n")
	indent := "      "
	for i := 1; i <= app.MaxPatternDepth; i++ {
		fmt.Fprintf(&b, "%sf%d:\n", indent, i)
		indent += "  "
	}
	b.WriteString(indent + "leaf: [x]\n")
	return b.String()
}

// TestReconcileTemplateRemovalRemovesKnownApp pins that losing template.yaml —
// whether deleted or renamed away — means a previously-known app is no longer a
// candidate and follows the normal removal path: active and pending registry
// entries are dropped, the generation-fenced state row and app_stats are
// deleted, and the service/schedule hooks converge like any removal.
func TestReconcileTemplateRemovalRemovesKnownApp(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "known")

	fn := initialFn("known", dir)
	var events []string
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
		cfg.RemoveServices = func(name string) {
			events = append(events, "remove-services "+name)
		}
		cfg.RemoveApp = func(name string) {
			events = append(events, "remove-app "+name)
		}
		cfg.UpdateSchedules = func(name string, _ *app.Template) {
			events = append(events, "update-schedules "+name)
		}
	})

	// An in-preparation desired generation and prior activity must be cleaned up
	// by the same removal.
	reg.SetPending("known", fn.App())
	st.RecordAppStats(state.AppStats{App: "known", EventsMatchedTotal: 3})

	if err := os.Rename(filepath.Join(dir, "template.yaml"), filepath.Join(dir, "template.yaml.old")); err != nil {
		t.Fatalf("rename template: %v", err)
	}
	r.reconcileApp("known")

	if pf := reg.GetByName("known"); pf != nil {
		t.Fatal("an app that lost its template must leave the active registry")
	}
	if reg.HasPending("known") {
		t.Fatal("an app that lost its template must clear its pending desired entry")
	}
	if _, ok := st.GetApp("known"); ok {
		t.Fatal("a removed app's state row must be deleted")
	}
	if _, ok := st.AppStats("known"); ok {
		t.Fatal("a removed app's app_stats must be deleted")
	}
	want := []string{"remove-services known", "remove-app known"}
	if len(events) != len(want) {
		t.Fatalf("removal hooks = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("removal hooks = %v, want %v", events, want)
		}
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0 (removal never builds)", b.prepares())
	}
	r.mu.Lock()
	_, fencingTokenHeld := r.fencingTokens["known"]
	r.mu.Unlock()
	if fencingTokenHeld {
		t.Fatal("removal must forget the app's transition fencing token")
	}
}

// TestReconcileUnknownTemplateLessDirIsNoOp pins that a directory that was
// never an app cannot cause state or registry churn: reconciling it writes no
// state row, allocates no transition fencing token, and performs no build.
func TestReconcileUnknownTemplateLessDirIsNoOp(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "unrelated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir unrelated: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("notes"), 0o644); err != nil {
		t.Fatalf("write unrelated file: %v", err)
	}

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)

	r.reconcileApp("unrelated")

	if pf := reg.GetByName("unrelated"); pf != nil {
		t.Fatal("a never-app directory must not enter the registry")
	}
	if _, ok := st.GetApp("unrelated"); ok {
		t.Fatal("a never-app directory must not create a state row")
	}
	if _, ok := st.AppStats("unrelated"); ok {
		t.Fatal("a never-app directory must not create app_stats")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepare calls = %d, want 0", b.prepares())
	}
	r.mu.Lock()
	_, fencingTokenHeld := r.fencingTokens["unrelated"]
	r.mu.Unlock()
	if fencingTokenHeld {
		t.Fatal("a never-app directory must not allocate a transition fencing token")
	}
}

// TestReconcileAllRemovesFencingTokenOnlyTemplateLessApp pins the startup-only
// case: an app that only ever persisted an invalid desired row (a fencing token
// but no registry entry) is still swept once its directory loses template.yaml,
// so its stale state cannot outlive the definition.
func TestReconcileAllRemovesFencingTokenOnlyTemplateLessApp(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "startup-bad")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir startup-bad: %v", err)
	}

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)
	templatePath := filepath.Join(dir, "template.yaml")
	if err := os.WriteFile(templatePath, []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write invalid template: %v", err)
	}
	fencingToken, err := st.RecordInvalidDesired(context.Background(), "startup-bad", errBoom)
	if err != nil {
		t.Fatalf("record invalid desired: %v", err)
	}
	if fencingToken <= 0 {
		t.Fatalf("record invalid desired fencing token = %d, want positive", fencingToken)
	}
	r.SeedFencingToken("startup-bad", fencingToken)
	if err := os.Remove(templatePath); err != nil {
		t.Fatalf("remove template: %v", err)
	}

	launchSkeletonPump(t, r)
	r.reconcileAll()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := st.GetApp("startup-bad"); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := st.GetApp("startup-bad"); ok {
		t.Fatal("a fencing-token-only app must be removed when its template is absent")
	}
	if reg.GetByName("startup-bad") != nil {
		t.Fatal("a fencing-token-only app must not remain in the registry")
	}
	r.mu.Lock()
	_, fencingTokenHeld := r.fencingTokens["startup-bad"]
	r.mu.Unlock()
	if fencingTokenHeld {
		t.Fatal("removal must forget the fencing-token-only app's transition fencing token")
	}
}

// TestReconcileFingerprintFailureRecordsInvalidAndKeepsRegistry pins the
// fingerprint-failure branch: when the injected identity resolver errors for a
// still-present desired definition, the reconciler records the invalid/failed
// view through state (retaining the active generation) without modifying the
// live registry or its fingerprint, and without treating the app as
// removed.
func TestReconcileFingerprintFailureRecordsInvalidAndKeepsRegistry(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "unreadable")

	fn := initialFn("unreadable", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, func(cfg *Config, _ *state.State) {
		cfg.Fingerprint = func(string, *app.Template) (*source.Selection, string, error) {
			return nil, "", errBoom
		}
	})

	// Seed an active generation so the retained view is observable.
	fencingToken := recordSuccess(t, st, "unreadable", "img-active", "fp-active", time.Now(), fn.App())
	r.SeedFencingToken("unreadable", fencingToken)
	before, _ := st.GetApp("unreadable")

	r.reconcileApp("unreadable")

	if pf := reg.GetByName("unreadable"); pf == nil || pf.Prepared() == nil {
		t.Fatal("a fingerprint failure must not drop the loaded function")
	}
	if b.prepares() != 0 {
		t.Fatalf("prepares = %d, want 0 (a fingerprint failure never builds)", b.prepares())
	}
	after, ok := st.GetApp("unreadable")
	if !ok {
		t.Fatal("row must survive a fingerprint failure")
	}
	if after.Status != state.StatusDegraded {
		t.Fatalf("status = %q, want degraded (active generation retained)", after.Status)
	}
	if after.Image != before.Image || after.Fingerprint != before.Fingerprint {
		t.Fatalf("active generation = %q/%q, want preserved %q/%q",
			after.Image, after.Fingerprint, before.Image, before.Fingerprint)
	}
	if after.LastReconcileStatus != state.ReconcileFailed || after.LastError == "" {
		t.Fatalf("outcome = %q/%q, want failed with an error", after.LastReconcileStatus, after.LastError)
	}
}

// TestReconcileGenuinelyMissingDirStillPrunes pins that the invalid-desired
// recording is NOT applied to a genuinely absent directory: removal semantics
// (registry drop + row delete) still win.
func TestReconcileGenuinelyMissingDirStillPrunes(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "vanished")

	fn := initialFn("vanished", dir)
	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileApp("vanished")

	if reg.GetByName("vanished") != nil {
		t.Fatal("a vanished directory must be removed from the registry")
	}
	if _, ok := st.GetApp("vanished"); ok {
		t.Fatal("a vanished directory must have its state row pruned, not marked invalid")
	}
}

// TestReconcileInvalidThenRestoredIdenticalContentRecovers pins the recovery
// seam: an invalid desired definition forgets the reconciler's skip fingerprint,
// so when the definition is later restored byte-identically (which would
// otherwise compare equal and take the unchanged skip path that writes nothing),
// the next reconcile re-verifies it and a real success replaces the recorded
// failure. The active generation is not left falsely degraded.
func TestReconcileInvalidThenRestoredIdenticalContentRecovers(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "restored")

	fn := initialFn("restored", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	fencingToken := recordSuccess(t, st, "restored", "img-v1", "fp-v1", time.Now(), fn.App())
	r.SeedFencingToken("restored", fencingToken)
	original, err := os.ReadFile(filepath.Join(dir, "template.yaml"))
	if err != nil {
		t.Fatalf("read template: %v", err)
	}

	// Break, then restore the EXACT original content.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("restored")
	if got, _ := st.GetApp("restored"); got.Status != state.StatusDegraded {
		t.Fatalf("precondition: status = %q, want degraded", got.Status)
	}

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), original, 0o644); err != nil {
		t.Fatalf("restore template: %v", err)
	}
	r.reconcileApp("restored")

	got, ok := st.GetApp("restored")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if got.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after an identical restore re-verifies", got.Status)
	}
	if got.LastReconcileStatus != state.ReconcileSuccess || got.LastError != "" {
		t.Fatalf("outcome = %q/%q, want success/cleared", got.LastReconcileStatus, got.LastError)
	}
}

// TestReconcileValidReplacementOverwritesInvalidGeneration pins that a later
// valid, successful reconcile fully replaces the invalid view: the active
// generation is overwritten, the desired fingerprint converges to the success,
// and the failure is cleared.
func TestReconcileValidReplacementOverwritesInvalidGeneration(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "recovers")

	fn := initialFn("recovers", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedApp{fn}, nil)

	fencingToken := recordSuccess(t, st, "recovers", "img-v1", "fp-v1", time.Now(), fn.App())
	r.SeedFencingToken("recovers", fencingToken)
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileApp("recovers") // invalid -> degraded
	if got, _ := st.GetApp("recovers"); got.Status != state.StatusDegraded {
		t.Fatalf("precondition: status = %q, want degraded (active generation retained)", got.Status)
	}

	// Fix the template and change source so a rebuild succeeds.
	writeFnDir(t, root, "recovers")
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	r.reconcileApp("recovers")

	got, ok := st.GetApp("recovers")
	if !ok {
		t.Fatal("expected row after recovery")
	}
	if got.Status != state.StatusReady {
		t.Fatalf("status = %q, want ready after a valid successful replacement", got.Status)
	}
	if got.LastError != "" || got.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("outcome = error=%q status=%q, want cleared/success", got.LastError, got.LastReconcileStatus)
	}
	if got.DesiredFingerprint == "" || got.DesiredFingerprint != got.Fingerprint {
		t.Fatalf("desired/active = %q/%q, want converged non-empty", got.DesiredFingerprint, got.Fingerprint)
	}
}
