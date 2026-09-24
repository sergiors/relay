package reconciler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"relay/internal/metrics"
	"relay/internal/runner"
	"relay/internal/state"
)

// discovered -> the state DB records a ready row with handlers.
func TestReconcileStateDiscoverSuccess(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "brand-new")

	b := &fakeBuilder{}
	r, reg, st := newTestStateReconciler(t, root, b, nil, nil)

	r.reconcileFunction("brand-new")

	if pf := reg.GetByName("brand-new"); pf == nil || pf.Prepared() == nil {
		t.Fatal("expected brand-new registered and prepared")
	}
	detail, ok := st.GetFunction("brand-new")
	if !ok {
		t.Fatal("expected state row for brand-new")
	}
	if detail.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileSuccess {
		t.Fatalf("last_reconcile_status = %s, want success", detail.LastReconcileStatus)
	}
	if len(detail.Handlers) != 1 {
		t.Fatalf("handlers = %d, want 1", len(detail.Handlers))
	}
}

// Build failure retains the prior active state fields but records failed.
func TestReconcileStateFailureKeepsActiveAndMarksFailed(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Change so a rebuild is attempted, then make it fail.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	// Seed an active version first (mirrors the startup success path).
	func() {
		b.fail = false
		r.reconcileFunction("flaky") // success -> ready row
	}()
	before, _ := st.GetFunction("flaky")

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileFunction("flaky") // fails -> retained + failed

	detail, ok := st.GetFunction("flaky")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready after failure", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed", detail.LastReconcileStatus)
	}
	if detail.Image != before.Image || detail.Fingerprint != before.Fingerprint {
		t.Fatalf("failed reconcile must preserve active image/fingerprint: got %q/%q want %q/%q",
			detail.Image, detail.Fingerprint, before.Image, before.Fingerprint)
	}
}

// Unchanged healthy function -> skipped periodic check persists NO reconcile
// outcome (RecordDiscovered seeds empty reconcile fields); the last meaningful
// reconcile (none yet) is untouched.
func TestReconcileStateUnchangedDoesNotRecordOutcome(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "stable")

	fn := initialFn("stable", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	r.reconcileFunction("stable") // unchanged -> skip

	detail, ok := st.GetFunction("stable")
	if !ok {
		t.Fatal("expected row")
	}
	if detail.LastReconcileStatus != "" {
		t.Fatalf("last_reconcile_status = %s, want empty (unchanged check must not record an outcome)", detail.LastReconcileStatus)
	}
	if detail.LastReconcileAt != "" {
		t.Fatalf("last_reconcile_at = %s, want empty (unchanged check must not record a timestamp)", detail.LastReconcileAt)
	}
}

// Removal deletes the state row and handlers.
func TestReconcileStateRemoved(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "tobe-removed")

	fn := initialFn("tobe-removed", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	r.reconcileFunction("tobe-removed")

	if _, ok := st.GetFunction("tobe-removed"); ok {
		t.Fatal("state row should be removed")
	}
}

// TestReconcileRemovalDeletesMetricsSeries wires a metrics.Registry through the
// RemoveFunction hook the worker builds (RemoveFunction: m.RemoveFunction(...) +
// image retirement), so a removed function's Prometheus series are dropped at
// reconciliation time while an unrelated function's series and the globals stay
// untouched. This is the reconciler-package view of the production hook wiring;
// internal/worker owns the actual closure.
func TestReconcileRemovalDeletesMetricsSeries(t *testing.T) {
	root := t.TempDir()
	victimDir := writeFnDir(t, root, "victim")
	bystanderDir := writeFnDir(t, root, "bystander")

	victim := initialFn("victim", victimDir)
	bystander := initialFn("bystander", bystanderDir)
	b := &fakeBuilder{}

	m := metrics.New()
	// Seed per-function series for both functions, mirroring prior activity.
	seedMetricsFunction(m, "victim")
	seedMetricsFunction(m, "bystander")
	// A global counter the removal must never touch.
	m.Inc(metrics.MetricEventsReceived)
	before := m.Snapshot()
	if !strings.Contains(before, "function_events_matched_total{function=victim}") {
		t.Fatalf("expected victim series before removal:\n%s", before)
	}
	if !strings.Contains(before, "function_events_matched_total{function=bystander}") {
		t.Fatalf("expected bystander series before removal:\n%s", before)
	}

	// Build the reconciler with the production-style RemoveFunction hook: delete
	// the function's metrics series, then retire its images (a no-op with the
	// fake builder). The runner registry and state wiring mirror production.
	r, _ := newTestReconciler(t, root, b, []*runner.PreparedFunction{victim, bystander}, func(cfg *Config) {
		cfg.RemoveFunction = func(name string) {
			m.RemoveFunction(name)
		}
	})

	if err := os.RemoveAll(victimDir); err != nil {
		t.Fatalf("remove victim: %v", err)
	}
	r.reconcileFunction("victim")

	got := m.Snapshot()
	// Victim's series are gone from every function-scoped vec.
	if strings.Contains(got, "function_events_matched_total{function=victim}") {
		t.Fatalf("victim series must be deleted on removal:\n%s", got)
	}
	if strings.Contains(got, "handler_invocations_total{function=victim,") {
		t.Fatalf("victim handler_invocations_total must be deleted:\n%s", got)
	}
	if strings.Contains(got, "function_build_seconds{function=victim}") {
		t.Fatalf("victim function_build_seconds must be deleted:\n%s", got)
	}
	// Bystander's series and the global counter survive.
	if !strings.Contains(got, "function_events_matched_total{function=bystander}") {
		t.Fatalf("bystander series must survive removal:\n%s", got)
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Fatalf("events_received_total = %d, want 1", got)
	}
}

// seedMetricsFunction increments every function-carrying vec for name on the
// given registry, so a function has a full set of series for the removal test.
func seedMetricsFunction(m *metrics.Registry, name string) {
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "success"}, {Name: "function", Value: name}, {Name: "handler", Value: "x"}})
	m.IncLabels(metrics.MetricHandlerInvocations, []metrics.Label{{Name: "outcome", Value: "failure"}, {Name: "function", Value: name}, {Name: "handler", Value: "x"}})
	m.IncLabels(metrics.MetricBuildFailures, []metrics.Label{{Name: "function", Value: name}})
	m.IncLabels(metrics.MetricFunctionEventsMatched, []metrics.Label{{Name: "function", Value: name}})
	m.IncLabels(metrics.MetricFunctionHandlerSuccess, []metrics.Label{{Name: "function", Value: name}})
	m.IncLabels(metrics.MetricFunctionHandlerFailure, []metrics.Label{{Name: "function", Value: name}})
	m.IncLabels(metrics.MetricFunctionRetries, []metrics.Label{{Name: "function", Value: name}})
	m.IncLabels(metrics.MetricFunctionDLQ, []metrics.Label{{Name: "function", Value: name}})
	m.ObserveDurationLabels(metrics.MetricHandlerDuration, []metrics.Label{{Name: "function", Value: name}, {Name: "handler", Value: "x"}}, time.Millisecond)
	m.ObserveDurationLabels(metrics.MetricFunctionBuild, []metrics.Label{{Name: "function", Value: name}}, time.Millisecond)
}

// Invalid template -> NO state write (no success/failure/skipped recorded).
func TestReconcileStateNoWriteOnInvalidTemplate(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("guarded")

	detail, ok := st.GetFunction("guarded")
	if !ok {
		t.Fatal("row should exist from initial seeding")
	}
	if detail.LastReconcileStatus != "" {
		t.Fatalf("invalid template must not write a reconcile outcome, got %q", detail.LastReconcileStatus)
	}
}

// TestReconcileStateRemovalCleansAllTables is the live fsnotify-path regression
// for FULL cleanup on removal: reconciling a function whose directory was deleted
// must remove its functions row, function_stats, and handlers while leaving an
// unrelated function and the global stats row untouched.
func TestReconcileStateRemovalCleansAllTables(t *testing.T) {
	root := t.TempDir()
	victimDir := writeFnDir(t, root, "victim")
	bystanderDir := writeFnDir(t, root, "bystander")

	victim := initialFn("victim", victimDir)
	bystander := initialFn("bystander", bystanderDir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{victim, bystander}, nil)

	// Simulate prior activity: per-function counters for both and a cumulative
	// global row.
	st.RecordFunctionStats(state.FunctionStats{Function: "victim", EventsMatchedTotal: 5, HandlerSuccessTotal: 3})
	st.RecordFunctionStats(state.FunctionStats{Function: "bystander", EventsMatchedTotal: 7, HandlerSuccessTotal: 4})
	wantGlobal := state.Stats{EventsMatchedTotal: 12, HandlerSuccessTotal: 7}
	st.RecordStats(wantGlobal)

	// Capture bystander's handler count before the removal for the untouched check.
	bystanderBefore, ok := st.GetFunction("bystander")
	if !ok {
		t.Fatal("expected bystander row before removal")
	}

	// Remove the victim's directory and reconcile: the live removal path.
	if err := os.RemoveAll(victimDir); err != nil {
		t.Fatalf("removeall victim: %v", err)
	}
	r.reconcileFunction("victim")

	// Victim's functions row and function_stats are gone.
	if _, ok := st.GetFunction("victim"); ok {
		t.Fatal("victim functions row must be removed")
	}
	if _, ok := st.FunctionStats("victim"); ok {
		t.Fatal("victim function_stats must be removed")
	}

	// Handlers must not be resurrected: re-create the dir, reconcile, and the
	// detail must carry only the fresh template's handlers (no stale ones).
	writeFnDir(t, root, "victim")
	r.reconcileFunction("victim")
	detail, ok := st.GetFunction("victim")
	if !ok {
		t.Fatal("expected victim re-discovered after re-creating dir")
	}
	if len(detail.Handlers) != 1 {
		t.Fatalf("victim handlers = %d, want 1 (fresh template only)", len(detail.Handlers))
	}

	// Bystander untouched: row, function_stats, and handler count unchanged.
	if _, ok := st.GetFunction("bystander"); !ok {
		t.Fatal("bystander functions row must survive")
	}
	bs, ok := st.FunctionStats("bystander")
	if !ok || bs.EventsMatchedTotal != 7 || bs.HandlerSuccessTotal != 4 {
		t.Fatalf("bystander function_stats = %+v, ok=%v; want events 7 success 4", bs, ok)
	}
	bystanderAfter, ok := st.GetFunction("bystander")
	if !ok {
		t.Fatal("expected bystander row after removal")
	}
	if len(bystanderAfter.Handlers) != len(bystanderBefore.Handlers) {
		t.Fatalf("bystander handler count changed: before=%d after=%d", len(bystanderBefore.Handlers), len(bystanderAfter.Handlers))
	}

	// Global stats row unchanged.
	gs, ok := st.Stats()
	if !ok {
		t.Fatal("expected global stats row")
	}
	if gs.EventsMatchedTotal != wantGlobal.EventsMatchedTotal || gs.HandlerSuccessTotal != wantGlobal.HandlerSuccessTotal {
		t.Fatalf("global stats changed by removal: %+v, want %+v", gs, wantGlobal)
	}
}

// TestReconcileStateFailedBuildDoesNotRemoveStats is a regression that a failed
// rebuild (not a removal) never deletes state or stats: the function row stays
// ready, its function_stats row keeps its original counters and updated_at, and
// no removal is recorded.
func TestReconcileStateFailedBuildDoesNotRemoveStats(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "flaky")

	fn := initialFn("flaky", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Change content so a rebuild is attempted, then seed an active version and
	// per-function stats, as production would after activity.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileFunction("flaky") // success -> ready row
	st.RecordFunctionStats(state.FunctionStats{Function: "flaky", EventsMatchedTotal: 5, HandlerSuccessTotal: 3})

	before, ok := st.FunctionStats("flaky")
	if !ok {
		t.Fatal("expected flaky stats before failed rebuild")
	}

	// Change content again so the next pass attempts a rebuild, then fail it.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v3'); }\n"), 0o644); err != nil {
		t.Fatalf("write v3: %v", err)
	}
	b.fail = true
	r.reconcileFunction("flaky") // fails -> retained + failed

	// Function row still exists, still ready, marked failed (not removed).
	detail, ok := st.GetFunction("flaky")
	if !ok {
		t.Fatal("flaky row must survive a failed rebuild")
	}
	if detail.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready after failure", detail.Status)
	}
	if detail.LastReconcileStatus != state.ReconcileFailed {
		t.Fatalf("last_reconcile_status = %s, want failed (no removal)", detail.LastReconcileStatus)
	}

	// function_stats row still exists with identical counters and updated_at.
	after, ok := st.FunctionStats("flaky")
	if !ok {
		t.Fatal("flaky function_stats must survive a failed rebuild")
	}
	if after != before {
		t.Fatalf("function_stats changed by failed rebuild: before=%+v after=%+v", before, after)
	}
}

// TestReconcileStateInvalidTemplateDoesNotRemove is a regression that a broken
// template (unparseable) must not be mistaken for a removal: the function row
// and its function_stats survive untouched.
func TestReconcileStateInvalidTemplateDoesNotRemove(t *testing.T) {
	root := t.TempDir()
	dir := writeFnDir(t, root, "guarded")

	fn := initialFn("guarded", dir)
	b := &fakeBuilder{}
	r, _, st := newTestStateReconciler(t, root, b, []*runner.PreparedFunction{fn}, nil)

	// Change content so a rebuild is attempted, then seed an active version and
	// per-function stats.
	if err := os.WriteFile(filepath.Join(dir, "index.js"), []byte("export function hi(e){ console.log('v2'); }\n"), 0o644); err != nil {
		t.Fatalf("write v2: %v", err)
	}
	b.fail = false
	r.reconcileFunction("guarded") // success -> ready row
	st.RecordFunctionStats(state.FunctionStats{Function: "guarded", EventsMatchedTotal: 9, HandlerSuccessTotal: 6})
	before, ok := st.FunctionStats("guarded")
	if !ok {
		t.Fatal("expected guarded stats before invalid template")
	}

	// Break the template; reconcile must retain the previous version, not remove.
	if err := os.WriteFile(filepath.Join(dir, "template.yaml"), []byte("runtime: python9.9\n"), 0o644); err != nil {
		t.Fatalf("write broken template: %v", err)
	}
	r.reconcileFunction("guarded")

	detail, ok := st.GetFunction("guarded")
	if !ok {
		t.Fatal("guarded row must survive an invalid template")
	}
	if detail.Status != state.StatusReady {
		t.Fatalf("status = %s, want ready (retained)", detail.Status)
	}

	after, ok := st.FunctionStats("guarded")
	if !ok {
		t.Fatal("guarded function_stats must survive an invalid template")
	}
	if after != before {
		t.Fatalf("function_stats changed by invalid template: before=%+v after=%+v", before, after)
	}
}
