package reconciler

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/observability/metrics"
	"relay/internal/routing"
	"relay/internal/runtime"
	"relay/internal/testutil"
)

// TestServiceCoordinatorSupersededPassPreservesOldGeneration covers gap 2: a
// request B that has already started its replacement but not yet committed must
// notice that a newer desired state C was enqueued during its pass, and abandon
// the commit. Concretely B must stop neither A (the running old generation it
// was about to supersede) nor leave its own provisional replacement behind; it
// removes the provisional container it created and lets pending C converge.
//
// The ordering is deterministic: the fake parks B's StartService at the exact
// post-start/pre-commit boundary (startGate), C is enqueued while B is parked,
// and C is itself parked before its commit so the assertions observe the state
// between "B abandoned" and "C accepted".
func TestServiceCoordinatorSupersededPassPreservesOldGeneration(t *testing.T) {
	f := newFakeDocker()
	services := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	// base is the same image-source service with a distinct desired env, so A/B/C
	// are the SAME SourceRef identity and therefore the SAME replica slot: B and C
	// both replace A in place.
	base := func(env string) *app.Template {
		tmpl := serviceTemplate("", app.Service{Name: "ghcr.io/acme/api:1.2", Image: "ghcr.io/acme/api:1.2", Port: 8080, Replicas: 1})
		tmpl.Env = map[string]string{"MODE": env}
		return tmpl
	}

	// Phase 1: converge MODE=a, creating generation A.
	coordinator.Enqueue("fn", base("a"), "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait initial: %v", err)
	}
	a := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if a == nil {
		t.Fatal("no generation A was started")
	}
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	// Phase 2: B starts its replacement and parks before committing; C is
	// enqueued while B is parked, superseding it. The gate parks the FIRST call
	// (B) and the SECOND call (C) on separate channels so the test can observe
	// each boundary.
	bEntered := make(chan string, 1)
	bRelease := make(chan struct{})
	cEntered := make(chan string, 1)
	cRelease := make(chan struct{})
	var starts int
	f.mu.Lock()
	f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
		starts++
		switch starts {
		case 1:
			bEntered <- id
			<-bRelease
		case 2:
			cEntered <- id
			<-cRelease
		}
	}
	f.mu.Unlock()

	bComplete := make(chan error, 1)
	cComplete := make(chan error, 1)
	coordinator.EnqueueWithStatus("fn", base("b"), "", nil, nil, func(err error) { bComplete <- err })
	bID := <-bEntered

	coordinator.EnqueueWithStatus("fn", base("c"), "", nil, nil, func(err error) { cComplete <- err })

	// Phase 3: release B. It must detect the supersession at its commit boundary:
	// remove its own provisional replacement and keep A. Its worker then picks
	// pending C, whose replacement parks (cEntered) before committing.
	close(bRelease)
	<-cEntered

	// B is fully done and C has not committed yet. A must still be running, B's
	// provisional replacement must be gone, and B's stale completion must not have
	// fired (C owns status authority).
	if _, ok := f.containerByID(a.id); !ok {
		t.Fatal("generation A must remain running while C has not committed")
	}
	if _, ok := f.containerByID(bID); ok {
		t.Fatal("B's provisional replacement must be cleaned up on supersession")
	}
	select {
	case err := <-bComplete:
		t.Fatalf("superseded B completion must not fire (newer status authority), got %v", err)
	default:
	}

	// Phase 4: release C; it converges to the latest desired state and only then
	// replaces A (start-before-stop).
	close(cRelease)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait final: %v", err)
	}
	select {
	case err := <-cComplete:
		if err != nil {
			t.Fatalf("current C completion must fire with nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("current C completion did not fire")
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1.2"); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
	final := f.lastStartedFor("fn", "ghcr.io/acme/api:1.2")
	if final == nil || final.environmentFingerprint != runtime.EnvironmentFingerprint([]string{"MODE=c", "PORT=8080"}) {
		t.Fatalf("final = %+v, want the latest MODE=c effective env", final)
	}
	// A was preserved through B's abandoned pass and stopped only by C's commit.
	if si, ai := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+a.id); si < 0 || ai < 0 || si > ai {
		t.Fatalf("order = %v, want C's start before A's stop", f.order())
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// TestServiceCoordinatorSupersededPassPreservesRemovedSourceRef pins the
// supersession interaction with an identity change: while a pass converging a
// NEW SourceRef B is parked before commit, a newer desired state C (back to the
// original SourceRef A) arrives. B must abandon before it stops A's containers,
// which are "removed" under B's desired set, so A survives and C re-converges
// to it. This is the removal path of gap 1 combined with gap 2's authority
// guard.
func TestServiceCoordinatorSupersededPassPreservesRemovedSourceRef(t *testing.T) {
	f := newFakeDocker()
	services := NewServiceReconciler(f, nil, routing.TraefikConfig{}, testutil.DiscardLogger(), 0)
	coordinator := NewServiceCoordinator(services)
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	coordinator.Start(lifecycle)

	tmplA := serviceTemplate("", app.Service{Name: "ghcr.io/acme/api:1", Image: "ghcr.io/acme/api:1", Port: 80, Replicas: 1})
	tmplB := serviceTemplate("", app.Service{Name: "ghcr.io/acme/api:2", Image: "ghcr.io/acme/api:2", Port: 80, Replicas: 1})
	// C returns to A's SourceRef but with a changed env, so C has corrective work
	// (a replacement for the same slot) and can be parked before its commit.
	tmplC := serviceTemplate("", app.Service{Name: "ghcr.io/acme/api:1", Image: "ghcr.io/acme/api:1", Port: 80, Replicas: 1})
	tmplC.Env = map[string]string{"MODE": "c"}

	// Phase 1: converge generation A.
	coordinator.Enqueue("fn", tmplA, "", nil)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait initial: %v", err)
	}
	a := f.lastStartedFor("fn", "ghcr.io/acme/api:1")
	if a == nil {
		t.Fatal("no generation A was started")
	}
	f.mu.Lock()
	f.events = nil
	f.mu.Unlock()

	// Phase 2: B parks after starting its replacement; enqueue C (= A's
	// SourceRef, env c) while B is parked.
	bEntered := make(chan string, 1)
	bRelease := make(chan struct{})
	cEntered := make(chan string, 1)
	cRelease := make(chan struct{})
	var starts int
	f.mu.Lock()
	f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
		starts++
		switch starts {
		case 1:
			bEntered <- id
			<-bRelease
		case 2:
			cEntered <- id
			<-cRelease
		}
	}
	f.mu.Unlock()

	coordinator.EnqueueWithStatus("fn", tmplB, "", nil, nil, func(error) {})
	<-bEntered
	coordinator.EnqueueWithStatus("fn", tmplC, "", nil, nil, func(error) {})

	close(bRelease)
	<-cEntered

	// A's containers were "removed" under B's desired set, but B was superseded
	// before its removed-service cleanup: A must survive. C (which does desire
	// A's SourceRef) has started its replacement but not committed yet.
	if _, ok := f.containerByID(a.id); !ok {
		t.Fatal("A (removed under B) must be preserved while B is superseded and C has not committed")
	}

	close(cRelease)
	if err := coordinator.Wait(context.Background()); err != nil {
		t.Fatalf("wait final: %v", err)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:1"); got != 1 {
		t.Fatalf("A's SourceRef running = %d, want 1 after C converged", got)
	}
	if got := f.runningCount("fn", "ghcr.io/acme/api:2"); got != 0 {
		t.Fatalf("B running = %d, want 0", got)
	}
	// A was preserved through B's abandoned pass and stopped only by C's commit.
	if si, ai := f.indexOfEvent("start:0"), f.indexOfEvent("stop:"+a.id); si < 0 || ai < 0 || si > ai {
		t.Fatalf("order = %v, want C's start before A's stop", f.order())
	}

	cancel()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := coordinator.Join(joinCtx); err != nil {
		t.Fatalf("join: %v", err)
	}
}

// serviceLogRecord is the parsed shape of one captured JSON log line for
// summary assertions: the message and the outcome attribute when present.
type serviceLogRecord struct {
	Msg     string `json:"msg"`
	Outcome string `json:"outcome"`
	Stopped *int   `json:"stopped"`
}

// serviceLogRecords parses captured JSON log output into records, failing the
// test on any line that is not a structured record.
func serviceLogRecords(t *testing.T, raw string) []serviceLogRecord {
	t.Helper()
	var records []serviceLogRecord
	for _, line := range strings.Split(raw, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec serviceLogRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("captured log line is not JSON: %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

// summaryOutcomes returns the outcome attribute of every generic
// "Service: reconciled" summary line, in order. An empty entry means the
// summary was emitted WITHOUT an outcome attribute.
func summaryOutcomes(records []serviceLogRecord) []string {
	var outcomes []string
	for _, rec := range records {
		if rec.Msg == "Service: reconciled" {
			outcomes = append(outcomes, rec.Outcome)
		}
	}
	return outcomes
}

// hasLogRecord reports whether any record carries the exact message.
func hasLogRecord(records []serviceLogRecord, msg string) bool {
	for _, rec := range records {
		if rec.Msg == msg {
			return true
		}
	}
	return false
}

// newSupersessionSummaryHarness wires a ServiceReconciler over a fake Docker
// holding one running old generation the desired template has changed (a new
// app image), with a JSON log capture and an inspectable metrics registry.
func newSupersessionSummaryHarness(t *testing.T) (
	*fakeDocker, *app.Template, *ServiceReconciler, *metrics.Registry, *testutil.SyncBuffer,
) {
	t.Helper()
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	logs := &testutil.SyncBuffer{}
	m := metrics.New()
	c := NewServiceReconciler(
		f, nil, routing.TraefikConfig{},
		slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		testReconcileTimeout, WithMetrics(m),
	)
	return f, tmpl, c, m, logs
}

// changedMetric reads the service-reconcile outcome counter for "fn".
func changedMetric(m *metrics.Registry) int64 {
	return m.CounterLabels(metrics.MetricServiceReconciles, []metrics.Label{
		{Name: "app", Value: "fn"},
		{Name: "outcome", Value: metrics.ServiceOutcomeChanged},
	})
}

// TestServiceSupersededPassSummaryOmitsChangedOutcome pins the summary edge of
// a superseded pass: the pass issues corrective work (its replacement start
// sets changed, the existing metrics contract) but discovers the supersession
// at its commit boundary, removes its provisional container, and preserves the
// old generation. Such a pass must NOT be summarized as a generic
// outcome=changed convergence — the summary either carries the explicit
// superseded outcome or is suppressed entirely — while the explicit
// "pass superseded" lines and the changed metric stay as they were. The
// control subtest proves the generic summary is what an ordinary corrective
// pass reports, so the negative assertion is not vacuous.
func TestServiceSupersededPassSummaryOmitsChangedOutcome(t *testing.T) {
	t.Run("an ordinary corrective pass still reports outcome changed", func(t *testing.T) {
		// given a changed desired image over a running old generation, applied
		// directly with no supersession authority
		f, tmpl, c, m, logs := newSupersessionSummaryHarness(t)

		// when the pass converges the replacement
		if err := c.Apply(context.Background(), "fn", tmpl, "img-new", nil); err != nil {
			t.Fatalf("apply: %v", err)
		}

		// then exactly one summary reports the convergence as changed, and the
		// metric observes it
		outcomes := summaryOutcomes(serviceLogRecords(t, logs.String()))
		if len(outcomes) != 1 || outcomes[0] != "changed" {
			t.Fatalf("summary outcomes = %v, want [changed]", outcomes)
		}
		if got := changedMetric(m); got != 1 {
			t.Fatalf("changed metric = %d, want 1", got)
		}
		if got := f.runningCount("fn", "service.js"); got != 1 {
			t.Fatalf("running = %d, want 1 after convergence", got)
		}
	})

	t.Run("a pass superseded at its commit boundary is not summarized as changed", func(t *testing.T) {
		// given a pass whose authority holds while it starts its provisional
		// replacement and flips the moment the start returns — the exact
		// window between the top-of-pass check and the commit boundary
		f, tmpl, c, m, logs := newSupersessionSummaryHarness(t)
		superseded := false
		startedID := ""
		f.mu.Lock()
		f.startGate = func(id string, _ runtime.ServiceSpec, _ int) {
			startedID = id
			superseded = true
		}
		f.mu.Unlock()
		ctx := withReconcileAuthority(context.Background(), func() bool {
			return !superseded
		})

		// when the pass is applied
		if err := c.Apply(ctx, "fn", tmpl, "img-new", nil); err != nil {
			t.Fatalf("apply: %v", err)
		}
		records := serviceLogRecords(t, logs.String())

		// then the pass really issued corrective work and really superseded at
		// its commit boundary (the negative assertions below are not vacuous)
		if !hasLogRecord(records, "Service: reconciling") {
			t.Error("missing the corrective-work start line: the pass issued no change")
		}
		if !hasLogRecord(records, "Service: pass superseded; preserving old generation") {
			t.Error("missing the explicit supersession line at the commit boundary")
		}

		// then no generic summary claims the abandoned pass converged state: a
		// summary that exists must carry the explicit superseded outcome, and a
		// bare summary (no outcome) is equally ambiguous
		for _, outcome := range summaryOutcomes(records) {
			if outcome != "superseded" {
				t.Errorf("Service: reconciled reported outcome=%q; a superseded pass must carry outcome=%q or suppress the summary",
					outcome, "superseded")
			}
		}

		// then the existing metrics contract is untouched: the pass still
		// observed as changed for the counter
		if got := changedMetric(m); got != 1 {
			t.Errorf("changed metric = %d, want 1 (superseded pass retains changed for metrics)", got)
		}

		// then the old generation still serves and the provisional replacement
		// was cleaned up
		if startedID == "" {
			t.Fatal("replacement never started: the pass did not reach the commit boundary")
		}
		if _, ok := f.containerByID(startedID); ok {
			t.Error("provisional replacement survived the supersession")
		}
		if got := f.runningCount("fn", "service.js"); got != 1 {
			t.Errorf("running = %d, want 1 (the old generation preserved)", got)
		}
	})
}

func TestServiceErrorSummaryReportsPartialStops(t *testing.T) {
	f := newFakeDocker()
	seedOldService(f, "old-1", "fn", "service.js", "img-old", 80, 0)
	seedOldService(f, "old-2", "fn", "service.js", "img-old", 80, 0)
	f.failStopFor = "old-2"
	tmpl := serviceTemplate("nodejs24.x", app.Service{
		Name: "service.js", Entrypoint: "service.js", Port: 80, Replicas: 1,
	})
	logs := &testutil.SyncBuffer{}
	c := NewServiceReconciler(
		f, nil, routing.TraefikConfig{},
		slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		testReconcileTimeout,
	)

	if err := c.Apply(context.Background(), "fn", tmpl, "img-new", nil); err == nil {
		t.Fatal("Apply unexpectedly succeeded")
	}
	var summary *serviceLogRecord
	for _, record := range serviceLogRecords(t, logs.String()) {
		if record.Msg == "Service: reconciled with errors" {
			summary = &record
			break
		}
	}
	if summary == nil {
		t.Fatal("missing service error summary")
	}
	if summary.Stopped == nil || *summary.Stopped != 1 {
		t.Fatalf("stopped count = %v, want 1 completed removal before the batch error", summary.Stopped)
	}
}
