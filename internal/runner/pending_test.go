package runner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"relay/internal/app"
	eventmatch "relay/internal/event"
	"relay/internal/observability/metrics"
	"relay/internal/stream"
	"relay/internal/testutil"
)

// pendingTmplV1 is an active generation with a single "created" rule.
const pendingTmplV1 = `runtime: node24
events:
  - handler: handler.created
    pattern:
      event_name: [created]
`

// pendingTmplV2 adds a "deleted" rule to the v1 generation. The two rules carry
// DISTINCT handlers so the template validates (a parsed template rejects two
// event rules sharing a handler).
const pendingTmplV2 = `runtime: node24
events:
  - handler: handler.created
    pattern:
      event_name: [created]
  - handler: handler.deleted
    pattern:
      event_name: [deleted]
`

// pendingTmplOther is a generation whose rules do not overlap v1/v2, used to
// prove slice/index coherence across concurrent pending swaps.
const pendingTmplOther = `runtime: node24
events:
  - handler: handler.other_one
    pattern:
      event_name: [other]
  - handler: handler.other_two
    pattern:
      event_name: [other]
`

// pendingTmplDistinctSamePattern declares two rules with DISTINCT handlers that
// match the SAME event, so an active rule and a pending-only rule can both match
// one delivery without being deduped by invocation identity.
const pendingTmplDistinctSamePattern = `runtime: node24
events:
  - handler: handler.created
    pattern:
      event_name: [created]
  - handler: handler.created_v2
    pattern:
      event_name: [created]
`

func mustTemplate(t *testing.T, yaml string) *app.Template {
	t.Helper()
	tmpl, err := app.ParseTemplate([]byte(yaml))
	if err != nil {
		t.Fatalf("parse template: %v", err)
	}
	return tmpl
}

// pendingApp builds the non-runnable app.App a pending desired entry carries.
func pendingApp(name, yaml string, t *testing.T) app.App {
	t.Helper()
	return app.App{Name: name, Template: mustTemplate(t, yaml)}
}

// TestHandlePendingOnlyRuleIsUnavailableAndNotExecuted pins the core new-app
// window: a brand-new app registered ONLY as pending (no active, runnable
// generation) must classify an event matching its desired rule as
// matched-but-unavailable — never unmatched/ACKed, never executed, and with no
// invocation attempt claimed.
func TestHandlePendingOnlyRuleIsUnavailableAndNotExecuted(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics(nil, testutil.DiscardLogger(), nil)
	r.Registry().SetPending("brand-new", pendingApp("brand-new", pendingTmplV1, t))

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "m", map[string]any{"event_name": "created"})
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable", err)
	}
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("pending match must never be exhausted: %v", err)
	}
	if exec.count() != 0 {
		t.Fatalf("pending rule must not execute: calls = %d, want 0", exec.count())
	}
	if len(prog.attempts) != 0 || len(prog.marks) != 0 || len(prog.exhausted) != 0 || len(prog.failures) != 0 {
		t.Fatalf("pending match must not touch invocation state: attempts=%v marks=%v exhausted=%v failures=%v",
			prog.attempts, prog.marks, prog.exhausted, prog.failures)
	}

	// Once the generation becomes active (preparation succeeded and the pending
	// entry was cleared by Replace), the same event executes and ACKs.
	r.Registry().Replace("brand-new", alwaysMatchFn(t, "brand-new", exec))
	if r.Registry().HasPending("brand-new") {
		t.Fatal("Replace must have cleared the pending entry")
	}
	if err := r.Handle(ctx, "m", map[string]any{"event_name": "created"}); err != nil {
		t.Fatalf("redelivery after activation error = %v, want nil (ACK)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("activated handler executions = %d, want 1", exec.count())
	}
}

// TestHandlePendingRuleDedupedAgainstActive pins the overlap rule: a rule that
// matches through the ACTIVE generation is executed and is NOT separately held
// by an identical pending rule. With only that shared rule matching, Handle
// returns nil (all matched work complete → ACK).
func TestHandlePendingRuleDedupedAgainstActive(t *testing.T) {
	exec := &countingExecutor{}
	r := NewWithMetrics([]*PreparedApp{parsedFn(t, "alpha", pendingTmplV1, exec)}, testutil.DiscardLogger(), nil)
	// Pending v2 re-declares the same created rule (and a new one that this event
	// does not match).
	r.Registry().SetPending("alpha", pendingApp("alpha", pendingTmplV2, t))

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	if err := r.Handle(ctx, "m", map[string]any{"event_name": "created"}); err != nil {
		t.Fatalf("handle error = %v, want nil (active rule dedupes the identical pending rule)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("executions = %d, want 1 (executed once, not held)", exec.count())
	}
	if !prog.IsComplete("alpha/handler.created") {
		t.Fatal("the shared invocation must be marked complete")
	}
}

// TestHandleMixedActiveAndPendingFanOut pins the aggregate: an event matching an
// active invocation (which completes) plus a pending-only invocation stays
// pending as unavailable, and a redelivery does not re-run the completed active
// invocation. The pending v2 rule is a distinct handler from the active v1 rule,
// so an event cannot match both through one pattern; the active rule is exercised
// via its own event and the pending-only rule via the event both generations
// share in identity but only v2 declares.
func TestHandleMixedActiveAndPendingFanOut(t *testing.T) {
	exec := &countingExecutor{}
	active := parsedFn(t, "alpha", pendingTmplV1, exec)
	r := NewWithMetrics([]*PreparedApp{active}, testutil.DiscardLogger(), nil)
	r.Registry().SetPending("alpha", pendingApp("alpha", pendingTmplV2, t))

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	// Event matches ONLY the active created rule: executes and ACKs.
	if err := r.Handle(ctx, "m1", map[string]any{"event_name": "created"}); err != nil {
		t.Fatalf("active-only event error = %v, want nil", err)
	}
	if exec.count() != 1 || !prog.IsComplete("alpha/handler.created") {
		t.Fatalf("active-only: calls=%d complete=%v, want 1/true", exec.count(), prog.IsComplete("alpha/handler.created"))
	}

	// Event matches ONLY the pending deleted rule: unavailable, no execution.
	if err := r.Handle(ctx, "m2", map[string]any{"event_name": "deleted"}); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("pending-only event error = %v, want ErrAppUnavailable", err)
	}
	if exec.count() != 1 {
		t.Fatalf("pending-only event must not run a handler: calls = %d, want 1", exec.count())
	}

	// The published active generation is still exactly v1 (no partial install of
	// the pending rules).
	if got := handlerNames(r.Registry().GetByName("alpha").fn.Template.Events); strings.Join(got, ",") != "handler.created" {
		t.Fatalf("active rules = %v, want only [handler.created] while v2 is pending", got)
	}
}

// TestHandleMixedFanOutAvailableCompletesPendingHoldsNoRerun pins the full mixed
// aggregate against the pending seam: one event matches an ACTIVE invocation
// (which completes) and a PENDING-ONLY invocation (distinct handler, same
// pattern). Handle returns ErrAppUnavailable so the message stays pending; a
// redelivery does not re-run the completed active invocation while the pending
// sibling is still unresolved.
func TestHandleMixedFanOutAvailableCompletesPendingHoldsNoRerun(t *testing.T) {
	exec := &countingExecutor{}
	// Active v1 has only handler.created; pending declares handler.created plus a
	// pending-only handler.created_v2, both matching the event.
	r := NewWithMetrics([]*PreparedApp{parsedFn(t, "alpha", pendingTmplV1, exec)}, testutil.DiscardLogger(), nil)
	r.Registry().SetPending("alpha", pendingApp("alpha", pendingTmplDistinctSamePattern, t))

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)
	event := map[string]any{"event_name": "created"}

	err := r.Handle(ctx, "m", event)
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("delivery 1 error = %v, want ErrAppUnavailable (pending-only sibling holds)", err)
	}
	if exec.count() != 1 {
		t.Fatalf("active invocation executions = %d, want 1", exec.count())
	}
	if !prog.IsComplete("alpha/handler.created") {
		t.Fatal("active invocation must be marked complete")
	}
	if prog.IsTerminal("alpha/handler.created_v2") {
		t.Fatal("pending-only invocation must remain unresolved")
	}

	// Redelivery: the completed active invocation is NOT re-run; the pending-only
	// one still holds the message pending.
	if err := r.Handle(ctx, "m", event); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("delivery 2 error = %v, want ErrAppUnavailable", err)
	}
	if exec.count() != 1 {
		t.Fatalf("active invocation re-ran on redelivery: calls = %d, want 1", exec.count())
	}
}

// TestHandlePendingMatchEngagesAppMetrics pins that a pending-only match is
// classified MATCHED (not unmatched) and engages the app, while touching no
// handler execution counters.
func TestHandlePendingMatchEngagesAppMetrics(t *testing.T) {
	m := metrics.New()
	r := NewWithMetrics(nil, testutil.DiscardLogger(), m)
	r.Registry().SetPending("brand-new", pendingApp("brand-new", pendingTmplV1, t))

	if err := r.Handle(context.Background(), "m", map[string]any{"event_name": "created"}); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("handle error = %v, want ErrAppUnavailable", err)
	}
	if got := m.Counter(metrics.MetricEventsReceived); got != 1 {
		t.Errorf("events_received = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsMatched); got != 1 {
		t.Errorf("events_matched = %d, want 1", got)
	}
	if got := m.Counter(metrics.MetricEventsUnmatched); got != 0 {
		t.Errorf("events_unmatched = %d, want 0", got)
	}
	fs := m.AppStatsSnapshot()
	if len(fs) != 1 || fs[0].App != "brand-new" || fs[0].EventsMatchedTotal != 1 {
		t.Fatalf("pending app must be engaged as matched: %+v", fs)
	}
	if got := m.Counter(metrics.MetricHandlerFailure); got != 0 {
		t.Errorf("handler_failure = %d, want 0", got)
	}
}

// TestHandlePendingOnlyPreventsDLQOfExhaustedSibling pins the aggregate ordering
// with a pending sibling: an ACTIVE invocation that exhausts on this delivery
// must NOT route the message to the DLQ while a PENDING-ONLY invocation is still
// unresolved. The pending invocation keeps the message pending (ErrAppUnavailable
// is returned before the exhaustion/DLQ branch), so the message is neither ACKed
// nor dead-lettered prematurely.
func TestHandlePendingOnlyPreventsDLQOfExhaustedSibling(t *testing.T) {
	failExec := &countingExecutor{fail: true}
	// Active v1 has one always-matching rule with retries:0, so its first failure
	// exhausts it.
	activeRule := alwaysMatchRule(0)
	activeRule.Retries = 0
	active := buildFn(fnSpec{name: "alpha", rules: []app.EventRule{activeRule}}, failExec)
	r := NewWithMetrics([]*PreparedApp{active}, testutil.DiscardLogger(), nil)
	// The pending desired generation declares a DISTINCT handler matching the
	// same event, so it is not deduped and stays unresolved.
	pendingTmpl := `runtime: node24
events:
  - handler: handler.extra
    pattern: {}
`
	r.Registry().SetPending("alpha", pendingApp("alpha", pendingTmpl, t))

	prog := newFakeInvocationState()
	ctx := stream.WithInvocationState(context.Background(), prog)

	err := r.Handle(ctx, "m", map[string]any{"event_name": "x"})
	if errors.Is(err, stream.ErrInvocationExhausted) {
		t.Fatalf("must NOT DLQ while a pending-only sibling is unresolved: %v", err)
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("error = %v, want ErrAppUnavailable (pending sibling holds)", err)
	}
	if !prog.IsTerminal("alpha/index.run") {
		t.Fatal("the active invocation must have exhausted")
	}
	if prog.IsTerminal("alpha/handler.extra") {
		t.Fatal("the pending-only invocation must remain unresolved")
	}
}

// TestPendingSnapshotCoherenceUnderConcurrentSwap swaps the pending generation
// while taking snapshots and asserts every snapshot's pending slice and pending
// index are from the SAME generation: for each pending entry the indexed match
// agrees with a full exact scan of that entry's own template. If the slice and
// index came from different generations the two non-overlapping rule sets would
// diverge.
func TestPendingSnapshotCoherenceUnderConcurrentSwap(t *testing.T) {
	r := New(nil, testutil.DiscardLogger())
	reg := r.Registry()

	event := map[string]any{"event_name": "created", "status": "COMPLETED"}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if w%2 == 0 {
					reg.SetPending("swapped", pendingApp("swapped", pendingTmplV2, t))
				} else {
					reg.SetPending("swapped", pendingApp("swapped", pendingTmplOther, t))
				}
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			snap := reg.snapshotPinned()
			for _, pf := range snap.pending {
				got := handlerNames(snap.matchingPendingRules(pf, event))
				want := handlerNames(eventmatch.MatchingEventRules(pf.fn.Template.Events, event))
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Errorf("pending snapshot incoherent: indexed=%v full=%v", got, want)
				}
				if snap.pendingRulesFor(pf) == nil {
					t.Errorf("pending entry missing its candidate index")
				}
			}
			snap.release()
		}
	}()
	<-done
	close(stop)
	wg.Wait()
}

// TestReplaceClearsPendingAtomicallyUnderConcurrency pins the atomic
// active+pending transition: Replace publishes the new active generation and
// clears the pending entry in ONE locked step, so a snapshot can never observe
// the successful new ACTIVE generation beside its now-obsolete pending rules. A
// writer runs the reconciler's real sequence (active v1 -> SetPending(v2) ->
// Replace(active v2) -> back to active v1); a reader asserts that whenever an
// active v2 (two-rule) generation is visible for the name, no pending entry for
// that name is visible in the same snapshot.
func TestReplaceClearsPendingAtomicallyUnderConcurrency(t *testing.T) {
	r := New(nil, testutil.DiscardLogger())
	reg := r.Registry()
	reg.Set([]*PreparedApp{parsedFn(t, "a", pendingTmplV1, &countingExecutor{})})

	activeV1 := func() *PreparedApp { return parsedFn(t, "a", pendingTmplV1, &countingExecutor{}) }
	activeV2 := func() *PreparedApp { return parsedFn(t, "a", pendingTmplV2, &countingExecutor{}) }

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Reconciler sequence: an available v1 is being rebuilt to v2, so
			// v2's rules are published as pending while v1 stays active; success
			// then installs active v2 and clears pending atomically.
			reg.Replace("a", activeV1())
			reg.SetPending("a", pendingApp("a", pendingTmplV2, t))
			reg.Replace("a", activeV2())
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			snap := reg.snapshotPinned()
			activeV2Visible := false
			hasPending := false
			for _, pf := range snap.fns {
				if pf.fn.Name == "a" && pf.Prepared() != nil && len(pf.fn.Template.Events) == 2 {
					activeV2Visible = true
				}
			}
			for _, pf := range snap.pending {
				if pf.fn.Name == "a" {
					hasPending = true
				}
			}
			if activeV2Visible && hasPending {
				t.Error("observed active v2 published together with its obsolete pending v2 entry")
			}
			snap.release()
		}
	}()
	<-done
	close(stop)
	wg.Wait()
}
