package reconciler

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"relay/internal/runtime"
	"relay/internal/testutil"
)

// observerFakeDocker scripts the observer's narrow Docker seam: every inspect
// round is recorded, observations come from the obs map (IDs absent from the
// map model a transient per-ID inspect failure), and err models a whole-round
// failure.
type observerFakeDocker struct {
	mu    sync.Mutex
	obs   map[string]runtime.ServiceHealthObservation
	err   error
	calls [][]string
}

func newObserverFakeDocker() *observerFakeDocker {
	return &observerFakeDocker{obs: map[string]runtime.ServiceHealthObservation{}}
}

func (d *observerFakeDocker) ServiceContainerList(
	context.Context,
) ([]runtime.ServiceContainer, error) {
	return nil, nil
}

func (d *observerFakeDocker) ObserveServiceHealth(
	_ context.Context, ids []string,
) (map[string]runtime.ServiceHealthObservation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, append([]string(nil), ids...))
	out := make(map[string]runtime.ServiceHealthObservation, len(ids))
	for _, id := range ids {
		if observation, ok := d.obs[id]; ok {
			out[id] = observation
		}
	}
	return out, d.err
}

func (d *observerFakeDocker) set(id string, observation runtime.ServiceHealthObservation) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.obs[id] = observation
}

func (d *observerFakeDocker) setErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

// round returns the inspected ID set of the n-th Docker call (0-based), sorted
// so map-iteration order never leaks into assertions.
func (d *observerFakeDocker) round(n int) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if n >= len(d.calls) {
		return nil
	}
	got := append([]string(nil), d.calls[n]...)
	sort.Strings(got)
	return got
}

// rounds returns how many Docker calls one or more observeOnce runs made.
func (d *observerFakeDocker) rounds() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

// newDeterministicObserver builds an observer rooted in a caller-owned context
// WITHOUT starting its ticker loop, so observeOnce runs exactly when the test
// invokes it. The returned cancel releases the rooted context.
func newDeterministicObserver(
	d *observerFakeDocker,
	pending func() []runtime.ServiceHealthCandidate,
	recoverFn func(context.Context) ([]runtime.ServiceHealthCandidate, error),
	notify func(string),
) (*ServiceHealthObserver, context.CancelFunc) {
	o := NewServiceHealthObserver(d, pending, recoverFn, notify, testutil.DiscardLogger())
	o.ctx, o.cancel = context.WithCancel(context.Background())
	return o, o.cancel
}

// pendingCandidates returns a pending() function over a fixed candidate list.
func pendingCandidates(
	candidates ...runtime.ServiceHealthCandidate,
) func() []runtime.ServiceHealthCandidate {
	return func() []runtime.ServiceHealthCandidate { return candidates }
}

// notifyCounter returns a notify sink plus the count accessor for it.
func notifyCounter() (func(string), func() int) {
	var mu sync.Mutex
	var apps []string
	return func(app string) {
			mu.Lock()
			apps = append(apps, app)
			mu.Unlock()
		}, func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(apps)
		}
}

// A first observation only hints when it already carries a relevant signal: a
// non-running state, unhealthy, healthy, or a definitive removal — while a
// steady running candidate without a verdict stays silent. Every repeated
// identical observation is deduplicated, and each real transition hints again.
func TestServiceHealthObserverNotifiesOnlyOnRelevantTransitions(t *testing.T) {
	running := func(health container.HealthStatus) runtime.ServiceHealthObservation {
		return runtime.ServiceHealthObservation{State: container.StateRunning, Health: health}
	}

	t.Run("first observation", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			observation runtime.ServiceHealthObservation
			wantNotify  bool
		}{
			{"running without a verdict", running(""), false},
			{"running while starting", running(container.Starting), false},
			{"running healthy", running(container.Healthy), true},
			{"running unhealthy", running(container.Unhealthy), true},
			{"exited", runtime.ServiceHealthObservation{State: container.StateExited}, true},
			{"definitively missing", runtime.ServiceHealthObservation{Missing: true}, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d := newObserverFakeDocker()
				d.set("c1", tc.observation)
				notify, count := notifyCounter()
				o, cancel := newDeterministicObserver(d,
					pendingCandidates(runtime.ServiceHealthCandidate{App: "fn", ContainerID: "c1"}),
					nil, notify)
				defer cancel()

				o.observeOnce()

				if got := count(); (got == 1) != tc.wantNotify {
					t.Fatalf("notify count = %d for first observation %+v, wantNotify=%v",
						got, tc.observation, tc.wantNotify)
				}
			})
		}
	})

	t.Run("identical observations are deduplicated", func(t *testing.T) {
		d := newObserverFakeDocker()
		d.set("c1", runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Unhealthy,
		})
		notify, count := notifyCounter()
		o, cancel := newDeterministicObserver(d,
			pendingCandidates(runtime.ServiceHealthCandidate{App: "fn", ContainerID: "c1"}),
			nil, notify)
		defer cancel()

		for i := 0; i < 3; i++ {
			o.observeOnce()
		}
		if got := count(); got != 1 {
			t.Fatalf("notify count after three identical rounds = %d, want 1 (deduplicated)", got)
		}
	})

	t.Run("each transition hints exactly once", func(t *testing.T) {
		d := newObserverFakeDocker()
		notify, count := notifyCounter()
		o, cancel := newDeterministicObserver(d,
			pendingCandidates(runtime.ServiceHealthCandidate{App: "fn", ContainerID: "c1"}),
			nil, notify)
		defer cancel()

		// healthy -> unhealthy -> healthy -> missing -> missing: four distinct
		// states hint once each, and a repeated definitive removal stays
		// deduplicated rather than re-hinting every tick.
		steps := []struct {
			observation runtime.ServiceHealthObservation
			wantCount   int
		}{
			{runtime.ServiceHealthObservation{State: container.StateRunning, Health: container.Healthy}, 1},
			{runtime.ServiceHealthObservation{State: container.StateRunning, Health: container.Unhealthy}, 2},
			{runtime.ServiceHealthObservation{State: container.StateRunning, Health: container.Healthy}, 3},
			{runtime.ServiceHealthObservation{Missing: true}, 4},
			{runtime.ServiceHealthObservation{Missing: true}, 4},
		}
		for i, step := range steps {
			d.set("c1", step.observation)
			o.observeOnce()
			if got := count(); got != step.wantCount {
				t.Fatalf("after step %d (%+v): notify count = %d, want %d",
					i, step.observation, got, step.wantCount)
			}
		}
	})

	t.Run("hints carry the owning app", func(t *testing.T) {
		d := newObserverFakeDocker()
		d.set("c1", runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Healthy,
		})
		d.set("c2", runtime.ServiceHealthObservation{Missing: true})
		var apps []string
		var mu sync.Mutex
		o, cancel := newDeterministicObserver(d,
			pendingCandidates(
				runtime.ServiceHealthCandidate{App: "fn-a", ContainerID: "c1"},
				runtime.ServiceHealthCandidate{App: "fn-b", ContainerID: "c2"},
			), nil, func(app string) {
				mu.Lock()
				apps = append(apps, app)
				mu.Unlock()
			})
		defer cancel()

		o.observeOnce()

		// The candidate set is iterated from a map, so the hint ORDER within
		// one round is nondeterministic; the contract is the ownership
		// mapping — one hint per candidate, each carrying that app.
		mu.Lock()
		got := append([]string(nil), apps...)
		mu.Unlock()
		if len(got) != 2 {
			t.Fatalf("notified apps = %v, want exactly one hint per candidate", got)
		}
		sort.Strings(got)
		if got[0] != "fn-a" || got[1] != "fn-b" {
			t.Fatalf("notified apps = %v, want the owning apps [fn-a fn-b] (order-independent)",
				got)
		}
	})
}

// A transient inspect failure — a whole-round error or one ID missing from the
// result — must never be read as a removal: nothing is hinted for it, and the
// registration survives so the next round still inspects it.
func TestServiceHealthObserverTransientFailuresKeepRegistrations(t *testing.T) {
	d := newObserverFakeDocker()
	notify, count := notifyCounter()
	o, cancel := newDeterministicObserver(d,
		pendingCandidates(
			runtime.ServiceHealthCandidate{App: "fn-a", ContainerID: "c1"},
			runtime.ServiceHealthCandidate{App: "fn-b", ContainerID: "c2"},
		), nil, notify)
	defer cancel()

	// Round 1: the whole round fails with no observations at all. Nothing is
	// hinted, and both registrations survive.
	d.setErr(errors.New("daemon blip"))
	o.observeOnce()
	if got := count(); got != 0 {
		t.Fatalf("notify count after a failed round = %d, want 0", got)
	}

	// Round 2: c1 answers healthy while c2 is omitted (a per-ID transient
	// failure). Only c1 hints; c2 is not treated as missing.
	d.setErr(nil)
	d.set("c1", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Healthy,
	})
	o.observeOnce()
	if got := count(); got != 1 {
		t.Fatalf("notify count after the partial round = %d, want 1 (only c1)", got)
	}

	// Round 3: c2 is still inspected — its registration was retained — and its
	// first definitive observation hints normally.
	d.set("c2", runtime.ServiceHealthObservation{Missing: true})
	o.observeOnce()
	if got := d.round(2); len(got) != 2 || got[0] != "c1" || got[1] != "c2" {
		t.Fatalf("round 3 inspected %v, want both c1 and c2 (registration retained)", got)
	}
	if got := count(); got != 2 {
		t.Fatalf("notify count after c2 recovered = %d, want 2", got)
	}
}

// Startup recovery reconstructs uncommitted candidates exactly once and polls
// them until they resolve; malformed entries are skipped entirely.
func TestServiceHealthObserverRecoveryLoadsOnceAndPolls(t *testing.T) {
	d := newObserverFakeDocker()
	// A still-starting observation keeps the recovered entry registered: it is
	// neither healthy nor definitively gone.
	d.set("recovered", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Starting,
	})
	var recoverCalls int
	recoverFn := func(context.Context) ([]runtime.ServiceHealthCandidate, error) {
		recoverCalls++
		return []runtime.ServiceHealthCandidate{
			{App: "fn", ContainerID: "recovered"},
			// Malformed entries are skipped, never polled or hinted.
			{App: "fn", ContainerID: ""},
			{App: "", ContainerID: "nameless"},
		}, nil
	}
	notify, count := notifyCounter()
	o, cancel := newDeterministicObserver(d, nil, recoverFn, notify)
	defer cancel()

	o.observeOnce()
	if recoverCalls != 1 {
		t.Fatalf("recover calls = %d, want 1", recoverCalls)
	}
	if got := d.round(0); len(got) != 1 || got[0] != "recovered" {
		t.Fatalf("round 1 inspected %v, want only [recovered] (malformed entries skipped)", got)
	}

	o.observeOnce()
	if recoverCalls != 1 {
		t.Fatalf("recover calls after a second round = %d, want 1 (loaded once)", recoverCalls)
	}
	if got := d.round(1); len(got) != 1 || got[0] != "recovered" {
		t.Fatalf("round 2 inspected %v, want [recovered] (still registered)", got)
	}
	if got := count(); got != 0 {
		t.Fatalf("notify count for a steady starting candidate = %d, want 0", got)
	}
}

// Once the coordinator reports an app's own pending list it is authoritative:
// stale recovered entries for that app are dropped from polling, while
// recovered entries for apps the coordinator does not report keep being polled.
func TestServiceHealthObserverCoordinatorListSupersedesRecoveredEntries(t *testing.T) {
	d := newObserverFakeDocker()
	d.set("recovered", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Unhealthy,
	})
	d.set("fresh", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Healthy,
	})
	d.set("other-app", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Starting,
	})
	recoverFn := func(context.Context) ([]runtime.ServiceHealthCandidate, error) {
		return []runtime.ServiceHealthCandidate{
			{App: "fn", ContainerID: "recovered"},
			{App: "fn2", ContainerID: "other-app"},
		}, nil
	}
	var pendingMu sync.Mutex
	var pendingList []runtime.ServiceHealthCandidate
	pending := func() []runtime.ServiceHealthCandidate {
		pendingMu.Lock()
		defer pendingMu.Unlock()
		return pendingList
	}
	notify, count := notifyCounter()
	o, cancel := newDeterministicObserver(d, pending, recoverFn, notify)
	defer cancel()

	// Round 1: nothing reported by the coordinator; both recovered entries are
	// polled, and the unhealthy one hints.
	o.observeOnce()
	if got := d.round(0); len(got) != 2 || got[0] != "other-app" || got[1] != "recovered" {
		t.Fatalf("round 1 inspected %v, want [other-app recovered]", got)
	}
	if got := count(); got != 1 {
		t.Fatalf("notify count = %d, want 1 (first-seen unhealthy hints)", got)
	}

	// Round 2: the coordinator claims fn: its stale recovered entry leaves the
	// poll set in favor of the coordinator's own candidate, while fn2's
	// recovered entry (an app the coordinator does not report) stays.
	pendingMu.Lock()
	pendingList = []runtime.ServiceHealthCandidate{{App: "fn", ContainerID: "fresh"}}
	pendingMu.Unlock()
	o.observeOnce()
	if got := d.round(1); len(got) != 2 || got[0] != "fresh" || got[1] != "other-app" {
		t.Fatalf("round 2 inspected %v, want [fresh other-app] (fn's stale recovered entry dropped)", got)
	}
	if got := count(); got != 2 {
		t.Fatalf("notify count = %d, want 2 (fresh healthy hints; other-app still starting stays silent)", got)
	}
}

// An ID that leaves the poll set has its transition baseline cleared, so a
// later re-registration starts fresh instead of deduplicating against a stale
// record.
func TestServiceHealthObserverReRegistrationStartsWithCleanBaseline(t *testing.T) {
	d := newObserverFakeDocker()
	d.set("c1", runtime.ServiceHealthObservation{
		State: container.StateRunning, Health: container.Healthy,
	})
	var pendingMu sync.Mutex
	var pendingList = []runtime.ServiceHealthCandidate{{App: "fn", ContainerID: "c1"}}
	pending := func() []runtime.ServiceHealthCandidate {
		pendingMu.Lock()
		defer pendingMu.Unlock()
		return pendingList
	}
	notify, count := notifyCounter()
	o, cancel := newDeterministicObserver(d, pending, nil, notify)
	defer cancel()

	o.observeOnce()
	if got := count(); got != 1 {
		t.Fatalf("notify count after the first observation = %d, want 1", got)
	}
	// The coordinator drops the candidate: nothing is polled while absent.
	pendingMu.Lock()
	pendingList = nil
	pendingMu.Unlock()
	o.observeOnce()
	if got := d.rounds(); got != 1 {
		t.Fatalf("Docker calls while unregistered = %d, want 1 (no poll without candidates)", got)
	}

	// Re-registration observes the SAME healthy state again — with a clean
	// baseline it hints again rather than being deduplicated against the
	// record from before the gap.
	pendingMu.Lock()
	pendingList = []runtime.ServiceHealthCandidate{{App: "fn", ContainerID: "c1"}}
	pendingMu.Unlock()
	o.observeOnce()
	if got := count(); got != 2 {
		t.Fatalf("notify count after re-registration = %d, want 2 (clean baseline)", got)
	}
}

// A recover failure is retried on a later round instead of latching a
// half-initialized state; with no candidates recovered yet nothing is polled.
func TestServiceHealthObserverRecoveryFailureRetriesNextRound(t *testing.T) {
	d := newObserverFakeDocker()
	var recoverCalls int
	recoverFn := func(context.Context) ([]runtime.ServiceHealthCandidate, error) {
		recoverCalls++
		if recoverCalls == 1 {
			return nil, errors.New("list failed")
		}
		return []runtime.ServiceHealthCandidate{{App: "fn", ContainerID: "c1"}}, nil
	}
	o, cancel := newDeterministicObserver(d, nil, recoverFn, nil)
	defer cancel()

	o.observeOnce()
	if recoverCalls != 1 {
		t.Fatalf("recover calls = %d, want 1", recoverCalls)
	}
	if got := d.rounds(); got != 0 {
		t.Fatalf("Docker calls after a failed recovery = %d, want 0", got)
	}

	d.set("c1", runtime.ServiceHealthObservation{Missing: true})
	o.observeOnce()
	if recoverCalls != 2 {
		t.Fatalf("recover calls after the failed round = %d, want 2 (retried)", recoverCalls)
	}
	if got := d.round(0); len(got) != 1 || got[0] != "c1" {
		t.Fatalf("first Docker call inspected %v, want [c1]", got)
	}
}

// The lifecycle seam: Start is idempotent (one shared done channel), Join
// works before Start, after Start, and repeatedly, and cancelling the lifecycle
// stops the loop so Join drains promptly. A nil observer is safe everywhere.
func TestServiceHealthObserverLifecycleStartAndJoin(t *testing.T) {
	t.Run("nil observer", func(t *testing.T) {
		var o *ServiceHealthObserver
		o.Start(context.Background())
		if err := o.Join(context.Background()); err != nil {
			t.Fatalf("Join on nil = %v, want nil", err)
		}
	})

	t.Run("join without start", func(t *testing.T) {
		o := NewServiceHealthObserver(newObserverFakeDocker(), nil, nil, nil,
			testutil.DiscardLogger())
		if err := o.Join(context.Background()); err != nil {
			t.Fatalf("Join before Start = %v, want nil", err)
		}
		if err := o.Join(context.Background()); err != nil {
			t.Fatalf("repeated Join before Start = %v, want nil", err)
		}
	})

	t.Run("start is idempotent and join drains", func(t *testing.T) {
		d := newObserverFakeDocker()
		o := NewServiceHealthObserver(d,
			pendingCandidates(runtime.ServiceHealthCandidate{App: "fn", ContainerID: "c1"}),
			nil, nil, testutil.DiscardLogger())
		lifecycle, cancel := context.WithCancel(context.Background())
		o.Start(lifecycle)
		done := o.done
		o.Start(context.Background())
		if o.done != done {
			t.Fatal("a second Start rebuilt the loop channel; the loop must start exactly once")
		}

		// The live loop observes right away and keeps polling on its cadence.
		testutil.WaitFor(t, 2*time.Second, "first observation round",
			func() bool { return d.rounds() >= 1 })

		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer joinCancel()
		if err := o.Join(joinCtx); err != nil {
			t.Fatalf("Join after lifecycle cancellation = %v, want nil", err)
		}
		if err := o.Join(context.Background()); err != nil {
			t.Fatalf("repeated Join = %v, want nil", err)
		}
	})
}

// A startup-recovered registration lives until the coordinator's own view is
// AUTHORITATIVE for its app: before any logical request is tracked for that
// app, the recovered entry is polled even though the coordinator reports no
// candidates (which is what keeps a missed readiness notification catchable);
// once the app is tracked, an ID the coordinator's candidate list omits is
// stale and stops being polled, while an ID it still reports — or an
// untracked app's entry — keeps its registration.
func TestServiceHealthObserverRetiresRecoveredEntryOnlyWhenAppTracked(t *testing.T) {
	d := newObserverFakeDocker()
	for _, id := range []string{"recovered-a", "recovered-b"} {
		d.set(id, runtime.ServiceHealthObservation{
			State: container.StateRunning, Health: container.Starting,
		})
	}
	recoverFn := func(context.Context) ([]runtime.ServiceHealthCandidate, error) {
		return []runtime.ServiceHealthCandidate{
			{App: "fn-a", ContainerID: "recovered-a"},
			{App: "fn-b", ContainerID: "recovered-b"},
		}, nil
	}
	var mu sync.Mutex
	var pendingList []runtime.ServiceHealthCandidate
	tracked := map[string]bool{}
	pending := func() []runtime.ServiceHealthCandidate {
		mu.Lock()
		defer mu.Unlock()
		return pendingList
	}
	trackedApps := func() map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]bool, len(tracked))
		for name, ok := range tracked {
			out[name] = ok
		}
		return out
	}
	notify, count := notifyCounter()
	o := NewServiceHealthObserver(d, pending, recoverFn, notify,
		testutil.DiscardLogger(), WithTrackedServiceApps(trackedApps))
	o.ctx, o.cancel = context.WithCancel(context.Background())
	defer o.cancel()

	// Round 1: no logical request tracked anywhere — startup recovery keeps
	// BOTH entries polled despite the coordinator reporting no candidates.
	o.observeOnce()
	if got := d.round(0); len(got) != 2 || got[0] != "recovered-a" || got[1] != "recovered-b" {
		t.Fatalf("round 1 inspected %v, want both recovered entries (startup recovery)", got)
	}

	// Round 2: fn-a becomes tracked but its candidate list is empty — the
	// coordinator's view is now authoritative, so recovered-a is stale (it was
	// committed, replaced, or removed) and retires; untracked fn-b keeps its
	// registration.
	mu.Lock()
	tracked["fn-a"] = true
	mu.Unlock()
	o.observeOnce()
	if got := d.round(1); len(got) != 1 || got[0] != "recovered-b" {
		t.Fatalf("round 2 inspected %v, want only [recovered-b] "+
			"(tracked fn-a's stale entry retired; untracked fn-b retained)", got)
	}

	// Round 3: fn-a is tracked AND reports the recovered ID itself — the
	// authoritative list still contains it, so it is polled again.
	mu.Lock()
	pendingList = []runtime.ServiceHealthCandidate{{App: "fn-a", ContainerID: "recovered-a"}}
	mu.Unlock()
	o.observeOnce()
	if got := d.round(2); len(got) != 2 || got[0] != "recovered-a" || got[1] != "recovered-b" {
		t.Fatalf("round 3 inspected %v, want both entries (an ID the tracked "+
			"list still reports is not stale)", got)
	}
	if got := count(); got != 0 {
		t.Fatalf("notify count for steady starting candidates = %d, want 0", got)
	}
}
