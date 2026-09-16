package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/reconciler"
	"relay/internal/runner"
	"relay/internal/runtime"
)

// svcDeadlineDocker is a minimal reconciler.Docker fake that records the
// deadline of every ctx passed to ServiceContainerList. Apply drives Reconcile,
// which lists containers once per call, so the per-function Applys in
// reconcileStartupServices are observable: each must receive its OWN bounded
// context (a distinct ~startupTimeout deadline), not a single shared one.
type svcDeadlineDocker struct {
	mu        sync.Mutex
	deadlines []time.Time // deadline (local time) of each ServiceContainerList call; zero = no deadline
}

func (f *svcDeadlineDocker) StartService(_ context.Context, _ runtime.ServiceSpec, _ int) (string, error) {
	return "id-1", nil
}

func (f *svcDeadlineDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, d)
	} else {
		f.deadlines = append(f.deadlines, time.Time{})
	}
	return nil, nil
}

func (f *svcDeadlineDocker) StopServiceContainers(_ context.Context, _ []runtime.ServiceContainer) error {
	return nil
}

func (f *svcDeadlineDocker) RemoveFunctionServiceContainers(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (f *svcDeadlineDocker) recordedDeadlines() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.deadlines...)
}

// TestReconcileStartupServicesBoundedPerFunction pins the per-function timeout
// fix: reconcileStartupServices must give each function's service converge its
// OWN bounded context, so one slow Docker call cannot consume the budget of the
// functions that follow. It does so by recording the deadline each Apply's
// ServiceContainerList saw: two available functions must each observe a distinct
// ~startupTimeout deadline. The trailing SweepOrphans call (context.Background)
// must carry no deadline.
func TestReconcileStartupServicesBoundedPerFunction(t *testing.T) {
	fake := &svcDeadlineDocker{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, logger)

	tmpl := &function.Template{
		Runtime: "node24",
		Services: []function.Service{
			{Entrypoint: "service.js", Port: 80, Replicas: 1},
		},
	}
	prepared := []*runner.PreparedFunction{
		runner.NewPrepared(function.Function{Name: "alpha", Template: tmpl}, &runtime.Prepared{Image: "img-alpha", Env: nil}, nil),
		runner.NewPrepared(function.Function{Name: "beta", Template: tmpl}, &runtime.Prepared{Image: "img-beta", Env: nil}, nil),
	}
	// liveNames is derived from `functions` in the helper; provide the matching
	// on-disk set so the orphan sweep finds nothing to remove.
	functions := []function.Function{
		{Name: "alpha", Template: tmpl},
		{Name: "beta", Template: tmpl},
	}

	reconcileStartupServices(prepared, functions, svcCtrl, logger)

	deadlines := fake.recordedDeadlines()
	// Two per-function Applys (each with a bounded deadline) + the SweepOrphans
	// call (context.Background, no deadline).
	if len(deadlines) != 3 {
		t.Fatalf("ServiceContainerList saw %d calls, want 3 (two Applys + sweep): %v", len(deadlines), deadlines)
	}

	var applyDeadlines []time.Time
	for _, d := range deadlines {
		if !d.IsZero() {
			applyDeadlines = append(applyDeadlines, d)
		} else if deadlines[len(deadlines)-1] != d {
			t.Fatalf("a non-final call carried no deadline; only SweepOrphans should: %v", deadlines)
		}
	}
	// The sweep is the final call and must be unbounded.
	if !deadlines[len(deadlines)-1].IsZero() {
		t.Fatalf("SweepOrphans call must use an unbounded context, got deadline %v", deadlines[len(deadlines)-1])
	}

	if len(applyDeadlines) != 2 {
		t.Fatalf("per-function Applys = %d, want 2: %v", len(applyDeadlines), deadlines)
	}
	// Both Applys must be bounded and each must have a DISTINCT deadline (a
	// fresh ~startupTimeout bound per function, not one shared context).
	for i, d := range applyDeadlines {
		if got := time.Until(d); got <= 0 || got > startupTimeout {
			t.Fatalf("Apply %d deadline %v is not bounded to startupTimeout=%v (until=%v)", i, d, startupTimeout, got)
		}
	}
	if applyDeadlines[0].Equal(applyDeadlines[1]) {
		t.Fatalf("Apply deadlines are identical (%v); want distinct per-function bounds", applyDeadlines)
	}
}
