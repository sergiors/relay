package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"relay/internal/app"
	"relay/internal/reconciler"
	"relay/internal/routing"
	"relay/internal/runtime"
)

// deadlineBlockingDocker is a reconciler.Docker whose ServiceContainerList
// records the deadline of the context it receives, signals entry, then blocks
// until that context is done and reports its error. It models a daemon call that
// HONORS cancellation, so a per-pass bound makes the exclusive housekeeping
// window return and lift its pause.
type deadlineBlockingDocker struct {
	enteredOnce sync.Once
	entered     chan struct{}
	mu          sync.Mutex
	deadlines   []time.Time
}

func newDeadlineBlockingDocker() *deadlineBlockingDocker {
	return &deadlineBlockingDocker{entered: make(chan struct{})}
}

func (d *deadlineBlockingDocker) ResolveServiceImage(
	context.Context, string, *app.Template, app.Service, string,
) (runtime.ServiceImage, error) {
	return runtime.ServiceImage{Ref: "img"}, nil
}

func (d *deadlineBlockingDocker) StartService(context.Context, runtime.ServiceSpec, int) (string, error) {
	return "id-1", nil
}

func (d *deadlineBlockingDocker) ServiceContainerList(ctx context.Context) ([]runtime.ServiceContainer, error) {
	d.mu.Lock()
	if dl, ok := ctx.Deadline(); ok {
		d.deadlines = append(d.deadlines, dl)
	} else {
		d.deadlines = append(d.deadlines, time.Time{})
	}
	d.mu.Unlock()
	d.enteredOnce.Do(func() { close(d.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *deadlineBlockingDocker) StopServiceContainers(context.Context, []runtime.ServiceContainer) error {
	return nil
}

func (d *deadlineBlockingDocker) RemoveAppServiceContainers(context.Context, string) (int, error) {
	return 0, nil
}

func (d *deadlineBlockingDocker) NetworkExists(context.Context, string) (bool, error) {
	return true, nil
}

func (d *deadlineBlockingDocker) recordedDeadlines() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.deadlines...)
}

// TestStartupHousekeepingSweepBoundReleasesExclusive proves the startup service
// orphan sweep runs under its OWN fresh bound rooted in the housekeeping
// lifecycle, and that when that bound fires the RunExclusive pause is lifted
// (the callback returns and the coordinator resumes). It drives the REAL
// coordinator's RunExclusive and the production sweepStartupServiceOrphans
// helper, with a daemon fake that blocks until its context is done.
//
// The housekeeping lifecycle carries a short deadline so the test does not wait
// the full reconcileTimeout: the child bound is min(parent, now+reconcileTimeout),
// so the parent's deadline fires first and the sweep returns.
func TestStartupHousekeepingSweepBoundReleasesExclusive(t *testing.T) {
	fake := newDeadlineBlockingDocker()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtrl := reconciler.NewServiceReconciler(fake, nil, routing.TraefikConfig{}, logger, reconcileTimeout)

	coordinator := reconciler.NewServiceCoordinator(svcCtrl)
	coordCtx, cancelCoord := context.WithCancel(context.Background())
	defer cancelCoord()
	coordinator.Start(coordCtx)

	// The housekeeping lifecycle carries a short deadline; the sweep's child
	// bound is capped by it, so the blocking list unblocks promptly.
	housekeeping, cancelHK := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelHK()

	resumeObserved := make(chan struct{})
	done := startStartupHousekeeping(housekeeping, logger, startupHousekeeper{
		exclusive: coordinator.RunExclusive,
		sweep:     func(hctx context.Context) { sweepStartupServiceOrphans(hctx, svcCtrl, map[string]bool{"alpha": true}) },
		images:    func(context.Context) {},
		deps:      func(context.Context) { close(resumeObserved) },
	})

	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("orphan sweep listing never entered")
	}

	// The sweep is bounded: its recorded listing deadline is within
	// reconcileTimeout (and, here, the shorter housekeeping parent).
	deadlines := fake.recordedDeadlines()
	if len(deadlines) == 0 || deadlines[0].IsZero() {
		t.Fatalf("orphan sweep listing carried no deadline: %v", deadlines)
	}
	if until := time.Until(deadlines[0]); until <= 0 || until > reconcileTimeout {
		t.Fatalf("orphan sweep deadline %v is not bounded by reconcileTimeout=%v", deadlines[0], reconcileTimeout)
	}

	// When the bound fires, the sweep returns, the exclusive callback returns,
	// and the coordinator resumes — the deps pass (after the sweep) runs.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not finish after its bound fired; exclusive pause not released")
	}
	select {
	case <-resumeObserved:
	default:
		t.Fatal("the deps pass after the sweep did not run; the exclusive window never closed")
	}
}
