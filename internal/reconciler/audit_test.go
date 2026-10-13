package reconciler

import (
	"context"
	"testing"
	"time"
)

// TestAuditHookFiresOnPeriodicTick pins the periodic re-drive point for deferred
// image cleanup: the Config.Audit hook runs once per periodic audit pass, on the
// ticker goroutine, before that pass reconciles the apps. It is how an image
// whose bounded removal retries were exhausted is retried after the initial
// window elapsed, without a timer or goroutine per image.
func TestAuditHookFiresOnPeriodicTick(t *testing.T) {
	root := t.TempDir()

	tick := make(chan struct{}, 16)
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, nil, func(cfg *Config) {
		cfg.Interval = 5 * time.Millisecond
		cfg.Audit = func() {
			select {
			case tick <- struct{}{}:
			default:
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Start(ctx)
	}()

	select {
	case <-tick:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("the Audit hook never fired on a periodic tick")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not join its loops after cancellation")
	}
}

// TestAuditHookNilIsSafe pins that a reconciler with no Audit hook behaves
// exactly as before (the hook is optional).
func TestAuditHookNilIsSafe(t *testing.T) {
	root := t.TempDir()
	r, _ := newTestReconciler(t, root, &fakeBuilder{}, nil, nil)
	r.reconcileAll() // must not panic with a nil hook
}
