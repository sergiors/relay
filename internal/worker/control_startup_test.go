package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeControlProvisioner is the controlNetworkProvisioner seam for the startup
// control-network pre-flight: it records how it was called (call count,
// whether the provisioning context was bounded, and whether the worker
// lifecycle cancellation reached it) and returns a scripted error.
type fakeControlProvisioner struct {
	err       error
	calls     int
	bounded   bool
	cancelled bool
}

func (f *fakeControlProvisioner) EnsureControlNetwork(ctx context.Context) error {
	f.calls++
	deadline, ok := ctx.Deadline()
	if ok {
		f.bounded = time.Until(deadline) > 0 && time.Until(deadline) <= reconcileTimeout
	}
	f.cancelled = ctx.Err() != nil
	return f.err
}

// TestEnsureNodeControlNetwork pins the worker's startup control-network
// pre-flight semantics against the narrow provisioner seam: it delegates
// exactly once, its context is bounded by reconcileTimeout (a hung daemon
// cannot stall startup forever) while still observing the worker lifecycle, a
// provisioner failure is wrapped with its step context for an actionable
// startup error, and a lifecycle cancellation reaches the call so the preflight
// can classify it as a graceful interruption.
func TestEnsureNodeControlNetwork(t *testing.T) {
	t.Run("success delegates exactly once through a bounded context", func(t *testing.T) {
		f := &fakeControlProvisioner{}
		if err := ensureNodeControlNetwork(context.Background(), f); err != nil {
			t.Fatalf("ensure = %v, want nil", err)
		}
		if f.calls != 1 {
			t.Fatalf("provisioner calls = %d, want exactly 1", f.calls)
		}
		if !f.bounded {
			t.Fatal("provisioning context must be bounded by reconcileTimeout")
		}
		if f.cancelled {
			t.Fatal("a live worker lifecycle must reach the provisioner uncancelled")
		}
	})

	t.Run("failure is wrapped with the step context", func(t *testing.T) {
		boom := errors.New("relay-control-node already exists without relay.network.role=execution-control")
		f := &fakeControlProvisioner{err: boom}
		err := ensureNodeControlNetwork(context.Background(), f)
		if err == nil || !errors.Is(err, boom) {
			t.Fatalf("ensure = %v, want the wrapped provisioner error", err)
		}
		if !strings.Contains(err.Error(), "ensure control network") {
			t.Fatalf("error = %q, want the step context", err)
		}
	})

	t.Run("lifecycle cancellation reaches the provisioner", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f := &fakeControlProvisioner{err: context.Canceled}
		err := ensureNodeControlNetwork(ctx, f)
		if err == nil {
			t.Fatal("a cancelled lifecycle must surface the provisioning failure")
		}
		if f.calls != 1 {
			t.Fatalf("provisioner calls = %d, want 1 (classification needs the call's error)", f.calls)
		}
		if !f.cancelled {
			t.Fatal("the worker lifecycle cancellation must propagate into the provisioning context")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want it to wrap context.Canceled for startupInterrupted", err)
		}
	})
}
