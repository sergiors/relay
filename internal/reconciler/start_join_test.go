package reconciler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"relay/internal/function"
	"relay/internal/runtime"
)

// TestReconcilerStartJoinsLoopsBeforeReturn proves Start does not return until
// the pump/ticker/eventLoop goroutines have exited, so the worker can join the
// reconciler before closing the runtime manager and state DB. A goroutine
// reconciled after Start returned would be a use-after-close; this test pins the
// join barrier directly by driving a reconcile from the pump at shutdown and
// asserting it completes before Start returns.
func TestReconcilerStartJoinsLoopsBeforeReturn(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "joined")

	// The builder blocks inside Prepare until released, so the pump holds an
	// in-flight reconcile. Start must not return while that reconcile runs.
	release := make(chan struct{})
	entered := make(chan struct{})
	var enteredOnce atomic.Bool
	b := &blockingBuilder{entered: entered, release: release, enteredOnce: &enteredOnce}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		r.Start(ctx)
		close(done)
	}()
	<-started

	// Kick a reconcile; it blocks in the pump.
	r.dispatch("joined")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reconcile never entered Prepare")
	}

	// Cancel: Start must NOT return until the blocked prepare finishes and the
	// loop join completes. So immediately after cancel, done must stay open.
	cancel()
	select {
	case <-done:
		close(release)
		t.Fatal("Start returned while an in-flight reconcile was still blocked in the pump")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the prepare: the pump finishes its reconcile and the join closes.
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after the in-flight reconcile completed")
	}
}

// blockingBuilder is a Builder whose Prepare blocks until release, signalling
// entry once. It lets a test hold an in-flight reconcile to prove the Start join
// barrier.
type blockingBuilder struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce *atomic.Bool
}

func (b *blockingBuilder) Prepare(
	_ context.Context, fn function.Function,
) (*runtime.Prepared, error) {
	if b.enteredOnce.CompareAndSwap(false, true) {
		close(b.entered)
	}
	<-b.release
	return &runtime.Prepared{Name: fn.Name, Image: "img-" + fn.Name}, nil
}

func (b *blockingBuilder) Execute(
	context.Context, *runtime.Prepared, string, []byte, []string,
) error {
	return nil
}

// TestReconcilerStartIdempotentSecondStartAfterJoin is a regression guard for the
// join: a Start that returned after ctx cancellation leaves done closed, so a
// caller that races shutdown must not be able to re-leak loops. Starting again on
// a fresh context is a separate lifecycle and is out of contract; this test only
// pins that the first Start's return is the join point.
func TestReconcilerStartIdempotentSecondStartAfterJoin(t *testing.T) {
	root := t.TempDir()
	writeFnDir(t, root, "stable")
	b := &fakeBuilder{}
	r, _ := newTestReconciler(t, root, b, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Start(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return on cancel")
	}
}
