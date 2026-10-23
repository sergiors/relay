package worker

import (
	"context"
	"errors"
	"testing"
)

func TestTerminalReconcilerFailureCancelsLifecycleAndReachesRunResult(t *testing.T) {
	watchErr := errors.New("watch events channel closed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failures := make(chan error, 1)

	runReconcilerStart(ctx, func(context.Context) error {
		return watchErr
	}, func(err error) {
		reportReconcilerFailure(ctx, cancel, failures, err)
	})

	if ctx.Err() == nil {
		t.Fatal("terminal reconciler failure did not cancel the worker lifecycle")
	}
	got := reconcilerRunResult(nil, failures)
	if !errors.Is(got, watchErr) {
		t.Fatalf("Run result = %v, want wrapped terminal watcher error", got)
	}
}

func TestReconcilerLifecycleCancellationIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failureReported := false

	runReconcilerStart(ctx, func(ctx context.Context) error {
		return ctx.Err()
	}, func(error) {
		reportReconcilerFailure(ctx, cancel, make(chan error, 1), context.Canceled)
		failureReported = true
	})

	if failureReported {
		t.Fatal("ordinary lifecycle cancellation was reported as a reconciler failure")
	}
	if got := reconcilerRunResult(nil, make(chan error)); got != nil {
		t.Fatalf("Run result after ordinary cancellation = %v, want nil", got)
	}
}
