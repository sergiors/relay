package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"relay/internal/config"
	"relay/internal/observability/metrics"
)

// TestOpenWorkerStateCancellationCarriesStartupSentinel pins the Stage A
// extraction's cancellation contract: a canceled lifecycle while state.Open is
// in flight must return a nil handle AND the errStartupInterrupted sentinel,
// never (nil, nil). Run is the only place that converts the sentinel to a
// graceful nil return via startupResult, so the test also proves that
// conversion. A canceled context short-circuits the bounded connect before any
// real PostgreSQL is contacted.
func TestOpenWorkerStateCancellationCarriesStartupSentinel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	st, err := openWorkerState(
		ctx,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config.Config{
			DatabaseURL: "postgres://relay:relay@127.0.0.1:1/relay?sslmode=disable",
			WorkerID:    "worker-canceled",
			InstanceID:  "instance-canceled",
		},
		metrics.New(),
	)
	if st != nil {
		t.Fatalf("openWorkerState handle = %v, want nil on canceled open", st)
	}
	if !errors.Is(err, errStartupInterrupted) {
		t.Fatalf("openWorkerState error = %v, want errStartupInterrupted", err)
	}
	if got := startupResult(err); got != nil {
		t.Fatalf("startupResult(cancellation) = %v, want nil (graceful shutdown)", got)
	}
}

// TestOpenWorkerStateGenuineFailurePassesThrough pins that the Run-side
// startupResult conversion does not swallow a genuine state-open error: an
// invalid configuration fails before any database connection is attempted, and
// the wrapped error reaches the CLI unchanged.
func TestOpenWorkerStateGenuineFailurePassesThrough(t *testing.T) {
	st, err := openWorkerState(
		context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config.Config{},
		metrics.New(),
	)
	if st != nil {
		t.Fatalf("openWorkerState handle = %v, want nil on open failure", st)
	}
	if err == nil || errors.Is(err, errStartupInterrupted) {
		t.Fatalf("openWorkerState error = %v, want a genuine non-sentinel failure", err)
	}
	if got := startupResult(err); got == nil {
		t.Fatal("startupResult(genuine state failure) = nil, want the error")
	}
}
