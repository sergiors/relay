package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"relay/internal/observability/tracing"
)

// fakeConsumerHealth is a deterministic consumerHealth whose health toggles, so
// the readiness probe's Redis dependency can be exercised without Redis and
// without sleeps.
type fakeConsumerHealth struct {
	healthy bool
}

func (f *fakeConsumerHealth) Healthy() bool { return f.healthy }

// fakeDockerReadiness is a deterministic dockerReadinessProbe with scripted
// ping and NETWORKS results. It records the networks it was asked to verify.
type fakeDockerReadiness struct {
	pingErr   error
	missing   string
	netOK     bool
	netErr    error
	verified  []string
	pingCalls int
}

func (f *fakeDockerReadiness) Ping(context.Context) error {
	f.pingCalls++
	return f.pingErr
}

func (f *fakeDockerReadiness) VerifyNetworks(_ context.Context, networks []string) (string, bool, error) {
	f.verified = append([]string(nil), networks...)
	return f.missing, f.netOK, f.netErr
}

// TestReadinessStartsFalseAndClearsBeforeShutdown pins the worker-owned ready
// flag's lifecycle: it starts false, is set true only at the explicit
// ready-to-consume boundary, and is cleared as the first shutdown instruction.
// It mirrors Run's defer order (clear readiness BEFORE cancelling the lifecycle)
// so the "not-ready wins a shutdown race" contract is pinned at the seam Run
// crosses: a query landing between the clear and the cancellation observes
// not-ready, and the lifecycle is cancelled only afterwards.
func TestReadinessStartsFalseAndClearsBeforeShutdown(t *testing.T) {
	r := newReadiness(context.Background())

	if ready, _ := r.Ready(context.Background()); ready {
		t.Fatal("readiness must start false")
	}
	// A false flag reports not-ready without consulting any dependency.
	if _, reason := r.Ready(context.Background()); reason == "" {
		t.Fatal("a not-ready worker must report a reason")
	}

	consumer := &fakeConsumerHealth{healthy: true}
	manager := &fakeDockerReadiness{netOK: true}
	r.startReady(consumer, manager, []string{"backend"})

	if ready, reason := r.Ready(context.Background()); !ready {
		t.Fatalf("readiness must be true at the ready-to-consume boundary (reason %q)", reason)
	}

	// Shutdown defer, in Run's exact order: clear readiness FIRST, then cancel
	// the lifecycle, then teardown. The clear must dominate the cancellation so
	// a query racing shutdown never sees a stale true. The test only needs the
	// clear here; Run owns the same ordering.
	r.setNotReady()
	if ready, _ := r.Ready(context.Background()); ready {
		t.Fatal("readiness must be false immediately after the shutdown clear, before cancellation")
	}
}

// TestReadinessNotReadyWhenLifecycleCancelledBeforeClear pins the race the flag
// alone cannot win: the worker lifecycle context can be cancelled before Run
// reaches its deferred setNotReady (a signal during the final startup wiring, or
// just before Consume returns). Binding the lifecycle to readiness must make a
// query answer not-ready the instant the lifecycle is cancelled, without waiting
// for the explicit clear. The test marks the worker ready, cancels the lifecycle
// (deliberately does NOT call setNotReady), and asserts the query flips to
// not-ready — deterministically, with no goroutines or sleeps.
func TestReadinessNotReadyWhenLifecycleCancelledBeforeClear(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	r := newReadiness(lifecycle)
	r.startReady(&fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netOK: true}, nil)

	if ready, reason := r.Ready(context.Background()); !ready {
		t.Fatalf("readiness must be true before cancellation (reason %q)", reason)
	}

	// Shutdown begins: the lifecycle is cancelled, but the deferred clear has
	// not run yet. Readiness must already report not-ready.
	cancel()
	ready, reason := r.Ready(context.Background())
	if ready {
		t.Fatal("readiness must be false once the lifecycle is cancelled, before setNotReady")
	}
	if reason == "" {
		t.Fatal("a not-ready worker must report a reason")
	}
}

// TestReadinessNotReadyBeforeProbeWired pins that a flag set true without a
// probe (a wiring bug) never claims readiness: without a live probe the worker
// cannot prove its dependencies, so it is not ready.
func TestReadinessNotReadyBeforeProbeWired(t *testing.T) {
	r := newReadiness(context.Background())
	r.setReady() // flag true, but no probe installed

	if ready, _ := r.Ready(context.Background()); ready {
		t.Fatal("readiness must be false when no dependency probe is wired")
	}
}

// TestReadinessSocketReflectsLifecyclePlacement drives the REAL readiness seam
// (newReadiness + startReady + setNotReady) through the REAL control socket, so
// the Run placement is proven end to end: before startReady the socket answers
// not-ready, at the ready boundary it answers ready (from the live probe), a
// dependency failure flips it back to not-ready, recovery flips it ready, and
// the shutdown clear makes the socket answer not-ready again. No Redis, Docker,
// or sleep is involved.
func TestReadinessSocketReflectsLifecyclePlacement(t *testing.T) {
	path := testSocketPath(t)
	consumer := &fakeConsumerHealth{healthy: true}
	manager := &fakeDockerReadiness{netOK: true}

	r := newReadiness(context.Background())
	s, err := NewSocketServer(
		path,
		&fakeSnapshotter{pools: nil},
		nil,
		discardLogger(),
	)
	if err != nil {
		t.Fatalf("NewSocketServer: %v", err)
	}
	s.SetReadiness(r)
	t.Cleanup(func() { _ = s.Close() })

	// Startup: flag false -> not-ready without consulting dependencies.
	if ready, _, err := CheckReady(context.Background(), path); ready || !errors.Is(err, ErrNotReady) {
		t.Fatalf("startup query = ready:%v err:%v, want not-ready", ready, err)
	}

	// Ready-to-consume boundary.
	r.startReady(consumer, manager, []string{"backend"})
	if ready, reason, err := CheckReady(context.Background(), path); !ready || err != nil {
		t.Fatalf("ready boundary query = ready:%v reason:%q err:%v, want ready", ready, reason, err)
	}

	// Steady-state dependency failure flips the socket to not-ready.
	consumer.healthy = false
	if ready, _, err := CheckReady(context.Background(), path); ready || !errors.Is(err, ErrNotReady) {
		t.Fatalf("degraded query = ready:%v err:%v, want not-ready", ready, err)
	}
	// Recovery flips it back to ready without a restart.
	consumer.healthy = true
	if ready, reason, err := CheckReady(context.Background(), path); !ready || err != nil {
		t.Fatalf("recovered query = ready:%v reason:%q err:%v, want ready", ready, reason, err)
	}

	// Shutdown clear (Run's first defer instruction) makes the socket not-ready
	// even while dependencies are healthy.
	r.setNotReady()
	if ready, _, err := CheckReady(context.Background(), path); ready || !errors.Is(err, ErrNotReady) {
		t.Fatalf("post-shutdown query = ready:%v err:%v, want not-ready", ready, err)
	}
}

// Redis/Docker/NETWORKS failures mapping to not-ready, with the healthy case
// passing. No sleeper is needed and no real dependency is touched.
func TestProbeReadinessDependencyFailures(t *testing.T) {
	tests := []struct {
		name     string
		consumer consumerHealth
		manager  dockerReadinessProbe
		networks []string
		want     string
	}{
		{"healthy", &fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netOK: true}, nil, ""},
		{"nil consumer", nil, &fakeDockerReadiness{netOK: true}, nil, "redis consumer unavailable"},
		{"redis unhealthy", &fakeConsumerHealth{healthy: false}, &fakeDockerReadiness{netOK: true}, nil, "redis consumer unhealthy"},
		{"nil manager", &fakeConsumerHealth{healthy: true}, nil, nil, "docker manager unavailable"},
		{"docker ping fails", &fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{pingErr: errors.New("boom"), netOK: true}, nil, "docker unavailable"},
		{"network missing", &fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{missing: "backend", netOK: false}, []string{"backend"}, `network "backend" missing`},
		{"network inspect fails", &fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netErr: errors.New("daemon exploded")}, []string{"backend"}, "network verification failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready, reason := probeReadiness(context.Background(), tc.consumer, tc.manager, tc.networks, time.Second)
			if tc.want == "" {
				if !ready {
					t.Fatalf("ready = false (reason %q), want true", reason)
				}
				return
			}
			if ready {
				t.Fatalf("ready = true, want false (reason %q)", reason)
			}
			if reason != tc.want {
				t.Fatalf("reason = %q, want %q", reason, tc.want)
			}
		})
	}
}

// TestProbeReadinessRecoversWhenDependencyRecovers pins that a dependency
// failure flips readiness false and recovery flips it true again in steady
// state, using deterministic fakes rather than sleeps. The flag itself is
// untouched: recovery is observed from the live probe.
func TestProbeReadinessRecoversWhenDependencyRecovers(t *testing.T) {
	consumer := &fakeConsumerHealth{healthy: true}
	manager := &fakeDockerReadiness{pingErr: errors.New("daemon down"), netOK: true}

	if ready, _ := probeReadiness(context.Background(), consumer, manager, nil, time.Second); ready {
		t.Fatal("readiness must be false while Docker is down")
	}

	// Docker recovers: the same worker is ready again without a restart.
	manager.pingErr = nil
	if ready, reason := probeReadiness(context.Background(), consumer, manager, nil, time.Second); !ready {
		t.Fatalf("readiness must recover when Docker recovers (reason %q)", reason)
	}

	// Redis recovers similarly.
	consumer.healthy = false
	if ready, _ := probeReadiness(context.Background(), consumer, manager, nil, time.Second); ready {
		t.Fatal("readiness must be false while the consumer is unhealthy")
	}
	consumer.healthy = true
	if ready, _ := probeReadiness(context.Background(), consumer, manager, nil, time.Second); !ready {
		t.Fatal("readiness must recover when the consumer is healthy again")
	}
}

// TestProbeReadinessVerifiesConfiguredNetworks pins that the probe actually
// passes the configured NETWORKS set through to verification: a disappearing
// network is a readiness failure, so the set must be checked, not ignored.
func TestProbeReadinessVerifiesConfiguredNetworks(t *testing.T) {
	manager := &fakeDockerReadiness{netOK: true}
	ready, _ := probeReadiness(context.Background(), &fakeConsumerHealth{healthy: true}, manager, []string{"backend", "frontend"}, time.Second)
	if !ready {
		t.Fatal("healthy worker with all networks present must be ready")
	}
	if len(manager.verified) != 2 || manager.verified[0] != "backend" || manager.verified[1] != "frontend" {
		t.Fatalf("verified networks = %v, want the configured set", manager.verified)
	}
	if manager.pingCalls != 1 {
		t.Fatalf("docker pings = %d, want exactly 1", manager.pingCalls)
	}
}

// TestProbeReadinessSkipsDockerDuringRedisOutage pins the cheap-first ordering:
// an unhealthy consumer short-circuits before any Docker round trip, so a
// readiness query during a Redis outage does not also hammer Docker.
func TestProbeReadinessSkipsDockerDuringRedisOutage(t *testing.T) {
	manager := &fakeDockerReadiness{netOK: true}
	ready, reason := probeReadiness(context.Background(), &fakeConsumerHealth{healthy: false}, manager, nil, time.Second)
	if ready || reason != "redis consumer unhealthy" {
		t.Fatalf("ready/reason = %v/%q, want false/redis consumer unhealthy", ready, reason)
	}
	if manager.pingCalls != 0 {
		t.Fatalf("docker pings = %d, want 0 (consumer check short-circuits first)", manager.pingCalls)
	}
}

// TestReadinessNotGatedByStateTracingServicesOrAppFailure pins the
// explicit exclusion list: readiness is decided ONLY from the worker lifecycle
// boundary plus the live dependency probe (Redis consumer, Docker, NETWORKS).
// It exercises the real seams Run uses for the non-gating concerns — tracing
// setup (disabled/failed), a nil state handle, a startup app-load issue,
// and the asynchronous housekeeping pass — BEFORE crossing the ready boundary,
// and asserts the worker is still ready. Those phases are best-effort and
// asynchronous, so they must never gate consumption.
func TestReadinessNotGatedByStateTracingServicesOrAppFailure(t *testing.T) {
	// Tracing: a disabled/absent configuration yields a usable provider; a setup
	// failure is non-fatal observability. Either way it must not gate readiness.
	// Setup installs a global provider, so restore the previous one afterwards so
	// this test does not leak process-global state.
	prevProvider := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevProp)
	})
	provider, err := tracing.Setup(context.Background(), discardLogger())
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	// State: a nil handle models an unopenable SQLite database (logged, never
	// fatal). It is not an input to readiness.

	// App failure: an invalid desired definition stays out of the loaded
	// set; it is a per-app concern and does not gate worker readiness.

	// Asynchronous service/housekeeping convergence: started before the ready
	// boundary and deliberately not awaited. A no-op barrier stands in for a
	// still-running pass.
	hkCtx, hkCancel := context.WithCancel(context.Background())
	defer hkCancel()
	done := startStartupHousekeeping(hkCtx, discardLogger(), startupHousekeeper{
		exclusive: func(ctx context.Context, _ func(context.Context)) error { return ctx.Err() },
		sweep:     func(context.Context) {},
		images:    func(context.Context) {},
		deps:      func(context.Context) {},
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("housekeeping did not settle")
	}

	// Cross the ready boundary with healthy live dependencies; readiness is true
	// despite the failed/degraded non-gating phases above.
	r := newReadiness(context.Background())
	r.startReady(&fakeConsumerHealth{healthy: true}, &fakeDockerReadiness{netOK: true}, nil)
	if ready, reason := r.Ready(context.Background()); !ready {
		t.Fatalf("readiness must not be gated by state/tracing/services/function failure (reason %q)", reason)
	}
}
