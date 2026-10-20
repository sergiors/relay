package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/codes"

	"relay/internal/config"
	gitwh "relay/internal/git/webhook"
	"relay/internal/observability/metrics"
	"relay/internal/observability/tracing"
	"relay/internal/runtime"
)

// startRuntimeSocket opens the worker's live runtime-pool query socket, wires
// the readiness checker, and registers its best-effort shutdown step. The
// socket answers the CLI's live gauges, `reset stats`, manual invocation, DLQ
// replay, and `relay health`; cumulative counters stay in PostgreSQL.
//
// The parent process lock is already held by `relay start`, so removing a stale
// socket can never delete an active worker's socket. A bind failure is fatal,
// matching the metrics and webhook servers. Readiness is wired before the
// socket serves a request; its live dependency probe is installed later at the
// ready-to-consume boundary, so until then the checker reports not-ready.
func startRuntimeSocket(
	startupCtx context.Context,
	logger *slog.Logger,
	manager *runtime.Manager,
	statsFlusher *statsFlusher,
	workerReady *readiness,
	shutdown *shutdownRegistry,
) (*SocketServer, error) {
	_, socketSpan := tracing.Start(startupCtx, "runtime.socket")
	rtSocket, err := NewSocketServer(SocketPath, manager, statsFlusher, logger)
	if err != nil {
		socketSpan.RecordError(err)
		socketSpan.SetStatus(codes.Error, err.Error())
		socketSpan.End()
		return nil, fmt.Errorf("runtime state socket: start failed: %w", err)
	}
	socketSpan.End()
	rtSocket.SetReadiness(workerReady)
	// rtSocket.Close takes no context; the shutdown registry bounds its wait.
	shutdown.register(shutdownStep{
		name:    shutdownStepSocket,
		timeout: 5 * time.Second,
		run:     func(context.Context) error { return rtSocket.Close() },
	})
	logger.Info("Runtime state socket listening", "path", SocketPath)
	return rtSocket, nil
}

// startMetricsServer starts the optional metrics components created by
// setupMetrics and registers the HTTP server's shutdown step. It returns the
// metrics-logger done channel so the loops barrier can join that goroutine; the
// channel is nil when METRICS_ADDR is unset (metricsServer is nil).
//
// The snapshot logger and the /metrics HTTP server are BOTH opt-in: the
// registry always exists for the persistent stats flush, but with exposition
// disabled there is no server to bind (and the periodic logger would only
// duplicate the stats it already persists). The logger runs in its own
// goroutine (exits on ctx); the server binds synchronously — a bind failure
// (a taken metrics port) is a config error that must surface now, and is FATAL.
// The done channel is joined by the loops shutdown step before the state DB and
// Redis close, so no logger tick can touch a closed resource.
func startMetricsServer(
	ctx context.Context,
	logger *slog.Logger,
	cfg config.Config,
	registry *metrics.Registry,
	server *metrics.Server,
	shutdown *shutdownRegistry,
) (<-chan struct{}, error) {
	if server == nil {
		return nil, nil
	}

	metricsLogger := metrics.NewMetricsLogger(
		registry,
		metrics.DefaultLogInterval,
		func(format string, args ...any) {
			logger.Debug(fmt.Sprintf(format, args...))
		},
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		metricsLogger.Start(ctx)
	}()

	if err := server.Start(); err != nil {
		// A bind failure (taken metrics port) is a config error that must
		// surface at startup, not retry invisibly. The deferred cleanup stops
		// the metrics logger goroutine via the cancelled lifecycle.
		return nil, fmt.Errorf("metrics server: start failed: %w", err)
	}
	shutdown.register(shutdownStep{
		name:    shutdownStepMetrics,
		timeout: shutdownStepTimeout,
		run:     server.Stop,
	})
	logger.Info("Metrics http server listening", "addr", cfg.MetricsAddr)
	return done, nil
}

// startWebhookServer starts the Git webhook server right after metrics. It
// binds synchronously and fails fast on a taken/unparseable GIT_WEBHOOK_ADDR,
// matching metrics: a webhook port conflict must surface at startup. A nil
// server means the webhook was disabled, so there is nothing to start. The
// teardown step is registered only once it has actually started.
func startWebhookServer(
	logger *slog.Logger,
	cfg config.Config,
	server *gitwh.Server,
	shutdown *shutdownRegistry,
) error {
	if server == nil {
		return nil
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("git webhook server: start failed: %w", err)
	}
	shutdown.register(shutdownStep{
		name:    shutdownStepWebhook,
		timeout: shutdownStepTimeout,
		run:     server.Stop,
	})
	logger.Info("Webhook http server listening", "addr", cfg.GitWebhookAddr)
	return nil
}
