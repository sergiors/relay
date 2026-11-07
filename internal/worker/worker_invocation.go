package worker

import (
	"log/slog"

	"github.com/redis/go-redis/v9"

	"relay/internal/config"
	"relay/internal/observability/metrics"
	"relay/internal/runner"
	"relay/internal/secrets"
	"relay/internal/state"
	"relay/internal/stream"
)

// newWorkerRunnerShell constructs the live runner with an EMPTY registry. The
// worker builds this shell before startup preparation so each accepted startup
// generation can be added to the SAME registry atomically with its authoritative
// runtime-selection check (see runtime.Manager.PublishPrepared). A rejected
// generation is never added as available; it is replaced with an unavailable
// placeholder by the startup publication pass.
func newWorkerRunnerShell(
	logger *slog.Logger,
	metricsInstance *metrics.Registry,
) *runner.Runner {
	return runner.NewWithMetrics(nil, logger, metricsInstance)
}

// wireWorkerRunner installs every worker-owned runner setting before the
// consumer can dispatch to it: the stable NODE_ID as the execution-container
// node identity, the shared secrets provider, the handler-timeout cap, and the
// global invocation concurrency bound. It also exposes the live runner to the
// manual-invocation and DLQ-replay socket commands; both setters are safe to
// call after the socket exists because the socket guards its invoker/replayer
// under a mutex, so a command racing the wiring answers unavailable rather than
// observing a torn value. It is separate from construction because the registry
// must exist (and be populated through fenced publication) before consumption
// starts.
func wireWorkerRunner(
	runWorker *runner.Runner,
	cfg config.Config,
	secretProvider secrets.Provider,
	rtSocket *SocketServer,
) {
	// Stamp the relay.node_id label (the node/consumer identity) on every
	// execution container, so all invocations carry it.
	runWorker.SetNodeID(cfg.NodeID)
	// Wire the shared secrets provider; a missing secret surfaces per-invocation,
	// never at startup.
	runWorker.SetSecretProvider(secretProvider)
	// Cap every rule's handler timeout at the value template validation enforces
	// (app.MaxTimeout). Defense in depth: a misconfigured or hot-swapped
	// template can never run a handler past the cap.
	runWorker.SetMaxHandlerTimeout(stream.MaxRuleTimeout)
	// Bound the number of invocations executing concurrently
	// (MAX_CONCURRENT_INVOCATIONS); a value < 1 falls back to the runner's
	// default.
	runWorker.SetMaxConcurrentInvocations(cfg.MaxConcurrentInvocations)

	// Expose the live runner to the manual-invocation socket command. It was
	// wired after the socket was created (the runner is built later, once images
	// and services are prepared), so a `relay app invoke` that races the
	// wiring answers invoke_unavailable rather than a torn value. This is what
	// lets the CLI run handlers against the live runtime pool without ever
	// instantiating Docker/runtime in the CLI process. The same runner also
	// serves the `relay dlq replay` semantic command, which re-executes one DLQ
	// entry's exact recorded app/handler once against the current registry.
	rtSocket.SetInvoker(runWorker)
	rtSocket.SetReplayer(runWorker)
}

// newStreamConsumer constructs the Redis stream consumer after the runner so
// its ScheduleRunner seam can route schedule occurrences directly to the
// runner, bypassing event matching.
func newStreamConsumer(
	client *redis.Client,
	logger *slog.Logger,
	cfg config.Config,
	metricsInstance *metrics.Registry,
	st *state.State,
	runWorker *runner.Runner,
) *stream.Consumer {
	return stream.NewConsumer(stream.ConsumerConfig{
		Client:            client,
		Stream:            cfg.RedisStream,
		Group:             cfg.RedisGroup,
		Consumer:          cfg.NodeID,
		Log:               logger,
		Metrics:           metricsInstance,
		MaxBufferedEvents: cfg.MaxBufferedEvents,
		MaxEventBytes:     cfg.MaxEventBytes,
		// PostgreSQL is the durable DLQ store. An entry is written before the
		// source delivery is acknowledged.
		DLQStore: st,
		// Terminal invocation-state retention (cfg.InvocationRetention from
		// REDIS_INVOCATION_RETENTION, default 48h; unset/empty/zero/negative
		// behavior is resolved by config.Load). Passed as an explicit pointer so
		// an operator's disable (0) is honored verbatim and never defaulted back
		// to 48h by the stream package. It is the TTL the consumer applies to a
		// message's invocation-state hash only AFTER it leaves the PEL; while
		// recoverable the hash stays persistent.
		InvocationRetention: &cfg.InvocationRetention,
		// Schedule occurrences route directly to the runner, bypassing event
		// matching; the runner validates the occurrence against the current
		// template and resolves the handler timeout from it, and stamps the
		// message's real Redis stream ID.
		ScheduleRunner: runWorker.InvokeOccurrence,
	})
}
