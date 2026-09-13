// Package metrics provides Relay's operational metrics, backed by a dedicated
// Prometheus registry (github.com/prometheus/client_golang/prometheus). It is
// the single observability surface the runner, stream consumer, and runtime
// manager record against; the worker exposes it as Prometheus text format on
// /metrics, but only when METRICS_ADDR is set to a non-empty listen address.
// Unset or empty disables the HTTP endpoint entirely.
//
// Structure:
//
//   - Registry (metrics.go) is the nil-safe in-memory store — the single source
//     of truth for every recorded value, plus the Snapshot / FunctionStatsSnapshot
//     gather routines and the Prometheus exposition Handler (a pure promhttp
//     wrap, with no routing). It contains no lifecycle or network code.
//   - Server (server.go) owns the net/http /metrics lifecycle and routing: a
//     ServeMux that serves only `GET /metrics` with the exposition handler,
//     Start binds synchronously (fail-fast on a taken port) and serves in the
//     background; Stop performs a bounded graceful shutdown.
//   - Refresher (refresh.go) runs a periodic ticker that drives GaugeSource
//     implementations — external-lookup samplers that set point-in-time gauges
//     (e.g. the stream consumer's XPENDING depth).
//   - MetricsLogger (logger.go) runs a periodic ticker that logs the registry's
//     snapshot as logfmt lines, reading the same registry the Server serves on
//     /metrics.
//
// Metric kinds:
//
//   - Counters: events_received_total, events_processed_total, retries_total,
//     dlq_entries_total, handler_success_total, handler_failure_total, plus
//     CounterVecs handler_invocations_total{outcome,function,handler} and
//     build_failures_total{function}.
//   - Histograms: handler_duration_seconds{function,handler} and
//     function_build_seconds{function} (prometheus.DefBuckets).
//   - Gauges: pending_entries and pending_oldest_age_seconds (Redis backlog
//     depth and age sampled by the stream consumer).
//
// Cardinality is bounded: labels are limited to function/handler/outcome, which
// are validated low-cardinality identifiers. High-cardinality values such as
// event IDs, message IDs, container IDs, or fingerprints must never be used as
// labels.
//
// Single source of truth: stats are accumulated IN MEMORY in this registry —
// the runner, stream consumer, and runtime manager record against it directly,
// and Prometheus /metrics reflects the current values immediately. There is no
// second parallel counter store. The worker periodically snapshots this same
// store into SQLite (a fixed 5-second cadence); SQLite stores snapshots, not
// history. A hard crash loses at most the last unflushed interval of telemetry;
// graceful shutdown performs a final bounded flush.
//
// Function lifecycle: function-scoped series (the CounterVecs and HistogramVecs
// in functionMetrics) are created lazily on the first observation and deleted
// when the function is removed. Removal happens at two points: RemoveFunction
// is invoked from the reconciler's RemoveFunction hook at reconciliation time,
// and SweepFunctionMetrics runs in the worker's stats flush to re-delete any
// series an in-flight invocation may have recreated after removal. Global
// metrics are never deleted — they are process-lifetime totals.
//
// Invariant: metrics are best-effort and must never interfere with event
// processing. Every method on *Registry is safe to call on a nil receiver
// (a no-op), so a missing or broken metrics pipeline never blocks or crashes the
// worker.
package metrics
