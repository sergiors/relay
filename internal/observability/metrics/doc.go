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
// Metric kinds (canonical Prometheus names carry the "relay_" namespace
// prefix; log snapshots render without it). Every family is registered with a
// non-empty sentence-case HELP in metricHelp (metrics.go), and the metadata
// test gathers all of them to enforce that:
//
//   - Counters: relay_events_received_total, relay_events_matched_total,
//     relay_events_unmatched_total, relay_retries_total,
//     relay_dlq_entries_total, relay_handler_success_total,
//     relay_handler_failure_total, relay_concurrency_waits_total, the
//     schedule-coordination counters (relay_schedule_occurrences_published_total,
//     relay_schedule_occurrences_duplicate_total,
//     relay_schedule_publish_failures_total), plus CounterVecs
//     relay_handler_invocations_total{outcome,function,handler},
//     relay_build_failures_total{function}, the per-function operational
//     counters relay_function_events_matched_total{function},
//     relay_function_handler_success_total{function},
//     relay_function_handler_failure_total{function},
//     relay_function_retries_total{function}, relay_function_dlq_total{function},
//     and the warm-container pool acquire/discard/waits CounterVecs below.
//   - Histograms: relay_handler_duration_seconds{function,handler},
//     relay_function_build_seconds{function}, and
//     relay_runtime_container_acquire_duration_seconds{function}
//     (prometheus.DefBuckets; all observed in seconds).
//   - Gauges: relay_pending_entries and relay_pending_oldest_age_seconds
//     (Redis backlog depth and age sampled by the stream consumer),
//     relay_buffered_events (the consumer's local in-flight buffer occupancy),
//     and relay_in_flight_invocations (the runner's current executing
//     invocation count).
//   - Warm-container pool (runtime, function-scoped):
//     relay_runtime_pool_capacity{function} and
//     relay_runtime_containers{function,state=idle|busy|starting} gauges,
//     relay_runtime_container_acquires_total{function,outcome=warm|cold},
//     relay_runtime_container_discards_total{function,reason}, and
//     relay_runtime_container_waits_total{function} counters, and the
//     successful-acquire histogram
//     relay_runtime_container_acquire_duration_seconds{function}.
//
// The schedule-coordination counters are Prometheus-only: they are deliberately
// NOT wired into the SQLite stats snapshot.
//
// Event classification: the three relay_events_* counters form a closed
// partition of the logical incoming events the runner handled
// (received == matched + unmatched). Each logical event is classified exactly
// once across redeliveries and retries, using an atomic per-message claim in
// the invocation-state hash; a handler failure stays "matched". Schedule
// occurrences bypass event matching and are not counted here.
//
// Cardinality is bounded: labels are limited to function/handler/outcome plus
// the small closed runtime-pool value sets (state=idle|busy|starting,
// outcome=warm|cold, and the finite discard reasons), which are validated
// low-cardinality identifiers. High-cardinality values such as event IDs,
// message IDs, container IDs, or fingerprints must never be used as labels.
//
// The discard reason label carries ONLY real, finite teardown causes. Persisted
// per-function discards are one a-causal aggregate, so rather than expose a
// synthetic reason series at startup, the restored total is held in an internal
// per-function baseline (see Registry.restoredDiscards, seeded by
// SeedFunctionStat) and folded into the cumulative total reported by
// RuntimePoolCounters and FunctionStatsSnapshot. The baseline is never exposed
// on /metrics and is cleared by every function retirement path.
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
