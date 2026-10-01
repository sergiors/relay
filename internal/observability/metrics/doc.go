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
//     of truth for every recorded value, plus the Snapshot / AppStatsSnapshot
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
//     relay_schedule_publish_failures_total, the bounded-retry counters
//     relay_schedule_publish_retries_total / relay_schedule_publish_exhausted_total,
//     and relay_schedule_catchup_total), plus CounterVecs
//     relay_handler_invocations_total{outcome,app,handler},
//     relay_app_build_failures_total{app}, the per-app operational
//     counters relay_app_events_matched_total{app},
//     relay_function_handler_success_total{app},
//     relay_function_handler_failure_total{app},
//     relay_function_retries_total{app}, relay_function_dlq_total{app},
//     and the warm-container pool acquire/discard/waits CounterVecs below.
//     The four handler-execution counters carry the function_ namespace (they
//     count handler work) but keep the app identity LABEL.
//   - Histograms: relay_handler_duration_seconds{app,handler},
//     relay_app_build_seconds{app},
//     relay_runtime_container_acquire_duration_seconds{app}, and
//     relay_service_reconcile_duration_seconds{app}
//     (prometheus.DefBuckets; all observed in seconds).
//   - Gauges: relay_pending_entries and relay_pending_oldest_age_seconds
//     (Redis backlog depth and age sampled by the stream consumer),
//     relay_buffered_events (the consumer's local in-flight buffer occupancy),
//     relay_in_flight_invocations (the runner's current executing
//     invocation count), and relay_app_status{app,status} (one-hot
//     public lifecycle: exactly one of the closed status set is 1).
//   - Selective operational counters: relay_redis_read_errors_total{operation}
//     (failed Redis reads by the finite operation set; no raw error label) and
//     relay_service_reconciles_total{app,outcome=changed|unchanged|error}
//     (every ServiceReconciler pass, including periodic no-op verifications).
//   - Warm-container pool (runtime, app-scoped):
//     relay_runtime_pool_capacity{app} and
//     relay_runtime_containers{app,state=idle|busy|starting} gauges,
//     relay_runtime_container_acquires_total{app,outcome=warm|cold},
//     relay_runtime_container_discards_total{app,reason}, and
//     relay_runtime_container_waits_total{app} counters, and the
//     successful-acquire histogram
//     relay_runtime_container_acquire_duration_seconds{app}.
//
// The schedule-coordination counters are Prometheus-only: they are deliberately
// NOT wired into the SQLite stats snapshot. The selective metrics added later —
// relay_app_status, relay_redis_read_errors_total, and the
// relay_service_reconcile_* family — are likewise Prometheus-only: a
// current-state gauge and failure/histogram series are scrape-time observations,
// not cumulative Relay totals worth persisting.
//
// Event classification: the three relay_events_* counters form a closed
// partition of the logical incoming events the runner handled
// (received == matched + unmatched). Each logical event is classified exactly
// once across redeliveries and retries, using an atomic per-message claim in
// the invocation-state hash; a handler failure stays "matched". Schedule
// occurrences bypass event matching and are not counted here.
//
// Retries vs DLQ: relay_retries_total counts stream MESSAGE reclaims
// (redeliveries), while relay_function_retries_total counts failed handler
// executions that will be retried ("Handler retries"). relay_dlq_entries_total
// counts successful DLQ entry WRITES to Redis (one per exhausted invocation,
// plus a placeholder per malformed message), while relay_function_dlq_total
// counts invocations that exhausted their retry budget (the exhaustion commit,
// which may precede the write). The two per-family counters answer different
// questions and are deliberately not interchangeable. The function_* family
// counts handler EXECUTION attributed to an app; app lifecycle families
// (relay_app_events_matched_total, relay_app_status) keep the app_ namespace.
//
// Cardinality is bounded: labels are limited to app/handler/outcome plus
// the small closed runtime-pool value sets (state=idle|busy|starting,
// outcome=warm|cold, and the finite discard reasons) plus the closed
// app_status status set and the finite redis read/error operation set,
// which are validated
// low-cardinality identifiers. High-cardinality values such as event IDs,
// message IDs, container IDs, fingerprints, or raw error strings must never be
// used as labels.
//
// Deliberately NOT instrumented (review conclusion):
//   - Build/version info (a relay_build_info{version,commit} gauge or OTel
//     resource attribute): the project has no reliable injected version/commit
//     at build time (no linker-stamped variable is plumbed into the worker), so
//     any such metric would report a fabricated or empty value. It is skipped
//     rather than exposing misleading metadata.
//   - OTel exporter health/callback metrics: the OTel SDK in use exposes no
//     clean per-export success/failure callback (the trace exporter's internals
//     are not a supported observation surface). A metric derived from it would
//     be brittle; skipped.
//
// The discard reason label carries ONLY real, finite teardown causes. Persisted
// per-app discards are one a-causal aggregate, so rather than expose a
// synthetic reason series at startup, the restored total is held in an internal
// per-app baseline (see Registry.restoredDiscards, seeded by
// SeedAppStat) and folded into the cumulative total reported by
// RuntimePoolCounters and AppStatsSnapshot. The baseline is never exposed
// on /metrics and is cleared by every app retirement path.
//
// Single source of truth: stats are accumulated IN MEMORY in this registry —
// the runner, stream consumer, and runtime manager record against it directly,
// and Prometheus /metrics reflects the current values immediately. There is no
// second parallel counter store. The worker periodically snapshots this same
// store into SQLite (a fixed 5-second cadence); SQLite stores snapshots, not
// history. A hard crash loses at most the last unflushed interval of telemetry;
// graceful shutdown performs a final bounded flush.
//
// App lifecycle: app-scoped series (the CounterVecs and HistogramVecs
// in appMetrics) are created lazily on the first observation and deleted
// when the app is removed. Removal happens at two points: RemoveApp
// is invoked from the reconciler's RemoveApp hook at reconciliation time,
// and SweepAppMetrics runs in the worker's stats flush to re-delete any
// series an in-flight invocation may have recreated after removal. Global
// metrics are never deleted — they are process-lifetime totals.
//
// Invariant: metrics are best-effort and must never interfere with event
// processing. Every method on *Registry is safe to call on a nil receiver
// (a no-op), so a missing or broken metrics pipeline never blocks or crashes the
// worker.
package metrics
