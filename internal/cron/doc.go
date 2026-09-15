// Package cron owns the WHEN of a function template's schedules: it maps each
// cron schedule into a gocron/v2 job that, when due, hands the tick to the
// schedule package's Publisher for cluster-wide publication. It is a thin
// wrapper around gocron; it knows nothing about Docker or Redis.
//
// Execution model: the event path is `Redis -> matcher -> handler -> runner`;
// the schedule path is `gocron -> cron tick -> Publisher -> Redis stream ->
// consumer -> runner`. The cron package only decides when an occurrence is due
// and publishes the tick; every worker evaluates the same cron locally, so they
// contend on one atomic publish-if-new (see internal/schedule) and the stream's
// consumer group delivers the single entry to exactly one worker. Both funnel
// into the same runner boundary (runner.InvokeHandler), so scheduled executions
// reuse the runtime's image lifecycle, secrets, timeout cap, concurrency slots,
// and metrics.
//
// Lifecycle: New constructs the scheduler (no goroutines yet); ReplaceFunction /
// RemoveFunction register or converge a function's jobs by tag; Start begins
// firing; Stop performs a bounded graceful shutdown. ReplaceFunction is the
// single reconcile entry point: it removes and re-creates a function's jobs so
// added/changed/removed schedules converge, and a skipped tick during the swap
// is acceptable.
//
// Timezone semantics: each schedule carries an effective IANA timezone. The
// cron scheduler is pinned to UTC and the per-job timezone is encoded into the
// cron spec as a `CRON_TZ=<zone>` prefix, so every computed next-run instant is
// a UTC instant. DST and offset changes are delegated to Go's time.Location and
// the cron parser — the cron scheduler itself never does timezone math.
//
// The publisher interface (Publisher.PublishOccurrence) is the only seam out
// of this package: the cron scheduler never touches Redis or the invocation
// internals. It fires each due tick to the Publisher, which owns occurrence
// identity and deduplicated publication; distribution is therefore best-effort
// across the fleet, because every worker fires the same tick and the atomic
// publish-if-new admits exactly one entry cluster-wide.
package cron
