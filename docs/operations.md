# Operations

This guide covers running Relay, monitoring it, and handling recovery. For
environment variables, see [Configuration](configuration.md); for commands, see
[CLI](cli.md).

## Runtime lifecycle

`relay start` runs in the foreground. Use Docker, systemd, Kubernetes, or another
process manager to supervise it.

At startup Relay requires Redis, PostgreSQL, Docker, and any configured Docker
networks. It loads apps from `/apps`, prepares their runtimes, then starts event
consumption, schedules, and service reconciliation. A failed required dependency
or network check stops startup. Changes under `/apps` are applied live; a failed
app update leaves its previous working version available.

Shutdown is graceful and ordered. Relay stops accepting work and waits for
operations that still use shared dependencies before closing them. A stuck
dependency-using operation can extend shutdown beyond the cleanup timeout rather
than allowing its dependencies to close underneath it.

### Node ownership loss

Each concurrent worker must have a unique, stable `NODE_ID`. If Relay can no
longer verify ownership of that identity, it clears readiness, stops its
lifecycle, and skips cleanup that could affect a replacement worker. The process
exits abnormally so its supervisor can restart or alert. Detection is periodic,
so it is not instantaneous. See [Configuration](configuration.md#required).

## State paths and persistence

Relay uses these locations:

- `/apps`: authoritative app configuration and source.
- `/var/lib/relay`: persistent local secrets and Git configuration/checkout.
- `/run/relay`: process lock and worker control socket; not persistent state.
- PostgreSQL (`DATABASE_URL`): app status view, node statistics, and the durable
  schedule publication outbox.

PostgreSQL is required at worker startup, but `/apps` remains the source of truth
for matching, builds, and reconciliation. Replacing the database removes its
state view, statistics, and pending schedule publication intents; it does not
remove `/apps` or local secret files. Post-start state-view write failures do
not stop event processing.

When multiple Relay processes share a Docker daemon, they must use the same
PostgreSQL database. It coordinates shared dependency-image use and cleanup;
separate state databases on one daemon are unsupported.

### Runtime image selection state

PostgreSQL also holds the `runtime_image_selections` table. It is keyed by runtime
ID and records a pinned configured source image reference, a multi-platform index
digest (`sha256:<64 lowercase hex>`), a monotonically increasing generation, and
the selection time. A missing row means the runtime is **unpinned** and keeps its
historical configured base-image behavior exactly; existing deployments therefore
do not rebuild images.

Workers read this table at startup, before app preparation, and refresh the
complete snapshot once per periodic reconciliation pass (the default cadence is
30 seconds). A pin is used only as `FROM <configured-ref>@<digest>`; Relay never
resolves the registry or a child manifest digest. A changed pin invalidates only
the runtime-backed apps using that runtime, which reprepare without a worker
restart. A read or validation failure keeps the last-known-good snapshot and is
retried on the next pass. In particular, a row whose configured source image does
not match the runtime's built-in base reference is rejected rather than applied;
correct the row to let that runtime converge.

Publication is fenced: a transaction-scoped PostgreSQL advisory lock keyed by
runtime ID is shared by selection CAS writes and by the authoritative
check-plus-registry-swap. This includes the initial startup registry population,
so a pin cannot change during startup preparation and have a stale image
published or warmed. If a pin changes while an image is being built, that stale
generation is discarded (its preparation and publication leases are released),
the old active version keeps serving (or the app is represented by an unavailable
placeholder that still matches events), and a refreshed reconcile retries against
the current pin. The current CLI has no selection writer. Runtimes without a row
remain unpinned.

## Health and statistics

`relay health` reports the running worker's readiness and live Redis, Docker,
and configured-network health. It requires a running worker. PostgreSQL scheduler
degradation, tracing, app-level status, and service convergence do not by
themselves make the worker unready. There is no HTTP health endpoint.

`relay stats` reads persisted counters from PostgreSQL and works without a
running worker. Its snapshot may lag live metrics by about five seconds. Backlog
gauges show the Redis consumer group's pending entries and oldest pending age.
`relay stats reset` resets cumulative totals, not backlog gauges or Prometheus
counters.

## Observability

### Metrics

Set `METRICS_ADDR` to expose Prometheus metrics at `/metrics`. Useful metrics
include:

- `relay_events_received_total`, `relay_events_matched_total`, and
  `relay_events_unmatched_total` for external event classification.
- `relay_handler_success_total`, `relay_handler_failure_total`,
  `relay_function_retries_total`, and `relay_dlq_entries_total` for handler and
  DLQ activity. Stream reclaims and handler retries are different counts.
- `relay_pending_entries`, `relay_pending_oldest_age_seconds`,
  `relay_buffered_events`, and `relay_in_flight_invocations` for workload and
  backlog.
- `relay_schedule_occurrences_published_total`,
  `relay_schedule_occurrences_duplicate_total`, and
  `relay_schedule_pending_expired_total` for schedule publication.
- `relay_app_status` and `relay_service_reconciles_total` for app and service
  health.

Schedule occurrences bypass event classification, but share execution, retry,
recovery, and DLQ handling with events. Metric labels use bounded app/handler
and outcome values; message IDs and raw errors are not labels.

### Tracing

Tracing is opt-in through OTLP environment variables. Relay propagates W3C
TraceContext and Baggage through stream messages and handler invocations. A
tracing setup error is non-fatal; Relay continues without tracing. See
[Configuration](configuration.md#environment-owned-by-other-libraries).

### Logging

`LOG_LEVEL` controls Relay's logs. Handler stdout and stderr are forwarded
regardless of that level. Treat handler output as application data and avoid
printing secrets.

## Secrets

Secrets are stored as local files under `/var/lib/relay/secrets` and referenced
by name in app templates. Values are not stored in PostgreSQL, images, labels,
or Relay logs, and `relay app inspect` shows references only. Rotations apply to
the next function invocation or service reconciliation.

Anyone with Docker daemon access can inspect container environments and mounted
data. Treat Docker access as access to Relay's secrets. External secret providers
are not currently implemented.

## Git-managed apps

Git synchronization is manual by default; `relay start` does not poll or fetch.
For example:

```sh
relay git keygen
relay git set git@github.com:acme/repo.git --ref main --path apps
relay git sync
relay git status
```

Relay accepts SSH repository URLs. `git sync` makes `/apps` match the configured
repository and path, including removing apps no longer present. Review the
source and selected path before syncing. Updates are staged before live changes;
each app is replaced atomically, but a multi-app sync is not one atomic
repository-wide change. An optional GitHub push webhook can request a sync; see
[CLI](cli.md#relay-git).

Relay verifies SSH host keys using its managed `known_hosts` file and does not
silently replace a changed host key.

## Redis recovery and reliability

Messages that are not acknowledged remain pending in the Redis consumer group
and are eligible for reclaim. A reclaim is a recovery mechanism, not the handler
retry timer. Invocation state protects retries and stale claims; transport or
state errors leave work pending rather than acknowledging unresolved work.

Relay writes required PostgreSQL DLQ rows before acknowledging a source message.
It marks those rows settled only after ACK is confirmed (or recovery verifies
the delivery is no longer pending). See [Events](events.md#execution-retries-and-delivery)
and [Schedules](schedules.md#recovery-and-availability) for workload-specific
behavior.

## DLQ controls

The DLQ is a durable PostgreSQL table. `relay dlq inspect` shows the original
message value; replay uses the separately stored logical handler payload.
Malformed and oversized messages are not replayable. Only settled entries may
be removed or expired. Replay requires a running worker and is at least once, so
handlers should be idempotent. See [CLI](cli.md#relay-dlq).

## Reliability model and limitations

- Event and schedule delivery is at least once, not exactly once. A handler can
  run again after a crash or ambiguous completion.
- Event retry count is configured per rule; backoff and reclaim timing are not
  configurable. See [Events](events.md#execution-retries-and-delivery).
- Schedule publication is deduplicated cluster-wide. Handler execution remains
  at least once, and outbox recovery is bounded. See
  [Schedules](schedules.md#recovery-and-availability).
- Resource limits apply per container. Increasing concurrency or service
  replicas increases aggregate resource use.
- Workload network policy is not implemented. Relay verifies configured Docker
  networks but does not create or remove them.
