# Operations

This guide covers running Relay, monitoring it, and handling recovery. For
environment variables, see [Configuration](configuration.md); for commands, see
[CLI](cli.md).

## Runtime lifecycle

`relay start` runs in the foreground. Use Docker, systemd, Kubernetes, or another
process manager to supervise it.

At startup Relay requires Redis, PostgreSQL, Docker, and any configured
`NETWORKS` networks. A missing dependency or configured network stops startup.
See [Configuration](configuration.md) for network settings.

Relay and Docker Engine must have a working route for handler execution.
Host-based or remote-engine deployments without that route are unsupported.

Docker access is highly privileged, even with a read-only socket mount, and
workload networking does not isolate apps from a compromised Relay process. See
[Installation](installation.md#configuration) for Docker access requirements.

Changes under `/apps` are applied live; a failed app update leaves its previous
working version available.

Shutdown is graceful and ordered. Relay stops accepting work and waits for
operations that still use shared dependencies before closing them. A stuck
dependency-using operation can extend shutdown beyond the cleanup timeout rather
than allowing its dependencies to close underneath it.

### Node identity loss

Each concurrent worker must have a unique, stable `NODE_ID`. If a worker loses
that identity, it becomes unready and exits so its supervisor can restart or
alert. Detection is periodic, so it is not instantaneous. See
[Configuration](configuration.md#required).

## State paths and persistence

Relay uses these locations:

- `/apps`: authoritative app configuration and source.
- `/var/lib/relay`: persistent local secrets and Git configuration/checkout.
- `/run/relay`: temporary runtime files; not persistent state.
- PostgreSQL (`DATABASE_URL`): app status, node statistics, and pending
  scheduled occurrences.

PostgreSQL is required at worker startup, but `/apps` remains the source of truth
for matching, builds, and reconciliation. Replacing the database removes its
state view, statistics, and unpublished scheduled occurrences; it does not
remove `/apps` or local secret files. Post-start state-view write failures do
not stop event processing.

When multiple Relay processes share a Docker daemon, they must use the same
PostgreSQL database. Separate state databases on one daemon are unsupported.

### Managed runtime images

Managed runtimes use their configured base image by default. Operators can pin
that configured image to a registry digest with `relay runtime update`; use
`relay runtime check` to compare a selection with the current registry image.
Updates resolve the configured tag and require a multi-platform image index.
They do not pull or build images, restart the worker, or deploy apps. Running
workers pick up a changed selection during reconciliation and reprepare only the
apps using that runtime; a worker that is not running reads the selection at
startup.

`check` and `update` need Docker Engine access and registry credentials. See
[CLI](cli.md#relay-runtime) for command behavior and credential setup.

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

`LOG_LEVEL` controls Relay's logs. Handler stdout and stderr are available
through native Docker container logs. Runtime bootstraps write `INIT_START`
once per process and `START`/`END` records with the invocation request ID.
These lifecycle records do not alter or enrich handler output. An `END` record
may be absent when an invocation is interrupted or its process is terminated.
Treat handler output as application data and avoid printing secrets.

## Secrets

The default `local` provider reads files under `/var/lib/relay/secrets`. The
optional Infisical provider uses Universal Auth with a Machine Identity and
resolves the same provider-neutral secret references used by app templates. See
[Installation](installation.md#infisical-provider) for setup and
[Configuration](configuration.md) for its environment variables.

Relay validates Infisical configuration at startup but does not contact
Infisical until a secret is needed. Each worker has its own provider instance,
access token, and five-minute cache. A value is refreshed after its cache entry
expires; an expired value is never served as a fallback. Rotation is therefore
visible on a subsequent resolution after cache expiry, not immediately. If
Infisical is unavailable or rejects the request, secret resolution fails closed:
functions do not run without the value, and service reconciliation preserves a
healthy existing container when replacement cannot resolve its environment.
Workers may observe a rotation at different times within that cache window.

Use one shared Machine Identity for the Relay workers, with read-only access
limited to the required Infisical project, environment, and secret path. Do not
grant write or broader project access. Supply its credentials only to Relay;
Relay does not put them in app environments, images, state, or logs. Resolved
values are not stored in PostgreSQL, images, or Relay logs, and
`relay app inspect` shows references only. See
[Apps](apps.md#environment-and-secrets) for workload-specific exposure,
including service environment variables.

`relay secret ls` lists names from the single active provider: local files by
default, or Infisical when configured. It does not display secret values.
`relay secret set` and `relay secret rm` operate only on local files and fail
when Infisical is active; manage Infisical secrets in Infisical. Relay uses one
provider at a time and does not fall back between local files and Infisical.
Anyone with Docker daemon access can inspect container environments and mounted
data. Treat Docker access as access to Relay's secrets.

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
and are eligible for reclaim. Reclaim is a recovery mechanism, not the handler
retry timer. Transport or state errors leave unresolved work pending rather than
acknowledging it.

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
  at least once, and publication recovery is bounded. See
  [Schedules](schedules.md#recovery-and-availability).
- Resource limits apply per container. Increasing concurrency or service
  replicas increases aggregate resource use.
- Workload network policy is not implemented. Relay verifies configured Docker
  networks but does not create or remove them.
