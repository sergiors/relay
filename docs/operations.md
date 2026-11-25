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
running worker. Bare `relay stats` shows the current node's snapshot;
`--node <id>` selects one explicit node and `--all` aggregates every node.
See the [CLI guide](cli.md#relay-stats) for selector details.
The snapshot may lag live metrics by about five seconds. Backlog gauges show
the Redis consumer group's pending entries and oldest pending age.

`relay stats reset` resets the current node's cumulative totals;
`stats reset --all` resets every node's. Neither form resets backlog gauges
or Prometheus counters.

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
- `relay_consumer_group_recoveries_total` for lost-group recovery attempts,
  labeled `created`, `existing`, or `failed`. A `created`/`existing` result means
  the stream backlog was replayed.
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

`LOG_LEVEL` controls Relay worker logs, not handler logs or output. Handler
stdout and stderr are available through native Docker container logs. Python
standard `logging` records are written to stderr as timestamp, level,
`[logger]`, and message. Python keeps its default `WARNING` threshold unless
the application configures logging. The bootstrap installs a default handler
before loading app handlers, so a later bare `logging.basicConfig()` will not
replace it; apps should explicitly configure or add handlers, or use
`dictConfig`, to customize logging. Node `console` records are not filtered:
`console.log`/`info` use a UTC ISO timestamp and `INFO` prefix on stdout;
`console.debug` uses `DEBUG` on stdout; `console.warn` and `console.error` use
`WARN` and `ERROR` prefixes on stderr.
Runtime bootstraps write `INIT_START` once per process and `START`/`END` lines
with the invocation request ID. Completed successful and handled-failure
invocations also emit a `REPORT` line after `END`, with duration in milliseconds
and available memory values. Memory values are cgroup/container-wide;
`Container Memory Peak` covers the container's lifetime since creation and may
include warm-reuse history. Unavailable memory fields are omitted. Interrupted
invocations produce neither `END` nor `REPORT`. These lifecycle lines and direct
writes to stdout or stderr remain plain output: lifecycle records are single
lines without runtime console/logging prefixes, and direct writes remain
unformatted. Lifecycle lines do not alter or enrich handler output. Treat handler
output as application data and avoid printing secrets.

Example: `REPORT RequestId: abc Duration: 12.34 ms Memory Used: 45 MiB Container Memory Peak: 78 MiB`

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

Configure an SSH repository as the app source:

```sh
relay git keygen
relay git set git@github.com:acme/repo.git --ref main --path apps
relay git sync
```

Add the public key from `git keygen` to the repository as a read-only deploy key.
`git set` saves the SSH repository and optional ref, app path, and webhook secret;
it does not deploy apps. Run `git sync` to deploy the latest revision from the
configured ref to `/apps`. It removes apps no longer present in the selected
source, so review the repository and path before syncing.
Changing or removing the Git source on a running worker requires restarting it.

Relay verifies SSH host keys using its managed `known_hosts` file and does not
silently replace a changed host key.

Git updates are manual unless you configure a GitHub push webhook. Configure a
webhook secret with `--webhook-secret NAME` and expose the webhook listener using
`GIT_WEBHOOK_ADDR`; the production Compose file does not configure or expose that
endpoint. A webhook requests deployment of the revision in the push.

When a Git source is configured, nodes using the same repository, ref, and path
receive the deployed revision. Give each node access to the repository with its
own read-only deploy key. Nodes can fall behind while offline or while fetching
or activating an update. A failed update leaves the previous working apps active;
Relay retries, and the node catches up when it is available and the problem is
resolved.

At startup Relay verifies that `/apps` matches the last successfully
materialized revision. If an authorized app is missing or its content or
template identity no longer matches, the node refuses to consume events rather
than expose an incomplete registry. Otherwise, an event for that app could
appear unmatched and be acknowledged before its handler is restored. Relay exits
on this authorization inconsistency; it does not confuse it with an image-build
failure. After authorization succeeds, a build failure keeps the app registered
as unavailable, so matching events remain pending rather than being acknowledged.

Resolve the reported source or materialization problem. Then restart the node or
let its supervisor restart it; the next startup attempts to materialize the
published revision and verifies the resulting app set again. The production
Compose service uses `restart: unless-stopped`; configure an equivalent restart
policy with other supervisors. If app source content changes after verification
but before preparation completes, Relay rejects the changed generation and keeps
the app registered as unavailable; matching events remain pending while Relay
reconciles the authorized revision. A node with no previously deployed revision
also waits for its first successful deploy instead of starting with no apps.

Check the desired revision and node/app progress with:

```sh
relay git sync
relay deploy status
```

`relay deploy status` shows the desired generation and commit, whether it is
pinned, and each node's desired and current revision plus app status. A node
that is behind continues serving its previous apps; check its status and error
while it retries.

To pause updates, pin the current revision:

```sh
relay deploy pin
```

While pinned, `git sync` and webhook updates are blocked. To deploy a particular
commit from the configured repository, use `rollback`; this also pins the
selected revision:

```sh
relay deploy rollback 0123456789abcdef0123456789abcdef01234567
relay deploy unpin
```

`unpin` allows a future sync or webhook update; it does not immediately change
the current revision. Without a configured Git source, apps run from local
`/apps`. With Git configured, local `/apps` edits are not deployed and Relay
restores the selected Git revision.

## Redis recovery and reliability

Messages that are not acknowledged remain pending in the Redis consumer group
and are eligible for reclaim. Reclaim is a recovery mechanism, not the handler
retry timer. Transport or state errors leave unresolved work pending rather than
acknowledging it.

If Relay observes that its consumer group or the stream is missing, it recreates
the group at ID 0 and replays the entries still present in the stream. This is
deliberate at-least-once recovery, not a silent reset: the previous pending list
is gone, so already-read messages can run again, and entries whose bodies were
already trimmed cannot be recovered. Relay logs the recovery, reports not ready
while it is outstanding, and resumes normal readiness only after a successful
read. A group that another node already recreated is reused (Redis reports it as
existing); Relay never recreates a group for an ordinary Redis outage and does
not take a cluster-wide lock.

Relay writes required PostgreSQL DLQ rows before acknowledging a source message.
It marks those rows settled only when Redis proves the exact source delivery is
settled: the message left the group's pending list because the group had already
passed it. Rows are preserved while the source is still pending, not yet read by
a recreated group, or unknown. Redis and PostgreSQL cannot be updated atomically,
so a group recreation immediately after a settled observation can replay the
source; that replay is at least once and re-inserts the same idempotent row. See
[Events](events.md#execution-retries-and-delivery) and
[Schedules](schedules.md#recovery-and-availability) for workload-specific
behavior.

When a DLQ source body is gone, settlement requires an ACK receipt retained
with invocation state for `REDIS_INVOCATION_RETENTION` (48h by default). If that
receipt expires before reconciliation, the delivery remains unknown and its DLQ
row stays unsettled indefinitely; `DLQ_RETENTION` cannot remove unsettled rows.
Choose invocation retention long enough to cover expected reconciliation
outages.

Source-stream retention (`REDIS_STREAM_RETENTION`) trims only entries every
consumer group has acknowledged. Relay checks that its own group still exists
and issues the trim in one atomic step; while the group is absent it skips trims
and keeps checking instead of trimming unprotected or recreating the group.

## DLQ controls

The DLQ is a durable PostgreSQL table. `relay dlq inspect` shows the original
message value; replay uses the separately stored logical handler payload.
Malformed and oversized messages are not replayable. Only settled entries may
be removed or expired. Replay requires a running worker and is at least once, so
handlers should be idempotent. See [CLI](cli.md#relay-dlq).

## Reliability model and limitations

- Event and schedule delivery is at least once, not exactly once. A handler can
  run again after a crash, an ambiguous completion, or a lost consumer group.
- A lost stream/group is recreated at ID 0 and replayed from the entries still
  present. Pending state from the lost group is unrecoverable, and entries whose
  bodies were trimmed cannot be replayed.
- Source-stream retention requires Redis 8.2+ and the Relay consumer group to
  exist; Relay skips trims while the group is absent rather than trimming
  without protection.
- Event retry count is configured per rule; backoff and reclaim timing are not
  configurable. See [Events](events.md#execution-retries-and-delivery).
- Schedule publication is deduplicated cluster-wide. Handler execution remains
  at least once, and publication recovery is bounded. See
  [Schedules](schedules.md#recovery-and-availability).
- Resource limits apply per container. Increasing concurrency or service
  replicas increases aggregate resource use.
- Workload network policy is not implemented. Relay verifies configured Docker
  networks but does not create or remove them.
