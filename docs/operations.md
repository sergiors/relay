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
the current pin. Runtimes without a row remain unpinned.

#### Updating a runtime image selection

`relay runtime update RUNTIME` is the supported selection writer. It loads the
current row, resolves ONLY that runtime's exact configured base tag through the
Docker Engine Distribution API (`GET /distribution/<ref>/json`), and persists the
returned multi-platform index digest with a compare-and-swap on the persisted
generation. The Engine contacts the registry; Relay does not implement a registry
HTTP client and never pulls the image. If the resolved digest equals the current
selection, no write occurs, so the generation and timestamp are unchanged; that
no-op is confirmed against the authoritative row under the same per-runtime lock
used by selection writes, so a concurrent winner is reported instead of a stale
success. A concurrent write is an explicit conflict and is never overwritten
blindly. `relay runtime update --all` processes every configured runtime
independently, continues after failures, and exits non-zero if any runtime failed.
Both update forms report the previous identity (`unpinned` or the prior digest)
and the newly resolved digest.

There is no operator-facing CLI rollback or arbitrary-digest command:
`runtime update` resolves only the current configured tag. An authorized
administrative operation can roll back by calling the existing
`state.CompareAndSwapRuntimeImageSelection` API with a previously known valid
digest and the runtime's current expected generation; that path shares the
per-runtime advisory lock used by publication. Direct SQL or other manual edits
to `runtime_image_selections` must not bypass that CAS and lock path.

`relay runtime check [RUNTIME]` resolves the current registry digest read-only
through the same Engine API and compares it with the persisted selection. It
shows the selected digest (or `unpinned`), the remote digest, and a status
(`matching`, `different`, `available`, `unpinned`, `invalid`, or `failed`). It
performs no state write, pull, build, or restart; all-mode continues across every
runtime, renders per-runtime registry failures, and exits non-zero if any runtime
failed. `relay runtime ls` and `relay runtime inspect` remain local-only and read
PostgreSQL without contacting Docker or a registry; `ls` shows selection
generation and time, and `inspect` shows the target build platform and effective
image reference.

A digest is accepted only as a multi-platform index identity. Relay validates
that the Engine response refers to the queried reference, carries a valid
`sha256:<64 lowercase hex>` digest, and has an OCI image index or Docker manifest
list media type. A missing, unknown, or platform-specific manifest is rejected.
When the Engine reports platforms, the index must contain more than one distinct
concrete OS/architecture combination (unknown/attestation entries are ignored).
Relay never persists a child manifest digest; workers build from
`<configured-ref>@<index-digest>` for the target Linux architecture.

The Docker Engine must permit the Distribution endpoint. Relay uses the
daemon configured by the Docker client environment (by default the mounted
Unix socket); an externally managed socket proxy must allow the Distribution
endpoint. Registry credentials are read from the Docker client configuration
selected by `DOCKER_CONFIG` (a directory), falling back to
`$HOME/.docker/config.json`. The supported fields are `auths` (base64
`username:password` or `identitytoken`), `credHelpers`, and `credsStore`.
Credential helpers are invoked as context-bound
`docker-credential-<helper> get` subprocesses; their output is parsed but never
logged. Public Docker Hub works anonymously. When Relay runs in a container,
mount the Docker client configuration and set `DOCKER_CONFIG` if it is not at
the container user's `$HOME/.docker/config.json`; Relay does not read
credentials from the daemon or from the relay container's environment.

Persisting a selection is not an application rollout. Running workers refresh
the complete selection snapshot once per periodic reconciliation pass (the
default is 30 seconds) and reprepare only the runtime-backed apps using the
changed runtime. `update` does not pull or build an image, restart the worker,
restart apps, or deploy anything; wait for the next pass (or restart the worker
if it is not running) to observe app convergence.

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
values are not stored in PostgreSQL, images, labels, or Relay logs, and
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
