# Operations

This page covers running Relay: lifecycle, state, observability, secrets, git,
and recovery.

## Runtime lifecycle

`relay start` runs in the foreground, holds a process lock, and never
daemonizes, forks, or writes a PID file. Supervision belongs to Docker, systemd,
Kubernetes, etc.

At startup Relay:

1. loads configuration and initializes tracing (disabled by default) and the
   metrics registry;
2. runs the external-dependency preflight in a fixed order, before anything
   touches `/apps` or the state DB: Redis stream/consumer-group readiness
   (creates the group with `MKSTREAM` at position `0`, tolerating `BUSYGROUP`),
   then Docker daemon readiness (a ping with a 10s bound), then verification that
   every configured `NETWORKS` network exists, then the runtime manager's
   warm-container maintenance loop is started;
3. loads apps from `/apps` and computes each fingerprint once;
4. opens the local state database (errors are logged, never fatal);
5. runs a conservative orphan sweep (bounded to 30s) removing only stale
   Relay containers owned by this worker hostname;
6. prepares each app's image and starts the reconciler, scheduler, and
   stream consumer.

A preflight failure short-circuits every later phase: Redis or the Docker daemon
being unavailable, or a configured network missing, fails startup instead of
letting the worker load apps, open the state DB, or create containers. The
manager is opened with deferred maintenance, so a preflight failure before the
final step closes the manager with no background loop ever started. A lifecycle
cancellation (SIGTERM/SIGINT) during the preflight is a graceful shutdown, not an
error.

Changes under `/apps` are reconciled live: a new directory is built and
starts matching; edits rebuild only that app (debounced 750ms); a failed
rebuild keeps the previous working version; a 30s periodic pass is the backstop
for missed watch events. `/apps` is read-only to Relay; all builds happen in
temporary contexts.

Shutdown cancels the lifecycle first, then runs ordered teardown steps (socket,
scheduler, reconciler, housekeeping, services join/cleanup, loops, stats flush,
metrics, webhook, manager, state, tracing, Redis last). Best-effort cleanup
steps are bounded by an aggregate shutdown budget; a step failure or timeout is
logged and never aborts the sequence. Steps that gate a shared dependency (the
scheduler, reconciler, startup housekeeping, the service coordinator, and the
background loops) are quiescence barriers: on a missed bound they are logged,
cancelled, and then joined for real before later steps tear that dependency
down, so they may exceed the budget by design rather than close a resource an
operation is still using. A graceful shutdown performs a final bounded stats
flush. The process lock is held for the whole run and released last (the kernel
releases it if the process dies).

## State paths and persistence

Two path roots:

- **`/var/lib/relay`** — persistent state, volume-mountable:
  - `db.sqlite3` — the local state database (see below);
  - `secrets/` — the local secret files (`0700` dir, `0600` files, atomic
    writes);
  - `ssh/id_ed25519`, `ssh/known_hosts` — deploy key and TOFU host-key pins;
  - `git/source.json`, `git/checkout` — git sync config and managed checkout.
- **`/run/relay`** — ephemeral process state (never persisted):
  - `relay.lock` — the `flock` held by `relay start`; inert after a crash;
  - `relay.sock` — the live worker control socket (`0600`), removed on shutdown.

None of these paths are environment-configurable.

### Local state database

A read-mostly SQLite view of Relay's loaded apps — **not** the source of
truth (`/apps` is) and not immutable. The worker writes it (discovery,
reconcile outcomes, a fixed 5s stats flush) and `relay stats reset` writes it;
every other state-touching subcommand only reads. It never drives matching,
building, or reconciliation, and a missing/broken database is recreated or
degraded without stopping the worker.

- Each app is stored as one JSON (JSONB) snapshot: runtime/status/image/
  fingerprint/prepared-at/reconcile outcome, env **names** (values redacted) and
  secret **references** (values never stored), handlers, schedules, and services.
- `status` lifecycle: `preparing` → `building` → `reconciling` → `ready`;
  `degraded` retains a usable previous image, `unavailable` has none. A failed
  reconcile never displaces a still-serving previous generation and never reports
  `ready`.
- Stats are stored as latest absolute snapshots (no history). A hard crash may
  lose up to ~5s of telemetry; graceful shutdown flushes once. Prometheus is the
  time-series source.

`compose.dev.yaml` mounts a named volume at `/var/lib/relay`, so state, secrets,
and git material survive container restarts. Deleting the volume deletes them.

## Health and readiness

`relay health` reports whether the **running worker** is healthy by asking it,
over its Unix control socket, whether it is ready to consume with its live
dependencies healthy (the container healthcheck). It exits `0` when healthy, `1`
otherwise, and needs no Redis or Docker configuration in the CLI process: it
creates no dependency clients and loads no configuration, so the answer comes
entirely from the worker. With no running worker it fails — worker health is only
meaningful as observed by the worker that owns the dependencies.

Readiness is worker-owned: the flag starts false and is set true only at the
ready-to-consume boundary (after the external-dependency preflight, app
loading/preparation, and the socket/listener/loop and
consumer/schedule/reconciler/scheduler wiring, just before consumption), and
cleared first on shutdown; it is bound to the worker lifecycle context, so a
lifecycle cancellation that precedes the clear also reports not-ready. In steady
state the answer reflects live Redis consumer health, a bounded Docker ping, and
`NETWORKS` verification, so a dependency failing reports unhealthy and recovery
reports healthy again. Per-app degraded/unavailable/invalid status, SQLite (and
therefore the scheduler's storage state), optional tracing, and asynchronous
service convergence/housekeeping do not gate worker health.

There is no HTTP health/readiness endpoint; health is a CLI command.

`relay stats` reads the persisted global snapshot from SQLite (no Redis, Docker,
or worker needed; works when the runtime is down). It may lag live Prometheus by
up to ~5s. `relay stats reset` zeroes cumulative global and per-app totals —
with a running worker, over the Unix socket so the in-memory source is reset too;
otherwise directly against the database. It never touches pending events, Redis,
containers, schedules/services, or Prometheus counters (those stay monotonic; the
worker subtracts a reset baseline when snapshotting). Backlog gauges are not
reset.

## Observability

Relay's observability is logs + Prometheus metrics + the local state snapshot.
There is no HTTP health/readiness endpoint.

### Metrics

When `METRICS_ADDR` is set, `GET /metrics` exposes Prometheus text format.
Canonical names carry the `relay_` prefix. Highlights:

- **Event classification** (closed partition): `relay_events_received_total ==
relay_events_matched_total + relay_events_unmatched_total`, classified exactly
  once per external event across redeliveries. Schedule occurrences are excluded
  because schedule dispatch resolves/adopts the configured handler directly and
  bypasses ordinary event classification and pattern matching. Both dispatch
  types use the same Redis Stream/consumer group and invocation-state,
  retry/recovery, and DLQ infrastructure; schedule occurrences run their handler
  as a one-shot function. Persistent services are separate long-lived workloads.
- **Handlers/retries/DLQ:** `relay_handler_success_total`,
  `relay_handler_failure_total`, `relay_retries_total`, `relay_dlq_entries_total`,
  `relay_handler_invocations_total{outcome,app,handler}`,
  `relay_handler_duration_seconds{app,handler}`.
  `relay_retries_total` counts stream **message reclaims** (redeliveries), not
  handler retries. `relay_dlq_entries_total` counts successful **DLQ entry
  writes** (one per exhausted invocation, plus a placeholder per malformed
  message).
- **Per-app:** `relay_app_events_matched_total{app}`,
  `relay_function_handler_*_total{app}`, `relay_function_retries_total`
  (handler retry attempts), `relay_function_dlq_total` (invocations that
  exhausted their retry budget — the exhaustion commit, **not** a successful DLQ
  write), `relay_app_status{app,status}`.
- **Backlog/concurrency:** `relay_pending_entries`,
  `relay_pending_oldest_age_seconds` (sampled from `XPENDING` every 15s),
  `relay_buffered_events`, `relay_in_flight_invocations`,
  `relay_concurrency_waits_total`.
- **Warm pool:** `relay_runtime_pool_capacity{app}`,
  `relay_runtime_containers{app,state=idle|busy|starting}`,
  `relay_runtime_container_acquires_total{app,outcome=warm|cold}`,
  `relay_runtime_container_discards_total{app,reason}`,
  `relay_runtime_container_waits_total{app}`,
  `relay_runtime_container_acquire_duration_seconds{app}`.
- **Schedules:** `relay_schedule_occurrences_published_total`,
  `relay_schedule_occurrences_duplicate_total`,
  `relay_schedule_publish_failures_total`,
  `relay_schedule_publish_retries_total`,
  `relay_schedule_publish_exhausted_total`, `relay_schedule_catchup_total`.
- **Durable schedule recovery:** `relay_schedule_pending_persisted_total`
  (occurrences newly written to the local retry outbox after an immediate
  publication did not resolve), `relay_schedule_pending_retries_total`
  (publication attempts made by the durable retry worker), and
  `relay_schedule_pending_expired_total` (outbox records removed because their
  **7-day** retention deadline passed without resolving — an expiration, counted
  as neither a publish success, failure, nor duplicate, because no publish is
  attempted). A resolved attempt removes the outbox row; the publish outcome
  itself is still counted by the `relay_schedule_occurrences_*` families above.
  The Redis occurrence dedup key lives **14 days**, outliving the 7-day outbox.
- **Scheduler storage state:** `relay_scheduler_state{state=running|degraded|unavailable}`
  (one-hot gauge of the scheduler's durable-store state),
  `relay_scheduler_degraded_total` (transitions into degraded because an outbox
  operation failed), and `relay_scheduler_recoveries_total` (transitions back to
  running after a bounded recovery). Schedule firing requires a usable outbox,
  but this state does **not** gate worker readiness or event processing.
- **Services:** `relay_service_reconciles_total{app,outcome}`,
  `relay_service_reconcile_duration_seconds{app}`.
- **Anomaly:** `relay_missing_payload_total` (reclaimed PEL entries whose stream
  body no longer exists) and `relay_events_oversized_total` (delivered messages
  whose raw `event` value exceeded `MAX_EVENT_BYTES`, rejected before decode and
  routed non-retryably to the DLQ; unlabeled — event IDs and sizes are never
  labels).
- **Builds:** `relay_app_build_failures_total{app}`,
  `relay_app_build_seconds{app}`, and the unlabeled
  `relay_app_build_output_truncated_total` (build responses whose retained
  diagnostic reached the internal 1 MiB bound; the stream is still fully drained,
  only the retained diagnostic is bounded).

Labels are bounded to app/handler/outcome and small closed sets; IDs and
raw errors are never labels. The metrics server is fail-fast on a taken port and
isolated from the event path.

### Tracing

OpenTelemetry tracing is opt-in via the standard OTLP environment (see
[configuration.md](configuration.md)) and propagates W3C TraceContext + Baggage.
Relay installs exactly those two propagators; `OTEL_PROPAGATORS` is not
honored. The primary propagation path is the Redis stream: external events and
schedule occurrence publication carry `traceparent`/`tracestate`/`baggage` as
flat stream metadata, consumption extracts them, and invocation frames carry
them to the Python/Node bootstraps. Schedule occurrences use schedule-specific
dispatch, not ordinary event matching. Each handler attempt is its own span;
retries link to the prior attempt via persisted compact lineage; DLQ replay links
to the original failed
invocation. Manual invocations create a new-root span. Persistent services are
not propagation boundaries. A setup error is non-fatal — the worker runs
untraced.

### Logging

Relay logs with `slog` (fixed messages + structured attributes). Execution,
retry, failure, DLQ, reconcile, and build lines carry `app`, `handler`,
`message_id`, `attempt`, `duration`, and `exit_code` where available. Handler
stdout/stderr is a raw transport forwarded verbatim, unaffected by `LOG_LEVEL`,
prefixed `[app/handler@id] stream:`. `LOG_LEVEL` filters only Relay's own
lines.

## Secrets

Secrets are files on disk under `/var/lib/relay/secrets`; templates reference
them by name. They are never baked into images, never stored in the state
database, never logged, and never shown by `relay app inspect` (which shows
references only). Manage them with `relay secret ls|set|rm`.

- **Rotating** a secret value takes effect on the next event/schedule invocation
  (resolved per execution; no rebuild, no restart). For a persistent service, it
  takes effect at the next reconcile (the container is replaced), also with no
  rebuild.
- **Docker access boundary.** Anyone who can talk to the Docker daemon can read
  every container's environment, the mounted Relay data volume (including the
  secret files), and container memory. Relay's guarantee is only that a value is
  never exposed through Relay's own surfaces (state DB, `template.yaml`, labels,
  logs, metrics, traces) — not that it is hidden from the daemon. Treat daemon
  access as secret read access.
- The filesystem provider is the current single-host implementation behind a
  deliberately tiny interface; external providers (Vault, etc.) are not
  implemented.

## Git

Git synchronization is manual by default: `relay start` never polls, watches, or
fetches. Only `relay git sync` (or an accepted webhook delivery) updates
`/apps`.

```sh
relay git keygen                             # generate an SSH deploy key (once)
relay git set git@github.com:acme/repo.git   # remember the SSH source
relay git sync                               # materialize into /apps
relay git status
relay git remove -y
```

`relay git set` accepts SSH URLs only (scp-like or `ssh://`; no HTTPS) and
options `--ref` (default `main`), `--path` (monorepo subdir), and
`--webhook-secret NAME`. While a source is configured and synced, `/apps`
is owned by git: a sync rewrites it to reflect exactly the repository/path,
removing directories not in the source.

Host-key verification is never disabled; Relay maintains its own
`known_hosts` with TOFU. A changed host key fails the sync loudly and is never
auto-replaced.

### GitHub webhook (opt-in)

The endpoint `POST /github` on `GIT_WEBHOOK_ADDR` is **opt-in twice over**: it
starts only when the address is set **and** the git source names a webhook
secret. Only `push` deliveries for the configured repository/ref schedule a
coalesced sync (at most one at a time; bursts collapse to one follow-up run); the
handler returns `202` immediately and never syncs in the request path. Every
delivery must carry a valid `X-Hub-Signature-256` HMAC; a missing/invalid
signature is `401`, a malformed payload `400`. The secret is read from the local
store per delivery, so rotations apply without a restart. A bind failure is fatal
at startup.

## Redis recovery and reliability

- Consumer group is created with `MKSTREAM` at position `0`; a new group over an
  existing stream replays its backlog.
- Per-invocation state is a Redis hash keyed by message and
  `<app>/<handler>` with forms `ok`, `running:<deadline>`, `
next_attempt_at:<deadline>`, `exhausted`, and `exhausted:…:dlq`. Every
  transition is one atomic Lua script; active-claim transitions CAS both attempt
  and claim token, so a stale claim can never overwrite a newer claim or a
  terminal marker. While the message is pending the hash is **persistent**;
  only after it leaves the PEL (successful XACK or a cleared
  missing-payload reference) does it switch to terminal retention with a ~7-day
  TTL. A hash whose retention never runs is leaked, never prematurely expired.
- Deadlines are integer Unix milliseconds, compared exactly (`now_ms <
deadline_ms` is protected).
- Recovery reclaims idle pending messages (`XAUTOCLAIM`, default 1m) as a
  message-level backstop, not the retry timer.
- Redis outages are survived with bounded, jittered backoff (1s…30s cap).
- State, metrics, and cleanup failures are logged, never fatal. Transport errors
  leave messages pending (fail open); two failures fail closed and leave the
  message pending with no handler run and no ACK — an ambiguous `TryStart` claim
  (it could race a replica that won) and a `makeRecoverable` failure (a legacy-TTL
  hash that could not be made persistent may expire mid-delivery, so its state
  cannot be relied on).

## DLQ controls

The DLQ stream is `relay:<REDIS_STREAM>:dlq`. `relay dlq ls` lists entries;
`inspect` shows one entry's metadata and original event; `rm` deletes one;
`replay` re-executes one entry's exact app/handler once on the running
worker and deletes it only on success (kept on failure). See [cli.md](cli.md).

Malformed-message and oversized-event entries are non-replayable placeholders (no
app/handler to re-execute). An oversized entry's `event` is a bounded diagnostic
summary, never the oversized payload; use `inspect`/`rm` on it.

## Reliability model and limitations

- Delivery is at-least-once; there is a crash window between a handler's side
  effect and its recorded completion, so a handler can run twice. Idempotency is
  the application's responsibility; exactly-once is not claimed.
- Retry backoff, exhaustion limits, reclaim cadence, and the stats flush interval
  are fixed internals, not configurable.
- `resources` are per container; there is no aggregate per-app budget.
- Networking is enabled (outbound access is a legitimate app need);
  per-app network policy is not implemented. Relay never creates or removes
  Docker networks: `NETWORKS` and `TRAEFIK_NETWORK` are operator-owned, verified
  before use, and a network removed from the daemon afterwards surfaces as a
  container-create failure rather than being re-created.
- Build/version metadata is not plumbed into the binary (no build-info metric).
