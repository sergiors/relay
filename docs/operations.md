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
2. opens the required PostgreSQL state database and initializes its schema
   (a missing `DATABASE_URL` or an unreachable database fails startup);
3. runs the external-dependency preflight in a fixed order, before anything
   touches `/apps`: Redis stream/consumer-group readiness
   (creates the group with `MKSTREAM` at position `0`, tolerating `BUSYGROUP`),
   then Docker daemon readiness (a ping with a 10s bound), then verification that
   every configured `NETWORKS` network exists, then the runtime manager's
   warm-container maintenance loop is started;
4. loads apps from `/apps` and computes each fingerprint once;
5. runs a conservative orphan sweep (bounded to 30s) removing only stale
   Relay containers owned by this worker identity (`WORKER_ID`);
6. prepares each app's image and starts the reconciler, scheduler, and
   stream consumer.

A preflight failure short-circuits every later phase: Redis or the Docker daemon
being unavailable, or a configured network missing, fails startup instead of
letting the worker load apps or create containers. The
manager is opened with deferred maintenance, so a preflight failure before the
final step closes the manager with no background loop ever started. A lifecycle
cancellation (SIGTERM/SIGINT) during the preflight is a graceful shutdown, not an
error.

Changes under `/apps` are reconciled live: a new directory is built and
starts matching; edits rebuild only that app (debounced 750ms); a failed
rebuild keeps the previous working version; a 30s periodic pass is the backstop
for missed watch events. `/apps` is read-only to Relay; all builds happen in
temporary contexts.

Shutdown cancels the lifecycle first, then runs ordered teardown steps (ownership
join and final ownership verification, socket, scheduler, reconciler,
housekeeping, services join/cleanup, loops,
stats flush, metrics, webhook, manager, state, tracing, Redis last). Best-effort
cleanup steps are bounded by an aggregate shutdown budget; a step failure or
timeout is logged and never aborts the sequence. Steps that gate a shared
dependency (the ownership monitor, the scheduler, reconciler, startup
housekeeping, the service coordinator, and the background loops) are quiescence
barriers: on a missed bound they are logged, cancelled, and then joined for real
before later steps tear that dependency down, so they may exceed the budget by
design rather than close a resource an operation is still using. The ownership
step joins the monitor and then runs one final bounded ownership verification on
a fresh background context, so a lock lost after the monitor's last poll (but
before shutdown cancelled the lifecycle) is still detected before any
identity-scoped cleanup runs. A graceful shutdown performs a final bounded stats
flush (after re-proving the lock). The process lock is held for the whole run and
released last (the kernel releases it if the process dies).

### Ownership loss

The exclusive per-`WORKER_ID` PostgreSQL owner lock is the cross-process guarantee
that two workers never share an identity. The worker re-verifies that lock
periodically (every 2s, each check bounded to 1s) **on the same pinned connection
that holds it**; it never checks out a replacement connection or re-acquires the
lock, either of which could mask a loss. If the lock connection is lost while the
process keeps running — a terminated backend, a broken link — the pinned
connection's backend PID changes or the lock disappears, and verification fails
**closed**.

Detection is interval-bounded, not instantaneous (normally within about 3s of the
loss), so a side effect scoped to the identity can still occur in that window. On
detection the worker cancels its lifecycle, clears readiness immediately (so
`relay health` stops reporting ready), logs **one** warning with `worker_id` and
`instance_id`, and skips identity-scoped cleanup on the way out: it does not run
its final stats flush or remove its service containers, and it skips its
identity-scoped startup orphan sweeps, because a replacement process may already
own the identity. It then exits with a distinctive ownership-loss error so the
supervisor treats the exit as abnormal termination rather than a clean signal
shutdown. Existing per-instance stats fencing remains as defense in depth for the
window before detection.

Because detection is interval-bounded, shutdown does not rely on the monitor
alone. After the monitor's quiescence barrier joins, one final bounded
verification runs on a fresh background context (never the already-cancelled
lifecycle), and each identity-scoped cleanup step — the service-container
cleanup, the final stats flush, and the startup orphan and service-orphan
sweeps — re-proves the lock on the pinned connection immediately before acting.
If any proof fails, the worker fails **closed**: it latches the loss and skips
that identity-scoped work rather than risk disturbing a replacement owner. A
single verification that stalls to its bound is an ambiguous result and
therefore also fails closed. A loss detected only by the final verification
still produces the same once-only side effects (one warning, readiness cleared,
lifecycle cancelled, cleanup skipped).

## State paths and persistence

Two path roots:

- **`/var/lib/relay`** — persistent state, volume-mountable:
  - `secrets/` — the local secret files (`0700` dir, `0600` files, atomic
    writes);
  - `ssh/id_ed25519`, `ssh/known_hosts` — deploy key and TOFU host-key pins;
  - `git/source.json`, `git/checkout` — git sync config and managed checkout;
  - `git/relay-<checkout-base>.sync.lock` — persistent checkout-sibling flock
    file; the lock releases on process exit, but the inert file is retained.
- **PostgreSQL** — the shared state database (operator state view, per-worker
  stats snapshots, and the durable schedule outbox), addressed by
  `DATABASE_URL`. It is **not** a file under a path root; see below.
- **`/run/relay`** — ephemeral process state (never persisted):
  - `relay.lock` — the `flock` held by `relay start`; inert after a crash;
  - `relay.sock` — the live worker control socket (`0600`), removed on shutdown.
- **`os.TempDir()`** — transient build staging (never persisted): a private
  `relay-tmp/` root holding one owner-locked `session-*` directory per Relay
  process, each containing the source/dependency snapshot roots that ARE the
  Docker build context. Removed on normal exit; a crashed process's session is
  reclaimed at the next startup only when its advisory lock is free. The root's
  parent must be sticky (e.g. `/tmp`) or a private directory owned by the current
  user, otherwise scavenging is disabled and build roots fall back to plain
  `os.TempDir`.

None of these paths are environment-configurable.

### State database

A read-mostly PostgreSQL view of Relay's loaded apps — **not** the source of
truth (`/apps` is) and not immutable. The worker writes it (discovery,
reconcile outcomes, a fixed 5s stats flush) and `relay stats reset` writes it;
every other state-touching subcommand only reads. It never drives matching,
building, or reconciliation. Unlike the former file-backed store, the database
is a **required** dependency: the worker opens it (and initializes the schema)
before the Redis/Docker preflight and before loading apps, and a missing or
unreachable `DATABASE_URL` fails startup rather than degrading. Post-startup
write failures remain non-fatal to the event pipeline. Startup persists each
valid app's discovery state before preparation; a failed write aborts startup
rather than allowing preparation or service publication without its exact
fencing token. Invalid-app state writes are isolated per app because invalid
apps are never prepared.

- Each app is stored as one native JSONB snapshot: runtime/status/image/
  fingerprint/prepared-at/reconcile outcome, env **names** (values redacted) and
  secret **references** (values never stored), handlers, schedules, and services.
- App reconciliation ownership is tracked separately in `apps.fencing_token`,
  allocated from the global `apps_fencing_token_seq` sequence. Each desired-state
  transition advances it under the per-app advisory lock; callbacks with stale
  tokens cannot update or recreate the row. The state schema is initialized for
  the current release and is not migrated across releases. Stop old Relay workers
  before switching binaries that expect a different schema; the `/apps` tree
  remains authoritative, but replacing the PostgreSQL state database discards
  its state view, stats, and schedule outbox.
- `status` lifecycle: `preparing` → `building` → `reconciling` → `ready`;
  `degraded` retains a usable previous image, `unavailable` has none. A failed
  reconcile never displaces a still-serving previous generation and never reports
  `ready`. An invalid desired template preserves a currently active runtime
  during live reconciliation. A SHA-256 identity of the read template bytes and
  the original error are persisted separately from desired/active fingerprints;
  identical
  bytes do not repeat the state transition or allocate another fencing token,
  while changed bytes are revalidated. Filesystem reads and source fingerprinting
  failures are transient and retried rather than cached. This state survives
  restarts, but startup does not reconstruct a runtime from invalid source, even
  when an older active image remains recorded in PostgreSQL.
- Stats are stored as latest absolute snapshots (no history), **keyed by a stable
  worker identity** (`WORKER_ID`, required — the same value as the Redis consumer
  name and the `relay.hostname` label) plus a per-process `instance` fencing
  token. Two workers sharing one database keep independent rows instead of
  overwriting each other (each must set a distinct `WORKER_ID`; a second active
  worker reusing an in-use `WORKER_ID` fails startup on the per-identity
  PostgreSQL owner lock), same-host
  workers never collide, a restarted worker resumes its own totals, and a stale
  process incarnation cannot overwrite a
  newer one. `relay stats` aggregates across owners, a worker restores only its
  own row, and `relay stats reset` clears every owner's rows while advancing a
  shared generation that fences delayed snapshots from every worker. A hard crash
  may lose up to ~5s of telemetry; graceful shutdown flushes once. Prometheus is
  the time-series source. Stale owner rows are compacted after `STATS_RETENTION`
  (default 7d).
- The scheduler's durable publication outbox (`schedule_pending`) lives here too;
  its per-row claims use `FOR UPDATE SKIP LOCKED` and a fencing token so
  concurrent retriers — in one process or several — never double-claim.
- Dependency-image coordination also lives here. Dependency layers are
  content-addressed and shared by every app version built from the same
  manifest, and they live on the Docker daemon, which several Relay processes
  may share. A preparation therefore holds a **shared** PostgreSQL session
  advisory lock for its dependency reference from before the dependency probe
  through the app-image build, and dependency garbage collection takes the
  **exclusive** lock for one candidate (non-blocking: a contended candidate is
  skipped) and re-reads the daemon's label-derived referenced set before
  removing it. The daemon is **not**
  a parent-reference guard — force-free removal can untag a layer that a
  completed app image still references (observed on Docker 29.9.2) — so this
  database-scoped coordination, not a Docker refusal, is what keeps a layer
  unremovable while any process is building from it.
  - Advisory locks are scoped to one PostgreSQL database. **Every Relay process
    that shares a Docker daemon must use the same `DATABASE_URL` database.**
    Separate state databases on one daemon are unsupported; there is no
    Docker-level fallback.
  - A preparation that cannot take the shared lock within the fixed contention
    bound fails retryably (the app keeps its previous generation and the
    reconcile is retried). GC fails closed: when it cannot take the exclusive
    lock it removes nothing and retries at the next natural lifecycle point.
  - The coordination session is the worker's pinned ownership connection, so a
    lost database connection releases these locks at the same moment the
    existing ownership monitor observes the loss and cancels the lifecycle. A
    lease is revalidated immediately before and after the Docker build it
    protects; the short window between a revalidation and the Docker call cannot
    be fenced from the database side.

The bundled `compose.yaml` runs an internal PostgreSQL (`postgres` service, no
published port by default) with a named volume, and points Relay at it via
`DATABASE_URL`; deleting that volume deletes the state view and the outbox
(never `/apps`).

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
reports healthy again. A detected ownership loss clears readiness immediately and
is reported distinctly (`worker ownership lost`) from an ordinary shutdown. Per-app
degraded/unavailable/invalid status, PostgreSQL
(and therefore the scheduler's storage state), optional tracing, and asynchronous
service convergence/housekeeping do not gate worker health.

There is no HTTP health/readiness endpoint; health is a CLI command.

`relay stats` reads the persisted snapshot from PostgreSQL, aggregated across
workers (no Redis, Docker, or worker needed; it does need `DATABASE_URL`). It may
lag live Prometheus by up to ~5s. `relay stats reset` zeroes cumulative global and per-app totals —
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
  handler retries. `relay_dlq_entries_total` counts successful **DLQ row
  inserts** (one per exhausted invocation, plus a placeholder per malformed
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
  (occurrence intents newly written to the local retry outbox — every due
  occurrence is persisted **before** its Redis publish, so this counts all newly
  inserted intents, not only failed first publishes),
  `relay_schedule_pending_retries_total`
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

Materialization is staged before it is committed. Relay copies every desired
app tree into a reserved `.sync-*` directory under `/apps` and checks that each
copy is complete and contains `template.yaml` before touching any live app, so
a staging/copy failure leaves the previous `/apps` tree intact. These checks do
not parse or schema-validate the template: that remains the reconciler's
responsibility. A template-present but invalid config is still committed to
`/apps` as desired state for the reconciler to handle; invalid template contents
do not make Git sync fail.

After staging, each existing app's staged tree is compared with its live tree by
actual content — entry names, directory and file permission bits, file bytes, and
therefore empty directories too. An unchanged app is left exactly as it is: its
live directory and inode are preserved, it is not reported as materialized, and
no atomic exchange is attempted. Changed existing apps are replaced with an atomic directory exchange
(`renameat2(RENAME_EXCHANGE)` on Linux, `renamex_np(RENAME_SWAP)` on macOS), so
an updated app name is never absent and never partially copied; the
exchange-capability check runs only when a changed existing app must be replaced.
New apps are atomically renamed into an absent name, and removed apps are
atomically renamed into the reserved namespace before deletion. A sync with an identical revision therefore replaces nothing
(repeating a sync is a no-op apart from the recorded sync metadata). Comparison
errors fail the sync rather than treating an app as unchanged, and no comparison
fingerprint is persisted.

Because unchanged live directories are not exchanged, their inode and recursive
watch remain valid. When a directory is replaced, the reconciler releases the
watches that pointed at the detached old tree and registers the currently
installed tree, so later nested edits are still observed. A directory whose
watch registration or walk failed is retried with bounded backoff; one
unreadable or concurrently removed subtree does not disable watch delivery for
the rest of `/apps`. Recoverable watcher errors (including an event-queue
overflow) trigger a watch rescan and an immediate reconciliation sweep for the
missed gap, while the periodic pass remains the final backstop. A terminal
watcher/channel failure is logged at Error and returned by the reconciler's
`Start`, so the worker surfaces a dead live-change path instead of silently
continuing as if watches were healthy. `/apps` itself is a
fixed bind mount and is never renamed as a root. The commit boundary is per app
rather than a whole-revision snapshot, so a concurrent reader can briefly observe
a mixture of old and new apps; per-app atomicity is not whole-revision atomicity.
An app name always resolves to a complete tree, never a partially copied
directory; a reader whose walk spans a swap can still see files from both
generations. If a changed existing app must be replaced and the filesystem
cannot exchange directories, the sync fails before changing a live app rather
than falling back to remove-then-rename.

Syncs that share the managed checkout serialize on an advisory lock, so a manual
`relay git sync` and a webhook-triggered sync cannot interleave. Stale `.sync-*`
directories left by an interrupted sync are removed by the next locked sync.

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
next_attempt_at:<deadline>`, and `exhausted:<attempt>:<token>`. Every
  transition is one atomic Lua script; active-claim transitions CAS both attempt
  and claim token, so a stale claim can never overwrite a newer claim or a
  terminal marker. Dead-letter rows are written idempotently to PostgreSQL before
  the source ACK; the terminal invocation-state retention TTL is applied only
  after that ACK succeeds, so an entry can never be lost by an early expiry.
  While the message is pending the hash is
  **persistent**;
  only after it leaves the PEL (successful XACK or a purged
  missing-payload reference) does it switch to terminal retention with the
  configured `REDIS_INVOCATION_RETENTION` TTL (default 48h; empty/0/negative
  disables the expiry, leaving the marked hash persistent). The ACK and that
  retention switch are one atomic script, so the message can never leave the PEL
  before its hash carries the marker; a failed ACK stages only a persistent
  marker (no TTL) and never leaves pending state expiring. A hash whose
  retention never runs is leaked, never prematurely expired.
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

The DLQ is a durable PostgreSQL table. `relay dlq ls` lists entries;
`inspect` shows one entry's metadata and original event; `rm` deletes one
**settled** entry; `replay` re-executes one entry's exact app/handler once on the
running worker and deletes it only on success (kept on failure). See
[cli.md](cli.md). An unsettled entry is still needed by source-delivery recovery:
`rm` returns an explicit error for it, and retention never deletes it.

Replay admission is a fenced PostgreSQL lease: `replay` claims a settled entry
before dialing, so a concurrent replay is refused rather than executed twice.
The claim is released on a definitive failure, retained on an ambiguous exchange
or client cancellation until it expires, and the success delete requires the same
token and an unexpired lease. `rm` refuses an active claim and retention skips it.
Replay is at-least-once: work that finishes after a disconnect or after lease
expiry can be replayed again, so handlers must be idempotent.

Malformed-message and oversized-event entries are non-replayable placeholders (no
app/handler to re-execute). An oversized entry's `event` is a bounded diagnostic
summary, never the oversized payload; use `inspect`/`rm` on it.

## Reliability model and limitations

- Delivery is at-least-once; there is a crash window between a handler's side
  effect and its recorded completion, so a handler can run twice. Idempotency is
  the application's responsibility; exactly-once is not claimed.
- Per-invocation retry budgets are configured by each app rule's `retries`
  property (default `4`, maximum `100` additional attempts); see
  [Application templates](apps.md#timeouts-and-retries). Retry backoff, reclaim
  cadence, and the stats flush interval are fixed internals, not configurable.
- `resources` are per container; there is no aggregate per-app budget.
- Networking is enabled (outbound access is a legitimate app need);
  per-app network policy is not implemented. Relay never creates or removes
  Docker networks: `NETWORKS` and `TRAEFIK_NETWORK` are operator-owned, verified
  before use, and a network removed from the daemon afterwards surfaces as a
  container-create failure rather than being re-created.
- Build/version metadata is not plumbed into the binary (no build-info metric).
