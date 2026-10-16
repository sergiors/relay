# Configuration

Relay reads its own settings from the environment once, at the executable
boundary (`internal/config`). A missing required variable or an invalid tuning
value is returned to `cmd/main.go`, which prints it once and exits non-zero.

Every setting below maps 1:1 to an environment variable. Relay does **not** read
the Docker client or OpenTelemetry variables itself: those are consumed by the
pinned libraries (see the last section) and are listed separately.

## Required

| Variable       | Format                                                                   | Behavior                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REDIS_URI`    | `host:port`, `redis://[user:password@]host:port`, or `rediss://…` (TLS)  | Redis address or DSN. Missing/empty fails startup.                                                                                                                                                                                                                                                                                                                                                    |
| `REDIS_STREAM` | stream name                                                              | Stream Relay consumes. Missing/empty fails startup.                                                                                                                                                                                                                                                                                                                                                   |
| `REDIS_GROUP`  | group name                                                               | Consumer group. Missing/empty fails startup. Created with `MKSTREAM` at position `0` if absent, so a new group over an existing stream replays its backlog.                                                                                                                                                                                                                                           |
| `DATABASE_URL` | PostgreSQL DSN (`postgres://user:password@host:5432/db?sslmode=disable`) | The shared state database (operator state view, per-worker stats snapshots, and the durable schedule outbox). Missing/empty fails startup; an unreachable database fails startup. The value carries credentials and is never logged.                                                                                                                                                                  |
| `WORKER_ID`    | stable token, ≤128 chars of ASCII letters/digits/`.`/`_`/`-`             | The worker's **stable identity**. Missing/empty fails startup; a malformed value fails startup. It is the Redis consumer name, the `relay.hostname` container-ownership label, and the persisted-stats owner key, so a restart reuses one identity across all three. It must be unique among concurrently running workers and stable across restarts: the PostgreSQL state owner lock (a per-identity session advisory lock) is held for the worker's lifetime, so a second active worker presenting the same `WORKER_ID` fails startup with a clear error rather than silently sharing one identity. The worker re-verifies this lock periodically and, if it is lost while the process is still running (a terminated backend or broken link), cancels its lifecycle and exits with an ownership-loss error rather than continuing under an identity it no longer owns (see [operations.md](operations.md)). There is **no** hostname or generated fallback. |

The consumer name is **not** configurable: Relay uses the required, stable
`WORKER_ID`, so replicas sharing `REDIS_GROUP` must each configure a unique
`WORKER_ID`. The same identity is used for `relay.hostname` container labels
and persisted `stats`/`app_stats` rows, while a fresh per-process `instance_id`
provides fencing to prevent stale instances from overwriting statistics (see
[operations.md](operations.md)).

## Optional

| Variable                      | Default       | Format / accepted values                                                           | Behavior when invalid                                                                                                                                                                                                                                                                                                                                                                                           |
| ----------------------------- | ------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOG_LEVEL`                   | `INFO`        | `DEBUG`, `INFO`, `WARN`, `ERROR` (case-insensitive, trimmed)                       | Fails startup. `WARNING` is **not** an alias.                                                                                                                                                                                                                                                                                                                                                                   |
| `MAX_CONCURRENT_INVOCATIONS`  | `8`           | positive integer                                                                   | Fails startup. Bounds concurrent invocations **per worker**.                                                                                                                                                                                                                                                                                                                                                    |
| `MAX_CONCURRENT_BUILDS`       | `2`           | positive integer                                                                   | Fails startup. Bounds concurrent runtime-backed image-preparation pipelines **per worker**.                                                                                                                                                                                                                                                                                                                     |
| `MAX_WARM_CONTAINERS`         | `8`           | positive integer                                                                   | Fails startup. Hard per-worker bound on warm **execution** containers across all apps (idle + busy); when saturated every idle container is LRU-evicted, and if all are busy the invocation stays pending. See below.                                                                                                                                                                                           |
| `MAX_BUFFERED_EVENTS`         | `16`          | positive integer                                                                   | Fails startup. Per-worker bound on messages read from Redis and held locally, including deliveries waiting on execution capacity. It is **not** a bound on the Redis consumer-group PEL. See below.                                                                                                                                                                                                             |
| `MAX_EVENT_BYTES`             | `256KiB`      | human-readable binary size (`256B`, `256KiB`, `1MiB`; bare integer bytes accepted) | Fails startup on zero/negative/malformed/decimal-unit/above-1MiB. Byte length of a message's raw `event` value; over-limit messages are non-retryably dead-lettered with a bounded summary.                                                                                                                                                                                                                     |
| `WARM_CONTAINER_IDLE_TIMEOUT` | `5m`          | positive Go duration (`90s`, `10m`, `1h30m`)                                       | Fails startup.                                                                                                                                                                                                                                                                                                                                                                                                  |
| `HOT_RELOAD`                  | `false`       | boolean (`1`, `t`, `true`, `0`, `f`, `false`; trimmed)                             | Fails startup on an unparseable value. `false` bakes the source into the immutable runtime image; `true` source-mounts the live app directory from `/apps` read-only into containers for runtimes with a mountable dependency layout (Python and Node), so a source-only edit reuses the app and dependency images, managed services restart in place, and pooled execution containers are recycled. See below. |
| `METRICS_ADDR`                | unset         | listen address (`:9090`)                                                           | Empty disables the Prometheus endpoint. A bind failure is fatal at startup.                                                                                                                                                                                                                                                                                                                                     |
| `GIT_WEBHOOK_ADDR`            | unset         | listen address (`:8081`)                                                           | Empty disables the GitHub webhook. A bind failure is fatal. Starts only when the git source also names a webhook secret.                                                                                                                                                                                                                                                                                        |
| `NETWORKS`                    | unset         | comma-separated Docker network names                                               | Parsed at startup (trimmed, de-duplicated, declaration order kept). Every name is verified to exist; a missing network fails startup. Applied to execution containers and (as an order-independent set) service containers.                                                                                                                                                                                     |
| `REDIS_STREAM_RETENTION`      | `24h`         | Go duration (`6h`, `90m`)                                                          | The optional value that **logs and disables** instead of failing. See below.                                                                                                                                                                                                                                                                                                                                    |
| `REDIS_DLQ_RETENTION`         | `7d` (`168h`) | Go duration (`168h`, `90m`)                                                        | The optional value that **logs and disables** instead of failing. Age-only DLQ trim. See below.                                                                                                                                                                                                                                                                                                                 |
| `REDIS_INVOCATION_RETENTION`  | `48h`         | Go duration (`12h`, `90m`)                                                         | The optional value that **logs and disables** instead of failing. Terminal invocation-state TTL only. See below.                                                                                                                                                                                                                                                                                                |
| `STATS_RETENTION`             | `7d` (`168h`) | Go duration (`168h`, `90m`)                                                        | The optional value that **logs and disables** instead of failing. Bounds stale stats-owner rows: after a successful flush the worker deletes other workers' `stats`/`app_stats` rows older than this window. An explicitly empty value disables compaction. See below.                                                                                                                                          |
| `TRAEFIK_NETWORK`             | unset         | Docker network name                                                                | Required only when a service declares `host`; verified on every routed reconcile and joined in addition to `NETWORKS`.                                                                                                                                                                                                                                                                                          |
| `TRAEFIK_ENTRYPOINTS`         | unset         | comma-separated Traefik entrypoint names (`websecure`, `web,websecure`)            | Passed through verbatim into the router label; no default.                                                                                                                                                                                                                                                                                                                                                      |
| `TRAEFIK_CERTRESOLVER`        | unset         | resolver name (`letsencrypt`)                                                      | When set, adds both `tls=true` and `tls.certresolver`; unset adds neither.                                                                                                                                                                                                                                                                                                                                      |
| `TRAEFIK_PRIORITY`            | unset         | positive integer                                                                   | Any provided value must be positive (a fatal error otherwise); unset omits the label.                                                                                                                                                                                                                                                                                                                           |
| `TRAEFIK_HOST_OVERRIDE`       | unset         | hostname suffix (`localhost`)                                                      | Replaces the domain of each routed host, keeping its left-most label (`issuer.example.com` → `issuer.localhost`). Unset uses declared hosts verbatim.                                                                                                                                                                                                                                                           |

`METRICS_ADDR`, `GIT_WEBHOOK_ADDR`, and the `TRAEFIK_*` values are read only by
the worker. The read-only admin commands resolve only what they need — the state
DSN via `LoadDatabaseURL` (no `WORKER_ID`, `REDIS_*`, or tuning required) or the
Redis/DLQ settings via `LoadRedisConfig` — but the process logger is bootstrapped
from `LOG_LEVEL` before any command runs, so an invalid `LOG_LEVEL` fails every
command too. `WORKER_ID` and the tuning variables are required only by
`relay start`.

`NETWORKS` and `TRAEFIK_NETWORK` name Docker networks Relay attaches containers
to; Relay **never creates or removes** them (they are infrastructure owned
outside Relay). A name present at startup verification that is later deleted from
the daemon surfaces as a container-create failure on the next execution/service
create — Relay reports it and never re-creates the network. `NETWORKS` is startup
configuration: changing it requires a worker restart.

### Concurrency and backpressure

Three distinct quantities bound work, and they are not the same bound. Two are
**per worker** and multiply across `N` replicas:

- **Execution concurrency** — `MAX_CONCURRENT_INVOCATIONS` (default `8`) bounds
  concurrently _executing_ invocations in one worker. It is startup configuration
  (restart to change); a template's per-app `concurrency` is clipped to it live
  and is documented in [apps.md](apps.md).
- **Local processing slots** — `MAX_BUFFERED_EVENTS` (default `16`) bounds the
  messages a worker holds locally after reading them from Redis and before they
  finish, **including** deliveries that are waiting on execution capacity. It
  bounds local memory, not the Redis stream.

The third is **group-wide**, not a Relay buffer:

- **Redis PEL** (pending entries list) — the consumer-group-wide set of
  delivered-but-unacknowledged messages, shared by every worker in the group and
  **not** bounded by `MAX_BUFFERED_EVENTS`; its size is the sum of all workers'
  outstanding deliveries. A PEL entry references the original stream entry
  (owning consumer, idle time, delivery count) and carries **no independent copy
  of the event payload**. It is distinct from **group lag**, the count of stream
  entries not yet delivered to the group: lag shrinks when a message is
  delivered, while the PEL grows until that delivery is acknowledged.

`MAX_CONCURRENT_BUILDS` (default `2`) is also **per worker** and bounds how many
runtime-backed image-preparation pipelines run at once: one pipeline — source
selection and snapshot, dependency snapshot, reuse probes, dependency image
build, and app image build — holds one slot for its **whole** duration, so a
preparation's dependency and app sub-builds are sequential within its slot. It
is independent of `MAX_CONCURRENT_INVOCATIONS` and of `MAX_BUFFERED_EVENTS`, and
it is **not** a host-wide resource quota: each worker enforces its own, so a
deployment of `N` workers can run up to `N * MAX_CONCURRENT_BUILDS` preparations
concurrently. It is startup configuration (restart to change). Only
runtime-backed preparations take a slot; a template-only (no-runtime) prepare
does not.

```
Redis stream → bounded local buffer → matcher/dispatcher →
worker concurrency → per-app concurrency → warm-container budget →
runner/container → ACK
```

When the local buffer is full the consumer stops reading, so the backlog stays
in Redis. A full concurrency slot leaves the message pending (no retry charged)
and a later reclaim replays it. A worker shutdown is treated the same way: a
lifecycle cancellation claims no handler attempt if it lands before the claim,
and writes no retry backoff or exhausted/DLQ marker if it lands after one, so
shutdown cannot consume the retry budget or dead-letter work (the stream leaves
the message pending either way). This is a statement about persisted invocation
state, not telemetry: a post-claim execution that reports an error during
shutdown is still counted as a handler failure.

Backpressure is local and two-stage. The bounded local buffer is independent of
execution concurrency: every message an `XREADGROUP` returns is added to the
Redis **PEL** before it executes, so it is delivered — not merely unread — the
moment it is read. When execution capacity is saturated but local buffer space
remains, the consumer may still read the next batch. A delivery that cannot
claim an execution slot within its wait is left **pending**: no execution
attempt, no retry accounting, no ACK, no DLQ. It releases its local processing
slot, so the next read can proceed. Under repeated saturation those released
slots let the consumer keep reading, so the **cumulative group PEL can grow past
the local buffer bound**; the local bound caps only what one worker holds at
once, never the group's outstanding deliveries.

That is not a loss: a pending message stays recoverable and is replayed by a
later reclaim. The operational consequences are the ones to size for — a larger
PEL means more Redis metadata and more reclaim work, a rising per-entry delivery
count, and extra latency, while a saturated or slow worker can hold a temporary
ownership imbalance of the group's backlog until it catches up. Recovery is the
reclaim loop, which re-delivers only entries idle longer than the configured
threshold: there is no immediate hand-off and no latency guarantee.
At-least-once delivery, no event loss, and the independence of retention from
ACK are unchanged — a slot-timeout delivery is never counted as a handler
attempt. Relay applies no per-app fairness policy: the global concurrency bound
is shared by whichever invocation acquires a slot (no reservation, aging, or
weighted scheduling), so a continuously busy app can delay another app's backlog
without a latency guarantee. Reclaimed pending work is processed on a single
reclaim goroutine, so one slow reclaimed handler delays the rest of that batch —
bounded by the handler timeout — while fresh reads continue on the consume loop.

### Warm-container bound

`MAX_WARM_CONTAINERS` (default `8`) is a **per-worker** hard bound on the warm
**execution** containers Relay keeps across every app — a count bound on the
pooled execution-container population (idle **plus** busy, plus in-flight
creates), independent of `MAX_CONCURRENT_INVOCATIONS` and of any single app's
`concurrency`. It exists because the per-app warm pools are otherwise unbounded in
aggregate: `10` apps each with `concurrency: 2` could keep `20` warm containers
alive even though at most `MAX_CONCURRENT_INVOCATIONS` invocations run at once.

Enforcement is global (not per app) and is applied **before** an invocation
claims a handler attempt:

- While the bound has room, an invocation is admitted and a container slot is
  reserved.
- At the bound, the globally **oldest idle** execution container (by last-use
  time, across all apps) is evicted to make room, with the
  `warm_eviction` discard reason. Only **idle** containers are ever evicted; a
  **busy** container is never killed, and neither are the persistent service
  containers (which are outside this bound).
- When the bound is full and every execution container is busy, no container can
  be evicted: the invocation is left **pending** (backpressure) and is replayed
  by a later reclaim, exactly like a full concurrency slot — it is **not**
  charged a handler retry and is not dead-lettered. This is the deliberate
  difference from a handler failure.
- The admission above is deliberately before the claim, so saturation itself
  never spends a handler attempt. A narrow race can still arise when admission
  succeeds (an idle container existed) but that container is taken before the
  create: the create then reports saturation **after** the attempt was already
  claimed. As with a crash between the claim and the handler start, that claimed
  attempt is spent (no failure/retry/DLQ is charged, the message stays pending and
  is replayed); the claim alone never exhausts, so only a later attempt that
  actually runs and fails can dead-letter the message. See
  [events.md](events.md#where-the-attempt-count-advances).
- A stale-version throwaway container (a request for an already-retired image) is
  **not** counted against the bound; the bound covers the regular pooled
  population only.
- A container counts against the bound until its physical removal is
  **confirmed**. When a teardown's Docker remove genuinely fails, the container
  is still poisoned against reuse but its slot stays reserved for the worker's
  lifetime; a fresh container is never admitted on that phantom capacity, so the
  bound is a true upper bound on live containers rather than on tracked ones.

The idle timeout (`WARM_CONTAINER_IDLE_TIMEOUT`) still evicts on age; the bound
is an additional cap that evicts the oldest idle container early only when a new
container actually needs the room. `MAX_WARM_CONTAINERS` is startup
configuration: changing it requires a worker restart. A value below `1` is a
fatal configuration error (zero is never read as "unbounded").

The `relay_runtime_warm_capacity` and `relay_runtime_warm_containers` gauges
report the configured bound and current usage; `relay_runtime_warm_waits_total`
counts invocations that had to wait at the bound.

### Hot reload

`HOT_RELOAD` (default `false`) controls how a runtime-backed app's **source**
reaches its containers.

- With `HOT_RELOAD=false`, the app's source is **baked into the immutable
  runtime image**. The image is the app's source identity: a source edit
  produces a new image version, and the previous version keeps serving until
  the new one is prepared and swapped in.
- With `HOT_RELOAD=true`, Relay builds the app image **without baking the
  source** and **source-mounts** the app's live directory under `/apps`
  read-only into its execution and entrypoint-service containers. A source-only
  change reuses the existing app and dependency images (neither is rebuilt) and
  takes effect by reloading or restarting the application process.

Source mounting applies to every runtime with a mountable dependency layout —
Python, whose dependencies install into the system site-packages _outside_ the
app directory, and Node, whose `node_modules` and persisted esbuild live under
`/app` while the source is mounted at the distinct `/app/src`. Python mounts at
the engine work directory (`/app`); Node mounts at `/app/src` so the bind never
hides `/app/node_modules`, and its bootstrap bundles any TypeScript handler at
container startup with the same pinned esbuild the baked build used. It is
startup configuration: changing it requires a worker restart.

Consequences:

- **Source edits do not rebuild.** The app image becomes a function of the
  runtime, dependencies, bootstrap, and entrypoint only, so a source-only change
  reuses the existing app and dependency images without rebuilding either. The
  reconciler still detects the edit from the source fingerprint and applies it by
  reloading or restarting the application process: a managed entrypoint service
  is **restarted in place** — the same Docker container ID, same image, same
  bind, with a brief stop/start — so its process re-reads the live source, and
  warm execution containers are recycled onto fresh containers, with idle
  containers discarded immediately and busy containers **drained**: an in-flight
  invocation finishes before its container is replaced, so a warm container that
  had already imported the old modules never serves stale code. Because Docker
  labels are immutable, the reconciler records the applied source fingerprint in
  memory so a restarted service is recognized as converged instead of being
  restarted on every pass; after a worker restart each stale service is restarted
  once more, then left alone.
- **Dependency changes still rebuild.** A changed manifest changes the content
  addressed dependency image and therefore the app image tag, exactly as before;
  the dependency lifecycle is unchanged.
- **Runtime support is per runtime.** Python and Node are mountable, with
  runtime-specific placements: Python mounts the source at `/app` (its packages
  live in the system site-packages, outside the mount); Node mounts at `/app/src`
  so `/app/node_modules` stays visible, resolves handlers from the mount, and
  transpiles TypeScript at container startup with the pinned esbuild persisted in
  the image (a JS-only Node app still carries the tool so a JS/TS source edit
  never changes the image). A runtime whose dependency layout cannot support this
  keeps the baked image.
- **Node runs at the app root and never shadows the dependency image.** A
  source-mounted Node execution or entrypoint-service container runs with
  `/app/src` as its working directory, so `process.cwd()` and relative paths see
  the app root. A source-mounted Node image carries a shared resolve hook at
  `/relay/resolve-hook.mjs`: the invocation bootstrap imports it, and an
  entrypoint service — which never runs the bootstrap — preloads it with
  `node --import /relay/resolve-hook.mjs`. The hook re-anchors bare imports and
  CommonJS requires from the mounted source to the dependency image's
  `/app/node_modules`, so a host copy can never shadow it. When the host app
  directory already carries a top-level `node_modules` at preparation time,
  Relay additionally masks that one bounded path (`/app/src/node_modules`) with
  an empty, read-only filesystem as best-effort defense-in-depth for resolution
  paths outside the hook. A host `node_modules` that appears after the app's
  last preparation (and is excluded from the source fingerprint, e.g. by
  `.gitignore`) carries no mask, but the preloaded hook still keeps the
  dependency image authoritative.
- **The bind path is resolved by the Docker daemon**, not by the Relay
  process. Relay normally passes the app's directory (e.g. `/apps/foo`), which
  the daemon resolves on its own host filesystem. When Relay itself runs as a
  container on the same daemon — the bundled Compose layout, where the host's app
  tree is bind-mounted at `/apps` inside Relay — Relay instead inspects its own
  container and maps `/apps/foo` to the inspected mount's host `Source` plus the
  relative path, so the daemon binds the exact same live tree, read-only. The
  resolver is conservative: it maps only a path covered by one of Relay's own
  bind mounts (the most-specific mount wins), it verifies the inspected container
  really is this Relay before trusting it, and any failure falls back to the plain
  app directory without mutating app data; the daemon's own source-existence
  validation remains the final authority. Native-host Relay and any container
  where no mapping can be established (for example a custom `hostname:`) keep the
  plain app directory. A daemon on a _remote_ host — not the daemon Relay itself
  runs on — still cannot see Relay's app tree and remains unsupported.

Operational caveats:

- The bind is mounted read-only, and invocation containers already run with a
  read-only root filesystem and a non-root user; the source must be readable by
  the container user.
- The whole app directory is mounted, including `template.yaml`, any `.git`
  metadata, and files excluded from the build by `.gitignore` (for example a
  local `.env`). `template.yaml` holds environment _values_ and secret
  _references_ (never resolved secret values), which the baked image
  deliberately omits. Enable `HOT_RELOAD` only for app directories you are
  willing to expose to the handler process and its container.
- `HOT_RELOAD` does not add anything to the application environment.

### Event size limit

`MAX_EVENT_BYTES` caps the byte length of a message's raw `event` **value**, not
the whole Redis entry: the entry and the go-redis response have already been
materialized before Relay can inspect the value, so this is a pre-decode
guard at the Relay boundary, not transport-level protection. The value is a
human-readable binary size — an integer with an optional case-sensitive binary
suffix `B`, `KiB`, `MiB`, or `GiB` (e.g. `256B`, `256KiB`, `1MiB`) — or a bare
integer byte count for backwards compatibility with the former numeric form
(e.g. `262144`). The default is `256KiB` (262144 bytes); the hard ceiling is
`1MiB` (1048576 bytes) and a larger value is a fatal configuration error.
Decimal suffixes (`KB`/`MB`/`GB`) are rejected so a size is never ambiguous, and
zero is **not** "unlimited": it is rejected, so the cap is always a real bound.
The default applies when the variable is unset or empty.

A delivered message whose raw `event` value exceeds the cap is rejected before
JSON decode, schedule classification, event matching, invocation-state
migration, and handler execution; it is routed non-retryably to the DLQ and the
message is left pending if the DLQ write fails, exactly like a malformed
message. See [events.md](events.md) for the DLQ summary semantics. Changing this
value requires a worker restart.

### Log levels

`LOG_LEVEL` filters Relay's own `slog` output only. Handler stdout/stderr is a
raw transport forwarded verbatim at every level, independent of `LOG_LEVEL`.
Fatal startup failures always log regardless of the configured level.

### Retention windows

Three independent retention windows trim or bound three different things. All
are **Go durations** parsed with `time.ParseDuration`; there is no day suffix
(`7d` is **not** valid), so a 7-day override is written `168h`. All are startup
configuration (restart to change) and none affects ACK, retry, DLQ routing, or
the scheduler/outbox. (A fourth window, `STATS_RETENTION`, bounds stale
PostgreSQL stats-owner rows; it is described after the three Redis windows.)

`REDIS_STREAM_RETENTION` (default `24h`) drives the main stream trim. A single
goroutine inside `relay start` periodically trims the configured source stream
with `XTRIM <stream> MINID ~ <cutoff> ACKED`, where `<cutoff>` is
`<now - retention>` in Unix milliseconds.

- The tick interval is derived automatically from the window (`retention / 24`,
  clamped to `[1m, 1h]`); it is not another variable.
- The trim is approximate (`~`) and runs in **`ACKED`** mode, not the Redis
  default `KEEPREF`: an entry is removed only once **every** consumer group on
  the stream has read and acknowledged it. A group that never drains therefore
  blocks trimming of the range it covers (the safe direction); pending entries
  and unread current groups are protected.
- **Requires Redis 8.2+.** On an older server Relay refuses to trim, logs
  `Retention: disabled`, and never falls back to a mode that could evict pending
  entries. This disables **only** the main stream trim; DLQ retention below is
  unaffected.

`REDIS_DLQ_RETENTION` (default `7d`, i.e. `168h`) drives the DLQ trim. The
Relay-owned `relay:<REDIS_STREAM>:dlq` stream has **no consumer group, PEL, or
XACK flow**, so it is trimmed **age-only**:

- A separate goroutine trims **only** that DLQ key with
  `XTRIM <dlq> MINID ~ <cutoff>` — no `ACKED` (or any) mode token — so it works
  on every Redis that supports `XTRIM MINID ~` and does not depend on the
  server's `ACKED` support.
- The cutoff and derived tick cadence are the same as the main stream
  (`<now - retention>`, `retention / 24` clamped to `[1m, 1h]`).
- This is a bounded failure-history window, **not** a per-entry TTL and not a
  replay guarantee: a DLQ entry older than the window is removed whether or not
  anyone inspected or replayed it. `relay dlq ls|inspect|replay|rm` still work on
  what remains.
- The two trims target disjoint keys: the main trim never touches the DLQ and
  the DLQ trim never touches the source. Neither changes ACK-before-DLQ
  ordering.

`REDIS_INVOCATION_RETENTION` (default `48h`) is **not a stream trim**: it is the
TTL applied to a message's per-invocation Redis hash (`relay:invocation:…`) only
**after the message has left the PEL** — a successful XACK on the success,
obsolete-schedule, or DLQ path, or a purged missing-payload PEL reference. For
every path that ACKs, the XACK and the retention are one atomic script, so no
post-ACK window can leave an unretained hash; a failed ACK stages only a
persistent marker (no TTL). While the message is recoverable (still in the PEL
and therefore redeliverable)
the hash is **persistent with no TTL**, regardless of this value; the window
starts only once the message is no longer recoverable.

- It bounds terminal bookkeeping (`ok` / `exhausted` / `exhausted:…:dlq`
  markers, plus the reserved classification/trace/schedule fields and the
  malformed-message DLQ placeholder field): how long a
  completed or exhausted invocation stays inspectable after its message left the
  PEL. It does **not** affect retries or ACK ordering.
- The TTL is applied atomically with the reserved terminal marker, and is
  monotonic: a repeated retention (e.g. a redelivery racing the ACK) never
  re-extends it, and a stale in-memory transition can neither mutate the
  terminal state nor remove the TTL.
- This window is **independent** of `REDIS_DLQ_RETENTION` (which trims DLQ stream
  entries) and of the scheduler's PostgreSQL outbox, whose 7-day retention is a
  separate, local mechanism. A dead-lettered entry and the terminal hash for the
  same message are two different Redis/PostgreSQL objects with their own windows.

**Unset vs. explicitly empty.** For all three windows, leaving the variable
**unset** applies the documented default (`24h` / `168h` / `48h`); setting it to
an **empty** (or whitespace-only) string (`REDIS_STREAM_RETENTION=`) disables
that retention (`0`). A malformed value and an explicit `0` or negative value log
a line naming the variable and disable that behavior; **retention values never
fail startup**.

`STATS_RETENTION` (default `7d`, i.e. `168h`) is **not a Redis trim**: it bounds
the persisted stats-owner rows in PostgreSQL. A `WORKER_ID` that is retired
permanently leaves its `stats`/`app_stats` rows behind, so those would otherwise
accumulate forever. After a successful
flush the worker deletes rows owned by **other** workers whose `updated_at` is
older than the window, throttled to one attempt per derived tick (`retention / 24`
clamped to `[1m, 1h]`).

- A worker never prunes its **own** row, however stale its timestamp looks, so a
  temporarily quiet worker keeps the cumulative totals it will resume writing.
- Because `WORKER_ID` is stable across restarts, a restarted worker reuses its
  row; this window still bounds identities that were retired permanently.
- `STATS_RETENTION` follows the same unset-vs-empty rule: unset applies `168h`,
  and an explicit empty or `0`/negative value disables compaction (no rows are
  ever deleted).

For `REDIS_INVOCATION_RETENTION`, a disabled value (`0`) still writes the
terminal marker atomically — every stale-transition guard is unchanged — but
leaves the hash **persistent** (no expiry), so terminal bookkeeping survives
until an operator removes the keys. Re-enabling the setting later does **not**
retroactively assign a TTL to those earlier persistent terminal hashes — there is
no sweeper, and only a newly written terminal marker receives the expiry — which
preserves terminal-marker atomicity instead of adding a global scan. The worker
passes the operator's resolved value through verbatim, so an explicit `0` is
never silently replaced by the `48h` default.

```bash
REDIS_STREAM_RETENTION=6h          # trim the source stream (ACKED)
REDIS_DLQ_RETENTION=168h          # trim the DLQ after 7 days (age-only)
REDIS_INVOCATION_RETENTION=12h    # expire terminal invocation state after 12h
STATS_RETENTION=720h              # compact stale stats-owner rows after 30 days
REDIS_STREAM_RETENTION=          # disable source retention
REDIS_DLQ_RETENTION=             # disable DLQ retention
REDIS_INVOCATION_RETENTION=       # keep terminal invocation state persistent (no TTL)
STATS_RETENTION=                  # disable stats-owner compaction
```

## Environment owned by other libraries

These are honored but are **not** Relay configuration — Relay does not parse
them, it just constructs the clients with `FromEnv` and lets the pinned
libraries read the standard variables:

- **Docker client:** `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`.
  Relay talks to the Engine API directly and negotiates the API version.
- **OpenTelemetry (traces):** `OTEL_EXPORTER_OTLP_ENDPOINT`,
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`,
  `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`, `OTEL_SDK_DISABLED`, `OTEL_SERVICE_NAME`,
  `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_EXPORTER_OTLP_HEADERS`,
  `OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_EXPORTER_OTLP_COMPRESSION`,
  `OTEL_EXPORTER_OTLP_CERTIFICATE`, `OTEL_EXPORTER_OTLP_INSECURE`, the matching
  `OTEL_EXPORTER_OTLP_TRACES_*` overrides, `OTEL_TRACES_SAMPLER` /
  `OTEL_TRACES_SAMPLER_ARG`, and the `OTEL_BSP_*` batch-processor knobs.

Tracing is disabled by default: Relay dials a collector only when
`OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set, and
`OTEL_SDK_DISABLED=true` forces it off. Only `grpc` and `http/protobuf` are
supported; any other non-empty protocol fails trace setup with a clear error
(non-fatal — the worker continues untraced). `OTEL_PROPAGATORS` is intentionally
not honored: Relay always installs the W3C TraceContext + Baggage propagators.
See [operations.md](operations.md) for the tracing and metrics surfaces.
