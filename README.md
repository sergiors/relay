# Relay

Relay is an event-driven function runner. It consumes events from a Redis
Stream, matches each event against declarative patterns, selects the matching
handlers, executes them with managed runtimes in isolated containers, and
acknowledges successfully processed messages.

Relay does not know or care where events originate. It only reads from Redis
Streams and runs functions:

```
Event Producer → Redis Streams → Relay → Function Handler
```

## How it works

At startup Relay discovers the functions under `/functions` (one directory
per function), builds one image per function, and creates (if missing) the Redis
consumer group. It then watches `/functions` for changes and reconciles each
function on the fly. It then blocks
on the stream with a consumer group, decodes each message, and for every event:

1. evaluates every function's rules (declarative patterns in `template.yaml`),
2. executes each matching handler **sequentially** in an isolated container,
3. acknowledges the message (XACK) only after **all** matching invocations
   succeed.

Relay is distributed as **one** binary. `relay start` runs the long-running
process — it consumes events, loads functions, builds images, and reconciles
`/functions` live, blocking in the foreground until signalled. The remaining
subcommands are administrative/inspection commands around the same binary;
they never start the runtime. The only things they write are the local secrets
store (`relay secret set/rm`) and — read-only otherwise — the state database
they read from.

```
relay start                # start Relay in the foreground
relay health               # check Relay dependencies (Redis, Docker)
relay stats                # show current operational statistics
relay function ls          # list functions
relay function inspect <name>
relay secret ls|set|rm     # manage local secrets
```

## How to run

```sh
# build the single binary
go build -o relay ./cmd
# or, from the module root with defaults
go build ./...

# run Relay in the foreground (reads REDIS_* from the environment)
./relay start
```

Relay runs **in the foreground** by design: `relay start` blocks until the
process is interrupted (SIGINT/SIGTERM). It does not background itself, write a
PID file, or fork. Background execution and supervision belong to Docker,
systemd, Kubernetes, etc. — e.g. `docker compose up -d`, `docker run -d ...`, or
`systemctl start relay` — not to Relay itself.

## Docker requirement

Relay talks to the **Docker Engine API** directly (via the moby client) to
build images and run containers; it does not require the Docker CLI to be
installed. A running Docker daemon must be reachable on the host. At startup
Relay pings the daemon and fails fast with a clear error if it cannot connect.
Image builds and handler invocations use the **local Docker daemon** on the
host that runs Relay. The client is configured from the environment
(`DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`) and negotiates the API
version automatically.

The daemon must be permitted to execute arbitrary containers, so Relay needs
full host-level Docker permission (e.g. the user running Relay must be a member
of the `docker` group or otherwise have access to the Docker socket).

When Relay itself runs inside a container, do **not** mount the host's Docker
socket into Relay. Instead, run Relay against a Docker-socket proxy that
allow-lists the Engine API endpoints Relay actually uses
(`DOCKER_HOST=tcp://socket-proxy:2375` over an internal network):

```yaml
services:
  socket-proxy:
    image: tecnativa/docker-socket-proxy
    environment:
      - PING=1
      - VERSION=1
      - BUILD=1
      - CONTAINERS=1
      - POST=1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro

  relay:
    image: relay:latest
    depends_on:
      - socket-proxy
    environment:
      - DOCKER_HOST=tcp://socket-proxy:2375
      - REDIS_ADDR=redis:6379
```

> **Security warning: the Docker socket is privileged.**
> The socket is mounted read-only into a dedicated proxy container, and the
> proxy forwards only a curated allow-list of Docker Engine API requests (an
> endpoint allow-list plus read-only access) instead of granting Relay the full
> socket. This reduces Relay's attack surface against the daemon, but it is
> **not** a strong security boundary and it does **not** make Docker execution
> unprivileged: Relay still builds images and creates/runs containers, which is
> effectively root-equivalent on the host. This tradeoff is accepted at this
> stage of the project so Relay can build and run function images against the
> host daemon. Separating the executor into a remote/privileged sidecar or a
> properly isolated build+run service is out of scope for this iteration.

### Development with Compose

`compose.dev.yaml` runs the Relay runtime in a container (the image's `CMD` is
`relay start`) alongside a Redis service and a Docker-socket proxy, so
you can develop against the same containerized deployment the README above
describes without installing Go or Redis locally:

```sh
docker compose -f compose.dev.yaml up --build -d
docker compose -f compose.dev.yaml logs -f relay
```

Relay does **not** mount the Docker socket. Instead the topology is:

```
Relay → socket-proxy → Docker daemon
```

Relay reaches the host daemon through the proxy over the internal compose
network with `DOCKER_HOST=tcp://socket-proxy:2375` (Relay's moby client honors
`DOCKER_HOST` via `FromEnv`). The socket itself is mounted **read-only and only
into the `socket-proxy` container**, which forwards just the Engine API
endpoints Relay needs (ping/version, image build, and container create/attach/
start/wait/kill/remove). Port `2375` is **not** exposed to the host, so the
proxy is reachable only from the compose network. `./examples/functions` is still
mounted read-only into Relay at `/functions`. The socket mount is privileged (see the security
warning above); this is a dev-only convenience. The Relay container's healthcheck
runs `relay health` (see _Health check_), so `docker compose ps` reports it
healthy only while both Redis and the Docker daemon are reachable. Tear down with
`docker compose -f compose.dev.yaml down -v`.

## Configuration

| Env var                  | Required | Description                                       |
| ------------------------ | -------- | ------------------------------------------------- |
| `REDIS_ADDR`             | yes      | Redis address or DSN (see below).                 |
| `REDIS_STREAM`           | yes      | Redis stream to consume.                          |
| `REDIS_GROUP`            | yes      | Consumer group name.                              |
| `REDIS_STREAM_RETENTION` | no       | Stream retention window; unset disables trimming. |
| `METRICS_ADDR`           | no       | Metrics listen address (default `:9090`).         |

The first three `REDIS_*` variables are required: Relay fails startup (exits
immediately) if any of them is unset or empty. `REDIS_STREAM_RETENTION` is
optional and enables internal stream retention (see below). `METRICS_ADDR` is
optional and must be a non-empty listen address when set (an empty value falls
back to the default); an unbindable address is logged and retried, never fatal.

### Stream retention

`REDIS_STREAM_RETENTION` is an optional duration (e.g. `6h`) that enables
internal, periodic trimming of the configured `REDIS_STREAM`. Relay runs the
retention job **internally** — there is no external cronjob and no per-message
timer. While `relay start` runs, a single goroutine with a periodic
`time.Ticker` trims the stream with `XTRIM <stream> MINID ~ <cutoff-id>`, where
`cutoff-id` is `<unix-milliseconds>-0` for `now - retention`. One initial trim
runs shortly after startup so an already-large stream does not wait a full
interval.

The tick interval is derived automatically from the retention window
(`retention / 24`, clamped to `[1m, 1h]`) — it is **not** another environment
variable. For `6h` that is 15 minutes. Trim failures are logged and retried on
the next tick; they never stop the worker.

The trim is **approximate** (`~`): Redis removes whole internal stream nodes
(listpack blocks of up to `stream-node-max-entries`, default 100), so entries
older than the window that share a node with fresh entries are removed on a
later pass rather than immediately. Repeated ticks make progress toward the
cutoff one node at a time; an entry may linger at most about one tick interval
plus one node past its expiry.

> **Warning: retention applies to the WHOLE stream, not just Relay's consumer
> group.** Other consumer groups on the same stream may lose unprocessed entries
> older than the window. Fan-out is preserved for entries inside the window.

Unset or empty `REDIS_STREAM_RETENTION` disables retention entirely (no
goroutine, no trims). A malformed duration or a zero/negative value fails
startup like any other configuration error.

`REDIS_ADDR` accepts either a plain address or a Redis DSN:

- `host:port` (e.g. `redis:6379`)
- `redis://user:password@host:port`
- `rediss://user:password@host:port` (TLS)

`DOCKER_HOST` (and the other Docker client variables `DOCKER_TLS_VERIFY`,
`DOCKER_CERT_PATH`) are consumed by Relay through the Docker client at startup
(see _Docker requirement_); Relay itself does not parse them.

### Health check

`relay health` is an operational/container healthcheck command. It checks the
two dependencies the runtime needs at startup — Redis connectivity (a PING to
`REDIS_ADDR`) and Docker daemon connectivity (an Engine API Ping) — and exits
`0` when both are reachable, `1` otherwise (reporting the first failing check to
stderr). It is **not** a public API and no HTTP server runs; it only creates
clients and pings, so it never starts consumption, loads functions, builds
images, or touches the state database. `compose.dev.yaml` uses it as the Relay
container's healthcheck.

Relay's reliability settings — retry/delivery limits and the recovery loop — are
fixed internals, not env-configurable. See _Reliability defaults_ below.

Multiple Relay instances may share the same `REDIS_GROUP` with different
consumer names to scale out consuming; each worker uses its hostname as its
consumer name automatically (the container ID / pod name under
Docker/Kubernetes), so replicas are distinct without any configuration. The
consumer group is created automatically (with `MKSTREAM`) if the stream or
group does not exist; the group is created at position `0`, so only messages
added after startup are consumed.

## Functions

Each direct subdirectory of `/functions` is one function. The directory name
is the function name. Function names must be valid: they must match
`[a-z0-9][a-z0-9._-]*`, be at most 63 characters, and not end in a dot (so
`user-events`, `welcome_email`, and `jobs.v2` are fine, while `User Events`,
`hello/world`, and `.hidden` are not). Validation happens at load time — a name
is never silently sanitized — so valid names are already safe to use as docker
image tags. Each function directory must contain a `template.yaml` that declares
which runtime to use and which events it handles.

- A directory without a `template.yaml` is ignored.
- An invalid name is logged and skipped — it never prevents Relay from starting.
- An invalid `template.yaml` is logged and skipped — it never prevents Relay
  from starting.
- A function whose image cannot be built is logged and marked unavailable; the
  other functions continue to be served.

### Image lifecycle

Function images are **versioned by source fingerprint**. Each function's content
is hashed (SHA-256 over file paths + bytes) and the image is tagged
`relay-fn-<name>:<first-16-hex-of-fingerprint>`; the full 64-hex fingerprint
stays authoritative in the local state database and on the prepared function.

- A rebuild produces a **new immutable image version**; an existing image for
  the exact fingerprint is reused without rebuilding.
- Relay swaps to the new version only after preparation succeeds — the old
  version keeps serving until then, and a failed build leaves the old version
  active.
- Old Relay-owned images are removed only once they are no longer in use
  (in-flight executions are protected), including on function removal and at
  startup.
- Relay manages **only its own `relay-fn-*` images** — it never prunes globally
  or touches other apps' images or layers.

### Execution container lifecycle

Every function execution container is created with Docker **AutoRemove**, so the
daemon removes the container once its process exits. Relay relies on AutoRemove
for the normal execution path and does not explicitly remove containers that
exit on their own.

Explicit removal is used only as a backstop for abnormal lifecycle paths where a
container may still be running or may never have started correctly, such as
start failures, timeouts, cancellation, or wait errors. Backstop removal is
idempotent: a container that was already removed, or is already being removed by
the daemon, is treated as successfully cleaned up.

Every execution container also carries seven **diagnostic-only** Docker labels —
`relay.function`, `relay.handler`, `relay.message_id`, `relay.event_id`,
`relay.event_name`, `relay.hostname`, `relay.image` — so an orphan container can
be attributed to its function/handler/message/event/worker/image. They carry no
payload contents (only bounded IDs and function/handler names) and Relay's
event-processing correctness never depends on them.

At startup, before any function is prepared or any container created, Relay runs
a conservative **orphan sweep** (bounded to 30s): it lists containers and removes
only those carrying the `relay.function` label **and** a `relay.hostname` label
matching its own hostname (= its Redis consumer name) — leftovers from a previous
crashed process on the same host. It removes both exited and running stale
containers (running ones are force-removed/stopped). It **never** touches
containers owned by other hostnames (another replica's property, live or crashed)
or any non-Relay container, and does no global pruning.

Every execution container is hardened: it runs as a non-root user (uid 10001,
baked into the generated image), is limited to 512 MiB memory / 1 CPU / 128 PIDs,
drops all Linux capabilities, has a read-only root filesystem with a bounded
`/tmp` tmpfs, and keeps outbound networking enabled (a documented residual, not a
sandbox for untrusted code).

## Template format

```yaml
runtime: python3.14

events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
      table_name: [users]

  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
      table_name: [users]
    timeout: 20s

  - handler: events.deleted.handler
    pattern:
      event_name: [REMOVE]
      table_name: [users]
```

- `runtime` (required) selects the execution runtime. Only `python3.14` and
  `node24` are supported; any other value fails validation.
- `events` is a list of rules. Each rule has a required `handler` (of the form
  `module.function`), a required `pattern`, and optional `timeout` and `retries`.
- `timeout` (optional, per rule) is a Go duration string bounding a single
  invocation of that rule's handler (e.g. `20s`, `1m30s`). It must be positive.
  Zero, negative, unparseable, or values above `5m` (`MaxTimeout`) fail the
  function's template validation (the function is logged and skipped). Omitted
  rules use a `6s` default.
- `retries` (optional, per rule) is the number of additional executions attempted
  after the initial one (`0` = only the initial attempt). It must be a
  non-negative integer; a negative or non-integer value (e.g. `-1`, `abc`,
  `1.5`) fails template validation. Omitted rules use a `4` default, so a
  failing invocation is attempted `1 + 4 = 5` times in total before it is
  considered exhausted and the message is routed to the DLQ.
- `handler` is split at the **last** dot: `events.created.handler` → module
  `events.created`, function `handler`. Handlers may live in nested modules
  (for example the `events/` package), not only in top-level files.

### Environment variables and secrets

A template may define per-function environment variables and secret references:

```yaml
runtime: python3.14
env:
  API_URL: https://api.example.com
secrets:
  DATABASE_URL: database-url

events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
```

- `env` (optional) maps an env-var name to a **literal string value**, injected
  into every execution container at runtime. Values are literal — never masked,
  never treated as secret-looking. Empty values are allowed (flag-like
  variables). Env-var names must match `[A-Za-z_][A-Za-z0-9_]*`.
- `secrets` (optional) maps an env-var name to a **secret reference name**. The
  reference is resolved to a value immediately before each execution and
  injected into the container. Secret references must be valid secret names
  (lowercase letters, digits, `.`, `_`, `-`; no leading or trailing `.`; at
  most 63 chars).
- A variable may not be defined in both `env` and `secrets`.
- `RELAY_HANDLER` is reserved by Relay (it carries the rule's handler identity);
  a template may not set it.
- **Never put secret VALUES in `template.yaml`.** The template holds only the
  reference name; the value lives in the secrets store (see `relay secret`
  below). Editing `template.yaml` (including env values) changes the function's
  fingerprint and triggers a rebuild by design; rotating a secret **value**
  never changes the fingerprint and never requires a rebuild or restart.
- Env values and secret references are injected at runtime only — they are never
  baked into the function image (template.yaml is excluded from the build
  context), never stored in the state database, and never logged.

A pattern is a tree of field conditions:

- A plain YAML list is implicit equality: `status: [COMPLETED, FAILED]`.
- A map with only operator keys (`equals`, `prefix`, `suffix`, `exists`,
  `gt`/`gte`/`lt`/`lte`) holds operators.
- A nested map without operator keys holds nested field conditions.

### Operators

| Operator | Semantics                                                           |
| -------- | ------------------------------------------------------------------- |
| `equals` | Value equals any of the listed values (type-preserving).            |
| `prefix` | String value starts with any of the listed prefixes.                |
| `suffix` | String value ends with any of the listed suffixes.                  |
| `exists` | Key presence check. Takes a boolean, not a list.                    |
| `gt`     | Value is greater than a numeric threshold or `now()`-relative cutoff.      |
| `gte`    | Value is greater than or equal (numeric or `now()` cutoff).               |
| `lt`     | Value is less than a numeric threshold or `now()`-relative cutoff.       |
| `lte`    | Value is less than or equal (numeric or `now()` cutoff).                 |

#### Comparison operators (gt/gte/lt/lte)

`gt`, `gte`, `lt`, and `lte` take either a **number** (compared numerically) or
a **`now()`-relative expression** (compared as instants). Operands may be a
list, in which case the value matches if any operand holds (ordinary OR).

```yaml
pattern:
  new_image:
    created_at:
      gt: "now()-5m"
```

- `now()` expressions: `now()`, `now()-5m`, `now()+10m`, `now()-1h`,
  `now()+24h`, and any Go `time.ParseDuration` suffix (compound forms like
  `now()-1h30m` are fine). They are evaluated as a UTC instant **at match time**
  (never at template load), so the cutoff moves as time passes. `now()+0s` (zero
  offset) is valid.
- Only the exact `now()`/`now()±duration` syntax triggers temporal comparison.
  The event value must be an **RFC3339 string**; offsets are respected and
  values are compared as instants, not lexicographically. A numeric operand
  stays purely numeric.
- The template pattern never accepts an arbitrary timestamp string (e.g.
  `gt: "2026-09-12T10:00:00Z"`); only `now()`-syntax is valid for string
  operands, and anything else fails template validation rather than silently
  never matching. The old bare `now` / `now-5m` syntax is **not** accepted and
  is a validation error; rewrite it as `now()` / `now()-5m`.
- A missing, `null`, non-string, or invalid-RFC3339 event value never matches a
  temporal comparison (and never panics).

Examples:

```yaml
pattern:
  price:
    lte: 100.5           # numeric: value <= 100.5
  age:
    gt: 18               # numeric: value > 18
  created_at:
    gt: "now()-5m"       # temporal: value after (now() - 5m)
  updated_at:
    gte: "now()-1h"
    lt: "now()"          # two operators on one field are OR, not a range
```

`exists` checks **key presence only** — the value is irrelevant. `null` still
counts as "exists":

```yaml
pattern:
  new_image:
    name:
      exists: true # new_image.name must be present (any value, incl. null)
```

```yaml
pattern:
  new_image:
    name:
      exists: false # new_image.name must be absent from new_image
```

`exists` works recursively at any nesting depth: `new_image: { exists: true }`
checks the top-level `new_image` key; `new_image: { name: { exists: true } }`
checks `name` inside `new_image`. A nested `exists: true` fails when a parent is
missing (the nested key cannot be present). A nested `exists: false` matches
when the nested key is absent — including when the parent map itself is missing
(a missing parent means the nested key is necessarily absent).

The value must be a strict boolean; `exists: "true"`, `exists: 1`, and
`exists: null` fail template validation.

### Semantics

- Values within one operator's list are **OR**.
- Different fields (sibling keys) are **AND**.
- Multiple operators on the same field are **OR** (alternatives).
- Each rule's pattern is evaluated independently; multiple rules may match the
  same event, and no deduplication is performed (two matching rules referencing
  the same handler are both invoked).
- A missing event field fails that condition.
- Extra event fields are ignored.
- `prefix` and `suffix` only match string values; non-string values never match.
- `exists` composes with other operators on the same field under the normal OR
  rule: `name: { exists: false, prefix: ["12"] }` matches when `name` is absent
  **or** present with a value starting with `12`.

For example, a rule with `status: [COMPLETED, FAILED]` matches while
`id: { prefix: ["user_"] }` matches `user_123` but not `123`. The implemented
operators are `equals`, `prefix`, `suffix`, `exists`, and `gt`/`gte`/`lt`/`lte`.

## Supported runtimes

| Runtime      | Base image         | Dependency handling                                                                                  |
| ------------ | ------------------ | ---------------------------------------------------------------------------------------------------- |
| `python3.14` | `python:3.14-slim` | `requirements.txt` → `pip install --no-cache-dir -r requirements.txt`                                |
| `node24`     | `node:24-alpine`   | `package-lock.json` → `npm ci --omit=dev`; else `package.json` → `npm install --omit=dev`; else none |

Base images are fixed; arbitrary base images are not allowed. Dependencies are
installed **inside** the image at build time, never on the host.

For Node functions, if no `package.json` exists a minimal `{"type":"module"}`
package.json is injected so `.js` files are treated as ESM; if a user
`package.json` exists, its `type` field is respected.

Both runtimes support **sync and async** handlers. The Python bootstrap invokes
the handler and, if the result is awaitable, runs it with `asyncio.run`; the
Node bootstrap resolves the handler module by filesystem lookup (not by
attempting an import), imports it, and awaits a returned Promise.

Adding a future runtime (e.g. `python3.15` or `node26`) requires only a new
entry in the runtime registry map; the engine is reused.

## Handler contract

Each execution runs a container with the environment variable
`RELAY_HANDLER` set to the rule's handler (e.g. `events.created.handler`) and
the event JSON written to the container's stdin. An embedded bootstrap resolves
the module and function from `RELAY_HANDLER`, reads the event from stdin, and
invokes the function. The container's exit code decides the result: `0` is
success, non-zero is failure. The container's stdout and stderr are forwarded
to Relay's logs.

## Execution

- **One image per function**, never per handler or event. A function's single
  image is built at startup and, afterwards, rebuilt only when its directory
  changes (see _Hot reload_ below); the rebuilt image serves all of its
  handlers.
- **Sequential execution**: for each event, functions are iterated in order,
  then rules in order, and each matching handler runs one at a time (no
  concurrency).
- **Timeout**: each invocation is bounded by the matching rule's `timeout`
  (default `6s`). A timeout kills the invocation and is treated as an execution
  failure. Multiple matching rules each use their own rule's timeout.
- **Retries**: each rule's `retries` (default `4`) controls how many additional
  executions are attempted after the initial one. A failing invocation is
  retried with a per-invocation backoff (1m, 2m, 5m, then 10m capped) until its
  `1 + retries` attempts are exhausted, at which point the message is routed to
  the DLQ (see _Recovery and retries_ below).
- **Failure**: any non-zero container exit is a failure; errors include the
  function and handler names. **Abort on first failure**: a failed invocation
  stops the remaining rules for that event and returns the message to the
  pending entries list (no XACK). See _Acknowledgment semantics_ below.

For example, `examples/functions/user-events-python/` declares three handlers
(`events.created.handler`, `events.updated.handler`,
`events.deleted.handler`), all served by the same function image.

## Hot reload

Relay watches `/functions` (with `fsnotify`) and reconciles each function on the
fly, without a restart:

- **Auto-discovery**: a new directory under `/functions` is detected and its
  image built, then it starts matching events.
- **Per-function rebuild on change**: edits to a function's template, source, or
  dependency files trigger a rebuild of _that function's_ image only. Events are
  debounced (750ms) so a burst of editor saves coalesces into one rebuild.
- **Fingerprinting**: each function's content is hashed (`SHA-256` over file
  paths + bytes); an unchanged function is skipped, so a rebuild happens only
  when its inputs actually changed.
- **Failure safety**: if a rebuild fails (invalid template or failed image
  build), the previous, still-working version is retained and keeps serving
  events. It is retried on the next change or periodic pass.
- **Removal**: deleting a function's directory removes it from matching.
  In-flight invocations are never interrupted; they finish against the snapshot
  they started with.
- **Missing template**: a directory present but with no `template.yaml` yet is
  treated as "not ready" — Relay waits for more events rather than dropping a
  previously-active function.
- **Periodic fallback**: a 30s reconciliation pass re-scans `/functions` as a
  backstop for watch events that were missed.
- **Nested directories**: the watcher covers files in nested subdirectories of a
  function, so sources split into packages are tracked too.

Functions are read-only to Relay (the directory is mounted read-only in the
container); all rebuilds happen in temporary build contexts, so Relay never
writes into `/functions`.

## Local state database

Relay keeps a small local **SQLite** database describing its current view of the
loaded functions — a read-only state view, **not** the source of truth. The
`/functions` directory remains authoritative; the local state database is
rebuilt automatically when empty and never drives matching, image building, or
reconciliation. It exists so operators can introspect what Relay has loaded and
how the last reconcile of each function went without touching Redis or Docker.

- **Location**: `/var/lib/relay/db.sqlite3` (a fixed internal path, not
  env-configurable). The parent directory is created automatically, so the file
  also works for host-side runs. It is **not** external infrastructure — it is
  a local file you can volume-mount to persist across restarts. `compose.dev.yaml`
  mounts a named volume `relay-data` at `/var/lib/relay`.
- **Schema**: a `functions` table (name, runtime, status, image, fingerprint,
  prepared_at, last_reconcile_at, last_reconcile_status, last_error, updated_at,
  env, secrets), a `handlers` table (function_name, handler, timeout), a
  single-row `stats` table (current global operational counters plus backlog
  gauges and `updated_at`), and a `function_stats` table (per-function counters
  and `updated_at`). The `env` and `secrets` columns store the function's
  env/secret **mappings** (JSON) — never secret values. These are **current
  snapshots only** — no per-event rows, no metric history (Prometheus is the
  time-series source).
- **State model**: `status` is `ready` (an active version is built and serving)
  or `pending` (loaded but not yet built). `last_reconcile_status` is
  `success` / `failed` / `skipped`. A **failed rebuild never marks a whole
  function unavailable**: the previously active image and fingerprint are
  retained, so the last good version keeps serving while `last reconcile` shows
  the failure. All timestamps are RFC3339.
- **Fault-tolerance**: state errors are logged and never fatal — Relay runs
  without the state database if the DB is missing or broken (Open recreates a
  missing DB).
- **Stats data flow**: operational stats accumulate **in memory** in the
  Prometheus registry (the single source of truth); `GET /metrics` reflects
  them immediately. SQLite receives the current **absolute snapshot** every
  5 seconds (fixed, non-configurable) — snapshots, not history. `relay stats`
  reads the latest global snapshot (may lag live Prometheus by up to ~5s);
  `relay function inspect <name>` reads the latest persisted per-function
  stats. Graceful shutdown performs a final bounded flush; a hard crash may
  lose up to ~5s of telemetry. Redis event-processing correctness never depends
  on SQLite stats.

Relay persists state at startup and on every reconcile. Three read-only CLI
commands expose it (no Redis, Docker, or `/functions` needed — they read the
state database file only):

```sh
relay function ls
```

```
NAME                  RUNTIME      STATUS    HANDLERS   UPDATED
user-events-python    python3.14   ready     3          12s ago
welcome-email-node    node24       ready     1          12s ago
```

The `UPDATED` column is `prepared_at` (else `updated_at`) as a relative age
(`12s ago`, `3m ago`, `2h ago`, `5d ago`), falling back to an absolute date
beyond ~30 days. Rows are sorted by name; fingerprints, images, and errors are
deliberately omitted from `ls`.

```sh
relay function inspect user-events-python
```

```
Name:              user-events-python
Runtime:           python3.14
Status:            ready
Image:             relay-fn-user-events-python
Fingerprint:       <sha256>
Prepared:          2026-09-08T12:00:00Z (12s ago)
Last reconcile:    success (12s ago)
Last error:        <error>

Handlers:
  events.created.handler   timeout=6s
  events.updated.handler   timeout=20s
  events.deleted.handler   timeout=6s
```

The `Image`, `Fingerprint`, and `Prepared` lines are omitted while a function is
`pending` (never built); the `Last error` line is omitted when there is none. A
failed reconcile with an active version keeps `Status: ready` and shows
`Last reconcile: failed (...)` — the function is never marked unavailable.

`relay function inspect <name>` also includes the current per-function
operational stats (zeros until the function has processed events):

```
Stats:
  Events processed:    12493
  Handler successes:   12470
  Handler failures:    23
  Retries:             17
  DLQ entries:         2
```

`relay function inspect <name>` also shows the function's env and secret
**mappings** (from its template) when it defines any — literal env values and
secret references, never secret values:

```
Environment:
  API_URL=https://api.example.com

Secrets:
  DATABASE_URL=database-url
```

## Secrets

Relay stores secrets as files on disk, one per secret, under a fixed directory.
Templates reference secrets by name; the runtime resolves each reference to its
value immediately before an execution and injects it into the container's
environment. Resolved values live only in the container's `Config.Env` — they
are never baked into images, never stored in the state database, never logged,
and never shown by `relay function inspect` (which shows only the reference).

- **Location**: `/var/lib/relay/secrets` (a fixed internal path, not
  env-configurable). The directory is created on first write with mode `0700`;
  each secret file is written atomically with mode `0600`. `compose.dev.yaml`
  mounts the named volume `relay-data` at `/var/lib/relay`, so secrets survive
  container restarts. **Deleting the volume deletes the secrets.**
- **Rotation**: changing a secret's value takes effect on the next invocation —
  no rebuild, no restart, no fingerprint change. Secrets are resolved per
  execution.
- **Provider**: the local filesystem provider is the current (single-host, beta)
  implementation. The provider interface is deliberately tiny so a future
  external provider (Vault, a secrets API, ...) can be added without changing
  templates or the runner.

Manage secrets with the `relay` CLI:

```sh
relay secret ls
```

```
NAME
database-url
api-key
```

```sh
relay secret set database-url
```

`relay secret set NAME` reads the value from the terminal with echo disabled
(hidden), or — when stdin is not a terminal — from all of stdin (the safe
non-interactive path, e.g. `printf 'value' | relay secret set foo`). The value
is never echoed and never printed.

```sh
relay secret rm database-url
```

### Security model

- Secret **values** are never stored in SQLite, never baked into images, never
  in labels, logs, metrics, or `relay function inspect` output. They exist only
  as files under `/var/lib/relay/secrets` (mode `0600`) and, transiently, in the
  environment of a **running** execution container (visible via
  `docker inspect` of that running container only).
- Secret **references** (the names) are configuration metadata: they appear in
  `template.yaml`, in the fingerprint, and in `relay function inspect`.
- **Never put secret VALUES in `template.yaml`** — the template is copied into
  the function's build context and its content is fingerprinted.

## Observability

Relay's observability is logs plus Prometheus metrics plus the local state
snapshot. There is no HTTP health/readiness endpoint — `relay health` (above)
remains the health check.

- **Structured logs**: execution, retry, failure, DLQ,
  reconciliation, and build lines carry logfmt fields — `function`, `handler`,
  `message_id`, `event_id`, `event_name`, `attempt`, `duration`, and container
  `exit_code` where available. Handler stdout/stderr is still forwarded
  verbatim.
- **Prometheus metrics**: the Relay runtime exposes `GET /metrics` on
  `METRICS_ADDR` (default `:9090`) in Prometheus text format via the official
  Prometheus client. Counters: `events_received_total`, `events_processed_total`,
  `handler_success_total`, `handler_failure_total`, `retries_total`,
  `dlq_entries_total`, `handler_invocations_total{outcome,function,handler}`,
  `build_failures_total{function}`, and per-function
  `function_*_total{function}` counters. Histograms:
  `handler_duration_seconds{function,handler}`,
  `function_build_seconds{function}`. Gauges: `pending_entries`,
  `pending_oldest_age_seconds` — sampled from the Redis consumer group
  (`XPENDING`) every 15s, not per event. Labels are bounded to
  `function`/`handler`/`outcome`; IDs (message, event, container, fingerprint)
  are never labels. The metrics server is operationally isolated: bind failures
  are logged and retried, scrape errors never stop event consumption, and
  shutdown is graceful. Prometheus is the source for time-series metrics.
- **SQLite operational snapshots**: the local state database also keeps the
  **current** operational counters (`stats`) and per-function counters
  (`function_stats`) — latest totals only, never history or per-event rows. The
  worker flushes the in-memory registry into SQLite every 5 seconds (fixed,
  non-configurable), so these rows may lag live Prometheus by up to ~5s; a
  graceful shutdown performs a final bounded flush, while a hard crash may lose
  up to ~5s of telemetry. A read-only CLI command renders the global snapshot:

```sh
relay stats
```

```
Events processed:    152934
Handler successes:   152801
Handler failures:    133
Retries:             82
DLQ entries:         4
Pending entries:     17
Oldest pending age:  2m14s
Updated:             10s ago
```

`relay stats` reads the state database file only (no Redis, Docker, or
`/functions`); it works even when the runtime is down. A fresh database renders
zeroes with `Updated: never`. Backlog gauges (`pending_entries`,
`oldest_pending_age_seconds`) are global — the consumer-group backlog is not
attributed to individual functions.

## Acknowledgment semantics

A message is acknowledged (XACK) only after **all** matching invocations
succeed:

```
event → handler A ✓ → handler B ✓ → ... → XACK
```

If any invocation fails (or times out), the message is **not** acknowledged and
remains pending for redelivery (at-least-once semantics):

```
event → handler A ✗ → STOP → no XACK (message stays pending)
```

An event that matches no rules is a success and is acknowledged. Because
delivery is at-least-once, handlers should tolerate duplicate delivery.

### Recovery and retries

Relay consumes with a consumer group, so every delivered message records an
entry in the group's Pending Entries List (PEL) until it is acknowledged. A
message a consumer reads but never acknowledges — a crash, an outage, or a
handler failure — stays in the PEL.

- **Recovery loop** (`XAUTOCLAIM`): a background goroutine runs every
  `DefaultReclaimInterval` (1m) and reclaims messages that have sat pending for
  longer than `MinPendingIdle` (default 1m). This is a message-level
  recovery-pacing backstop, not the concurrency guard and not the retry timer:
  reclaiming transfers ownership of the message and replays it through the same
  processing path as a fresh read, but whether an individual handler actually
  executes is decided per-invocation from the invocation state (see below). Rule
  timeouts are capped at 5m (`timeout` values above `5m` fail template
  validation). This makes Relay survive restarts: a message left pending by a
  dead consumer is picked up and retried by a live one.
- **Retry counting**: the per-message delivery count is read from Redis
  (`XPENDING` full form / retry counter), not kept in process memory, so the
  count survives restarts. Each reclaim of an idle message increments the count.
  Retry timing is defined per-invocation, not by the reclaim cadence: a failed
  attempt records a `next_attempt_at` deadline in the invocation state, and a
  redelivery before that deadline is skipped. Actual retry timing is quantized
  by the reclaim cadence (~1m granularity), so a 1m backoff effectively fires at
  the first redelivery after 1m.
- **Per-invocation retries and backoff**: each rule's `retries` (default `4`)
  bounds the number of additional executions after the initial one. A failing
  invocation is retried with a fixed backoff schedule — 1m after attempt 1, 2m
  after attempt 2, 5m after attempt 3, then 10m (capped) — persisted as a
  `next_attempt_at` marker in the invocation state. Once `1 + retries` attempts
  are exhausted, the invocation is marked `exhausted` (terminal).
- **Exhaustion → DLQ**: when every non-complete matched invocation is exhausted,
  the whole message is written to the dead-letter stream `<stream>:dlq` and the
  original is then acknowledged, removing it from the PEL. Per-invocation DLQ is
  not claimed; exhaustion of the last runnable invocation routes the message.
- **DLQ entry format** (flat fields): `original_stream`, `original_id`,
  `group`, `consumer`, `event` (the original payload string), `reason`,
  `attempts`, `timestamp` (RFC 3339).
- **DLQ write ordering**: the DLQ is written _before_ the original is
  acknowledged. If the DLQ write fails, the original is left pending so the next
  recovery cycle retries the DLQ write instead of losing the message.
- **Non-retryable failures**: a message whose `event` field is missing, is not a
  string, or is not a JSON object can never succeed. It is routed straight to
  the DLQ on first encounter — without running any handler — and acknowledged.
- **Malformed input** never consumes retry cycles.

These recovery defaults are a fixed part of the stream package and cannot be
overridden by environment variables. A zero-valued `ConsumerConfig` field
falls back to them in `NewConsumer`.

The at-least-once contract from the ACK table above is unchanged: XACK happens
only after all matching invocations succeed or the message is successfully
routed to the DLQ. To avoid re-running work that already succeeded, Relay records
per-handler invocation state in Redis: each message has a TTL'd hash keyed by
message (`relay:invocation:{stream}:{group}:{msgID}`, field
`<function>/<handler>`; stream/group names are percent-encoded in the key). The
field value describes the invocation's lifecycle for this message:

- `ok` — the invocation completed on a previous delivery; redeliveries skip it.
- `running:<unix-nano deadline>#<attempts>` — an attempt is (or was) executing,
  protected until that absolute deadline; `<attempts>` is the 1-based attempt
  number. A deadline marker without `#<attempts>` does not parse (treated as
  absent/eligible).
- `next_attempt_at:<unix-nano deadline>#<attempts>` — a failed attempt is
  waiting out its retry backoff, protected until that absolute deadline.
- `exhausted:<attempts>` — the invocation's attempts are exhausted; it is
  terminal and never eligible again (skipped like complete, but distinct so the
  runner can tell a message whose invocations are all terminal).
- absent — eligible to execute.

Before executing an invocation, the runner claims it via `TryStart`, which
persists `running:<now+timeout>#<attempts>` — the same capped timeout the local
`context.WithTimeout` enforces, so the persisted deadline and the local timer
match by construction. On success `MarkComplete` overwrites the marker with `ok`;
on failure `RecordFailure` persists `next_attempt_at:<now+backoff>#<attempts>`
with the rule's backoff (1m/2m/5m/10m); once attempts are exhausted
`MarkExhausted` writes `exhausted:<attempts>`. Another worker that redelivers
the message while `now < running_until` or `now < next_attempt_at` skips that
invocation, because a live attempt (this or another replica) may be executing it
or it is waiting out its backoff. A crashed worker's marker self-expires at its
deadline, so recovery waits it out (bounded by at most one timeout) instead of
racing a live attempt. Bookkeeping failures fail open: a Redis error on the read
or write never blocks delivery, preserving at-least-once. The message is
acknowledged once all matching invocations are complete, and the invocation-state
key is cleared on completion or DLQ routing. The keys expire after 7 days as a
fallback cleanup for abandoned messages.

This is still at-least-once, not exactly-once: there is a crash window between a
handler's side effect and its state being recorded, so a handler can still run
twice. Handlers must therefore remain idempotent. Renaming a function or handler
invalidates old invocation state (old entries simply never match), and a rule
removed from a template no longer gates the acknowledgment.

## Example functions

The repository's `examples/functions/` directory contains development / example
functions to copy and adapt. These are **samples for local development and the
README examples — not the production function directory**. Relay always reads
its functions from `/functions` inside the container; for production, mount
your own function directory there (see the deployment section above).
`examples/functions/` exists only so the repository ships working, testable
examples; each direct subdirectory is one function, deployed as described
above.

- `examples/functions/user-events-python/` (python3.14): three rules on the
  `users` table, each mapping an event to a module inside the `events/`
  namespace package (no `__init__.py`):
  - `events.created.handler` on `event_name: INSERT`.
  - `events.updated.handler` on `event_name: MODIFY` (with an explicit
    `timeout: 20s` demonstrating the per-rule timeout).
  - `events.deleted.handler` on `event_name: REMOVE`.

  Each module defines a single `handler(event)` function. `created`/`updated`
  read `event["new_image"]`; `deleted` reads `event["old_image"]`. Plain stdlib
  only, no `requirements.txt`.

- `examples/functions/welcome-email-node/` (node24): a single rule
  `handler.handler` on `event_name: INSERT` / `table_name: users`. The handler
  reads `event.new_image` and logs a welcome email. No `package.json` is
  provided, so Relay injects the ESM `package.json`. It omits `timeout`, so it
  exercises the `6s` default.

A single generic, cross-engine event matches both functions:

```json
{
  "event_id": "evt_123",
  "event_name": "INSERT",
  "table_name": "users",
  "new_image": {
    "id": "user_123",
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

It matches `user-events-python` → `events.created.handler` (Python, prints the
user id) **and** `welcome-email-node` → `handler.handler` (Node, logs the
welcome email).

## Try it

With Relay consuming stream `events` in group `relay` (as configured in
`compose.dev.yaml`), add the example event with `redis-cli`:

```
redis-cli XADD events '*' event '{"event_id":"evt_123","event_name":"INSERT","table_name":"users","new_image":{"id":"user_123","name":"John Doe","email":"john@example.com"}}'
```

Relay then logs the matched rules and the forwarded handler output:

```
relay: function "user-events-python" rule "events.created.handler" matched event "<msg-id>"
relay: function "user-events-python" handler "events.created.handler": User created: user_123
relay: function "user-events-python" handler "events.created.handler" executed for event "<msg-id>"
relay: function "welcome-email-node" rule "handler.handler" matched event "<msg-id>"
relay: function "welcome-email-node" handler "handler.handler": Sending welcome email to john@example.com
relay: function "welcome-email-node" handler "handler.handler" executed for event "<msg-id>"
```

## Out of scope

Custom images/Dockerfiles, other runtimes, pyproject/uv/poetry/pnpm/yarn/bun,
concurrency, warm containers, build caching, source hashing, git,
registries, k8s, configurable retry _policies per rule_ (delays/attempt counts
are fixed internals — a rule's `retries` count is configurable, the backoff
schedule is not), idempotency, exactly-once, per-function
resource limits/networking, external secret-management providers (Vault/AWS/K8s
— the local file provider is the current backend), HTTP API (beyond the
Prometheus `/metrics` scrape endpoint), UI, full observability platforms
(tracing, log shippers), and additional operators
(anything-but/regex/glob/scripts) are not implemented in this iteration.
