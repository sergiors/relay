# Configuration

Relay reads its own settings from the environment once, at the executable
boundary (`internal/config`). A missing required variable or an invalid tuning
value is returned to `cmd/main.go`, which prints it once and exits non-zero.

Every setting below maps 1:1 to an environment variable. Relay does **not** read
the Docker client or OpenTelemetry variables itself: those are consumed by the
pinned libraries (see the last section) and are listed separately.

## Required

| Variable       | Format                                                                  | Behavior                                                                                                                                                    |
| -------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REDIS_URI`    | `host:port`, `redis://[user:password@]host:port`, or `rediss://…` (TLS) | Redis address or DSN. Missing/empty fails startup.                                                                                                          |
| `REDIS_STREAM` | stream name                                                             | Stream Relay consumes. Missing/empty fails startup.                                                                                                         |
| `REDIS_GROUP`  | group name                                                              | Consumer group. Missing/empty fails startup. Created with `MKSTREAM` at position `0` if absent, so a new group over an existing stream replays its backlog. |

The consumer name is **not** configurable: Relay uses the process hostname
(container ID / pod name), so replicas sharing `REDIS_GROUP` are distinct
automatically.

## Optional

| Variable                      | Default            | Format / accepted values                                                | Behavior when invalid                                                                                                                                                                                                       |
| ----------------------------- | ------------------ | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOG_LEVEL`                   | `INFO`             | `DEBUG`, `INFO`, `WARN`, `ERROR` (case-insensitive, trimmed)            | Fails startup. `WARNING` is **not** an alias.                                                                                                                                                                               |
| `MAX_CONCURRENT_INVOCATIONS`  | `8`                | positive integer                                                        | Fails startup. Bounds concurrent invocations **per worker**.                                                                                                                                                                |
| `MAX_CONCURRENT_BUILDS`       | `2`                | positive integer                                                        | Fails startup. Bounds concurrent runtime-backed image-preparation pipelines **per worker**.                                                                                                                                  |
| `MAX_BUFFERED_EVENTS`         | `16`               | positive integer                                                        | Fails startup. Bounds messages read from Redis and held locally per worker.                                                                                                                                                 |
| `MAX_EVENT_BYTES`             | `262144` (256 KiB) | positive integer bytes, hard max `1048576` (1 MiB)                      | Fails startup on zero/negative/non-integer/>1 MiB. Byte length of a message's raw `event` value; over-limit messages are non-retryably dead-lettered with a bounded summary.                                                |
| `WARM_CONTAINER_IDLE_TIMEOUT` | `5m`               | positive Go duration (`90s`, `10m`, `1h30m`)                            | Fails startup.                                                                                                                                                                                                              |
| `METRICS_ADDR`                | unset              | listen address (`:9090`)                                                | Empty disables the Prometheus endpoint. A bind failure is fatal at startup.                                                                                                                                                 |
| `GIT_WEBHOOK_ADDR`            | unset              | listen address (`:8081`)                                                | Empty disables the GitHub webhook. A bind failure is fatal. Starts only when the git source also names a webhook secret.                                                                                                    |
| `NETWORKS`                    | unset              | comma-separated Docker network names                                    | Parsed at startup (trimmed, de-duplicated, declaration order kept). Every name is verified to exist; a missing network fails startup. Applied to execution containers and (as an order-independent set) service containers. |
| `REDIS_STREAM_RETENTION`      | `24h`              | Go duration (`6h`, `90m`)                                               | The optional value that **logs and disables** instead of failing. See below.                                                                                                                                                |
| `REDIS_DLQ_RETENTION`         | `7d` (`168h`)      | Go duration (`168h`, `90m`)                                             | The optional value that **logs and disables** instead of failing. Age-only DLQ trim. See below.                                                                                                                             |
| `REDIS_INVOCATION_RETENTION`  | `48h`              | Go duration (`12h`, `90m`)                                             | The optional value that **logs and disables** instead of failing. Terminal invocation-state TTL only. See below.                                                                                                            |
| `TRAEFIK_NETWORK`             | unset              | Docker network name                                                     | Required only when a service declares `host`; verified on every routed reconcile and joined in addition to `NETWORKS`.                                                                                                      |
| `TRAEFIK_ENTRYPOINTS`         | unset              | comma-separated Traefik entrypoint names (`websecure`, `web,websecure`) | Passed through verbatim into the router label; no default.                                                                                                                                                                  |
| `TRAEFIK_CERTRESOLVER`        | unset              | resolver name (`letsencrypt`)                                           | When set, adds both `tls=true` and `tls.certresolver`; unset adds neither.                                                                                                                                                  |
| `TRAEFIK_PRIORITY`            | unset              | positive integer                                                        | Any provided value must be positive (a fatal error otherwise); unset omits the label.                                                                                                                                       |
| `TRAEFIK_HOST_OVERRIDE`       | unset              | hostname suffix (`localhost`)                                           | Replaces the domain of each routed host, keeping its left-most label (`issuer.example.com` → `issuer.localhost`). Unset uses declared hosts verbatim.                                                                       |

`METRICS_ADDR`, `GIT_WEBHOOK_ADDR`, and the `TRAEFIK_*` values are read only by
the worker; the read-only admin commands that don't need Redis still call
`config.Load`, so an invalid `LOG_LEVEL` or tuning value fails those commands
too.

`NETWORKS` and `TRAEFIK_NETWORK` name Docker networks Relay attaches containers
to; Relay **never creates or removes** them (they are infrastructure owned
outside Relay). A name present at startup verification that is later deleted from
the daemon surfaces as a container-create failure on the next execution/service
create — Relay reports it and never re-creates the network. `NETWORKS` is startup
configuration: changing it requires a worker restart.

### Concurrency and backpressure

`MAX_CONCURRENT_INVOCATIONS` and `MAX_BUFFERED_EVENTS` are **per worker**. With
`N` replicas the effective global totals multiply.
`MAX_CONCURRENT_INVOCATIONS` is startup configuration (restart to change); a
template's per-app `concurrency` is clipped to it live and is documented in
[apps.md](apps.md).

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
worker concurrency → per-app concurrency → runner/container → ACK
```

When the local buffer is full the consumer stops reading, so the backlog stays
in Redis. A full concurrency slot leaves the message pending (no retry charged)
and a later reclaim replays it.

### Event size limit

`MAX_EVENT_BYTES` caps the byte length of a message's raw `event` **value**, not
the whole Redis entry: the entry and the go-redis response have already been
materialized before Relay can inspect the value, so this is a pre-decode
guard at the Relay boundary, not transport-level protection. The default is
256 KiB (`262144`); the hard ceiling is 1 MiB (`1048576`) and a larger value is a
fatal configuration error. Zero is **not** "unlimited": it is rejected, so the
cap is always a real bound. The default applies when the variable is unset or
empty.

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

### Stream and DLQ retention

Three independent retention windows trim or bound three different things. All
are **Go durations** parsed with `time.ParseDuration`; there is no day suffix
(`7d` is **not** valid), so a 7-day override is written `168h`. All are startup
configuration (restart to change) and none affects ACK, retry, DLQ routing, or
the scheduler/outbox.

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
obsolete-schedule, or DLQ path, or a cleared missing-payload PEL reference.
While the message is recoverable (still in the PEL and therefore redeliverable)
the hash is **persistent with no TTL**, regardless of this value; the window
starts only once the message is no longer recoverable.

- It bounds terminal bookkeeping (`ok` / `exhausted` / `exhausted:…:dlq`
  markers, plus the reserved classification/trace/schedule fields): how long a
  completed or exhausted invocation stays inspectable after its message left the
  PEL. It does **not** affect retries or ACK ordering.
- The TTL is applied atomically with the reserved terminal marker, and is
  monotonic: a repeated retention (e.g. a redelivery racing the ACK) never
  re-extends it, and a stale in-memory transition can neither mutate the
  terminal state nor remove the TTL.
- This window is **independent** of `REDIS_DLQ_RETENTION` (which trims DLQ stream
  entries) and of the scheduler's SQLite outbox, whose 7-day retention is a
  separate, local mechanism. A dead-lettered entry and the terminal hash for the
  same message are two different Redis/SQLite objects with their own windows.

**Unset vs. explicitly empty.** For all three windows, leaving the variable
**unset** applies the documented default (`24h` / `168h` / `48h`); setting it to
an **empty** (or whitespace-only) string (`REDIS_STREAM_RETENTION=`) disables
that retention (`0`). A malformed value and an explicit `0` or negative value log
a line naming the variable and disable that behavior; **retention values never
fail startup**.

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
REDIS_STREAM_RETENTION=          # disable source retention
REDIS_DLQ_RETENTION=             # disable DLQ retention
REDIS_INVOCATION_RETENTION=       # keep terminal invocation state persistent (no TTL)
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
