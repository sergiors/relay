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

| Variable                      | Default | Format / accepted values                                                | Behavior when invalid                                                                                                                                 |
| ----------------------------- | ------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOG_LEVEL`                   | `INFO`  | `DEBUG`, `INFO`, `WARN`, `ERROR` (case-insensitive, trimmed)            | Fails startup. `WARNING` is **not** an alias.                                                                                                         |
| `MAX_CONCURRENCY`             | `8`     | positive integer                                                        | Fails startup. Bounds concurrent invocations **per worker**.                                                                                          |
| `MAX_BUFFERED_EVENTS`         | `16`    | positive integer                                                        | Fails startup. Bounds messages read from Redis and held locally per worker.                                                                           |
| `WARM_CONTAINER_IDLE_TIMEOUT` | `5m`    | positive Go duration (`90s`, `10m`, `1h30m`)                            | Fails startup.                                                                                                                                        |
| `METRICS_ADDR`                | unset   | listen address (`:9090`)                                                | Empty disables the Prometheus endpoint. A bind failure is fatal at startup.                                                                           |
| `GIT_WEBHOOK_ADDR`            | unset   | listen address (`:8081`)                                                | Empty disables the GitHub webhook. A bind failure is fatal. Starts only when the git source also names a webhook secret.                              |
| `NETWORKS`                    | unset   | comma-separated Docker network names                                    | Parsed at startup (trimmed, de-duplicated, declaration order kept). Every name is verified to exist; a missing network fails startup. Applied to execution containers and (as an order-independent set) service containers. |
| `REDIS_STREAM_RETENTION`      | unset   | Go duration (`6h`)                                                      | The one optional value that **logs and disables** instead of failing. See below.                                                                      |
| `TRAEFIK_NETWORK`             | unset   | Docker network name                                                     | Required only when a service declares `host`; verified on every routed reconcile and joined in addition to `NETWORKS`.                               |
| `TRAEFIK_ENTRYPOINTS`         | unset   | comma-separated Traefik entrypoint names (`websecure`, `web,websecure`) | Passed through verbatim into the router label; no default.                                                                                            |
| `TRAEFIK_CERTRESOLVER`        | unset   | resolver name (`letsencrypt`)                                           | When set, adds both `tls=true` and `tls.certresolver`; unset adds neither.                                                                            |
| `TRAEFIK_PRIORITY`            | unset   | positive integer                                                        | Any provided value must be positive (a fatal error otherwise); unset omits the label.                                                                 |
| `TRAEFIK_HOST_OVERRIDE`       | unset   | hostname suffix (`localhost`)                                           | Replaces the domain of each routed host, keeping its left-most label (`issuer.example.com` → `issuer.localhost`). Unset uses declared hosts verbatim. |

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

`MAX_CONCURRENCY` and `MAX_BUFFERED_EVENTS` are **per worker**. With `N`
replicas the effective global totals multiply. `MAX_CONCURRENCY` is startup
configuration (restart to change); a template's per-app `concurrency` is
clipped to it live and is documented in [apps.md](apps.md).

```
Redis stream → bounded local buffer → matcher/dispatcher →
worker concurrency → per-app concurrency → runner/container → ACK
```

When the local buffer is full the consumer stops reading, so the backlog stays
in Redis. A full concurrency slot leaves the message pending (no retry charged)
and a later reclaim replays it.

### Log levels

`LOG_LEVEL` filters Relay's own `slog` output only. Handler stdout/stderr is a
raw transport forwarded verbatim at every level, independent of `LOG_LEVEL`.
Fatal startup failures always log regardless of the configured level.

### Stream retention

`REDIS_STREAM_RETENTION` is opt-in. When set, a single goroutine inside
`relay start` periodically trims the stream with
`XTRIM <stream> MINID ~ <cutoff> ACKED`, where `<cutoff>` is
`<now - retention>` in Unix milliseconds.

- The tick interval is derived automatically from the window (`retention / 24`,
  clamped to `[1m, 1h]`); it is not another variable.
- The trim is approximate (`~`) and runs in **`ACKED`** mode, not the Redis
  default `KEEPREF`: an entry is removed only once **every** consumer group on
  the stream has read and acknowledged it. A group that never drains therefore
  blocks trimming of the range it covers (the safe direction).
- **Requires Redis 8.2+.** On an older server Relay refuses to trim, logs
  `Retention: disabled`, and never falls back to a mode that could evict pending
  entries.
- Unset disables retention entirely. A malformed or non-positive value logs and
  disables retention; it never fails startup.

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
