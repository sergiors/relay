# Configuration

Relay reads its settings from environment variables. Required variables and
invalid startup settings cause `relay start` to fail. See
[Installation](installation.md) for deployment setup.

## Required

| Variable       | Purpose                                                                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `REDIS_URI`    | Redis address (`host:port`, `redis://…`, or `rediss://…`).                                                                           |
| `REDIS_STREAM` | Stream Relay consumes.                                                                                                               |
| `REDIS_GROUP`  | Consumer group. A new group starts at the beginning of the stream, so it can read existing messages.                                 |
| `DATABASE_URL` | PostgreSQL state database. Required at worker startup.                                                                               |
| `NODE_ID`      | Stable node identity, unique among concurrent workers and reused across restarts. Up to 128 ASCII letters, digits, `.`, `_`, or `-`. |

The Redis consumer name uses `NODE_ID`; there is no generated or hostname
fallback. A PostgreSQL lock prevents concurrent workers from using the same
identity. See [Operations](operations.md#node-ownership-loss).

## Optional

| Variable                      | Default                     | Purpose and accepted values                                                            |
| ----------------------------- | --------------------------- | -------------------------------------------------------------------------------------- |
| `LOG_LEVEL`                   | `INFO`                      | `DEBUG`, `INFO`, `WARN`, or `ERROR`, case-insensitive; `WARNING` is not accepted.      |
| `MAX_CONCURRENT_INVOCATIONS`  | `8`                         | Positive integer; maximum concurrently executing invocations per worker.               |
| `MAX_CONCURRENT_BUILDS`       | `2`                         | Positive integer; concurrent image builds per worker.                                  |
| `MAX_WARM_CONTAINERS`         | `8`                         | Positive integer; worker-wide cap on pooled execution containers.                      |
| `MAX_BUFFERED_EVENTS`         | `16`                        | Positive integer; maximum messages held locally by one worker.                         |
| `MAX_EVENT_BYTES`             | `256KiB`                    | Raw Redis `value` size limit; accepts binary sizes up to `1MiB`.                       |
| `WARM_CONTAINER_IDLE_TIMEOUT` | `5m`                        | Positive Go duration.                                                                  |
| `HOT_RELOAD`                  | `false`                     | Boolean; mount live source for supported runtimes instead of baking it into the image. |
| `PYTHON_ZUVLOOP`              | `false`                     | Boolean; use `zuvloop` for Python event and schedule handlers.                         |
| `METRICS_ADDR`                | unset                       | Listen address for Prometheus metrics; empty disables metrics.                         |
| `GIT_WEBHOOK_ADDR`            | unset                       | Listen address for the optional GitHub webhook. Requires a configured webhook secret.  |
| `NETWORKS`                    | unset                       | Comma-separated, existing Docker networks for workload containers.                     |
| `REDIS_STREAM_RETENTION`      | `24h`                       | Age window for trimming acknowledged stream entries; see below.                        |
| `DLQ_RETENTION`               | `168h`                      | Age window for settled PostgreSQL DLQ rows; see below.                                 |
| `REDIS_INVOCATION_RETENTION`  | `48h`                       | Retention for terminal invocation state after ACK.                                     |
| `STATS_RETENTION`             | `168h`                      | Retention for stale node statistics.                                                   |
| `TRAEFIK_NETWORK`             | unset                       | Additional Docker network; required for services with routed hosts.                    |
| `TRAEFIK_ENTRYPOINTS`         | unset                       | Comma-separated Traefik entrypoint names.                                              |
| `TRAEFIK_CERTRESOLVER`        | unset                       | Traefik certificate resolver; enables TLS when set.                                    |
| `TRAEFIK_PRIORITY`            | unset                       | Positive router priority.                                                              |
| `TRAEFIK_HOST_OVERRIDE`       | unset                       | Replaces the domain of each routed host with this suffix.                              |
| `SECRETS_PROVIDER`            | `local`                     | `local` or `infisical`; selects the worker-wide secret provider.                       |
| `INFISICAL_SITE_URL`          | `https://app.infisical.com` | Infisical HTTPS origin; used only with `SECRETS_PROVIDER=infisical`.                   |
| `INFISICAL_PROJECT_ID`        | Required with Infisical     | Infisical project; required only with `SECRETS_PROVIDER=infisical`.                    |
| `INFISICAL_ENVIRONMENT`       | Required with Infisical     | Infisical environment; required only with `SECRETS_PROVIDER=infisical`.                |
| `INFISICAL_SECRET_PATH`       | `/`                         | Secret path; used only with Infisical and must begin with `/`.                         |
| `INFISICAL_CLIENT_ID`         | Required with Infisical     | Universal Auth Machine Identity client ID.                                             |
| `INFISICAL_CLIENT_SECRET`     | Required with Infisical     | Universal Auth client secret.                                                          |

Set `INFISICAL_SITE_URL` to the self-hosted HTTPS origin when applicable. Other
Infisical settings are ignored with the local provider. When Infisical is
selected, missing required values or an invalid HTTPS URL fail startup; Relay
never falls back to local files.

Invalid values for required and tuning settings fail startup. Invalid retention
values are logged and disable the corresponding retention behavior. Network
names must already exist; Relay does not create or remove networks. Most worker
settings require a restart to change. App-level concurrency and resources are
documented in [Apps](apps.md).

Boolean settings accept `1`, `t`, `true`, `0`, `f`, or `false`. Durations use Go
syntax such as `90s` or `1h30m`; retention settings do not accept a `d` suffix.

Read-only admin commands need only the settings required for that command. For
example, `relay dlq` needs `DATABASE_URL`, while `relay health` asks the running
worker and does not initialize Redis or Docker clients in the CLI process.

### Concurrency and backpressure

These limits have different scopes:

- `MAX_CONCURRENT_INVOCATIONS` limits executing work per worker. An app's
  `concurrency` is capped by this value.
- `MAX_BUFFERED_EVENTS` limits messages held locally after Redis delivers them;
  it does not limit the group-wide Redis pending list.
- `MAX_WARM_CONTAINERS` limits pooled execution containers across apps (details
  below).
- `MAX_CONCURRENT_BUILDS` limits image builds per worker and is independent of
  execution capacity.

When execution capacity is unavailable, messages remain pending and are retried
by recovery without spending a handler retry. Limits are per worker, so their
aggregate capacity increases with the number of workers. Service containers are
not included in the warm-container limit. See [Events](events.md#execution-retries-and-delivery)
for delivery behavior.

### Warm-container bound

`MAX_WARM_CONTAINERS` is a worker-wide cap on pooled execution containers, both
idle and busy. Relay evicts the oldest idle container when it needs capacity; it
does not stop a busy container. If all containers are busy at the limit, the
invocation stays pending until capacity is available. Persistent service
containers are outside this limit. Changing the setting requires a worker
restart.

### Hot reload

`HOT_RELOAD` controls how source reaches runtime-backed containers:

- `false` (default): source is baked into the app image. Source changes prepare
  a new image before replacing the active version.
- `true`: Python and Node mount the live app directory read-only. Source-only
  edits reuse existing images; managed services restart and pooled execution
  containers are replaced. Other runtimes continue using baked source.

The mount exposes the app directory to the handler container, so enable hot
reload only for source trees the app may read. Changing the setting requires a
worker restart. Recursive read-only binds require Docker API 1.44 or newer and
daemon/kernel/runtime support (Moby requires Linux kernel 5.12 or newer). Relay
asks Docker to fail container creation if it cannot enforce that mode.
HOT_RELOAD does not make the source tree private from the host or from the
privileged Relay control plane.

### Python event loop

`PYTHON_ZUVLOOP` applies to Python event and schedule handlers only. When enabled,
Relay installs the pinned `zuvloop` package in eligible app images and uses it
for handler invocations. It does not affect services or Node apps. Changing the
setting requires a worker restart.

### Event size limit

`MAX_EVENT_BYTES` limits the raw Redis `value`, not the full stream entry. It
accepts an integer byte count or a binary size such as `256KiB` or `1MiB`; the
default is `256KiB` and the maximum is `1MiB`. Zero, negative, decimal-unit, or
larger values are invalid.

Oversized messages are dead-lettered before decoding or execution. Relay stores a
bounded diagnostic summary rather than the oversized body. The limit is checked
after Redis has delivered the entry, so it does not cap Redis or network memory.
Changing it requires a worker restart.

### Log levels

`LOG_LEVEL` filters Relay's own logs. Handler stdout and stderr are forwarded
regardless of this setting.

### Retention windows

Retention settings are independent and do not change retry or ACK behavior:

| Setting                      | Default         | What expires                                                                                           |
| ---------------------------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| `REDIS_STREAM_RETENTION`     | `24h`           | Acknowledged entries in the source stream. Requires Redis 8.2 or newer; pending entries are protected. |
| `DLQ_RETENTION`              | `168h` (7 days) | Settled PostgreSQL DLQ entries. Unsettled or actively replayed entries are retained.                   |
| `REDIS_INVOCATION_RETENTION` | `48h`           | Terminal invocation state, only after the message leaves the pending list.                             |
| `STATS_RETENTION`            | `168h` (7 days) | Statistics for retired nodes; a worker never deletes its own row.                                      |

Retention values use Go duration syntax (`168h`, not `7d`). Unset values use the
defaults. An explicitly empty value disables retention; zero, negative, or
malformed values also disable it and log a warning. Retention configuration
does not fail startup. An empty `REDIS_INVOCATION_RETENTION` leaves terminal
state persistent. See
[Operations](operations.md#state-paths-and-persistence) for stored data and
[Events](events.md#dead-letter-queue) for DLQ behavior.

### Service routing

`TRAEFIK_*` settings apply to services that declare routed hosts. Relay verifies
the configured network exists but does not manage the network itself. See
[Services](services.md) for routing configuration.

Workload networks must not include Relay's control plane. Relay refuses a
selected network labelled `relay.control=true` (and the reserved `host` /
`container:<id>` pseudo-networks) before creating any workload container.

## Environment owned by other libraries

Relay passes these settings to its clients; it does not parse them as Relay
configuration:

- Docker: `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`.
- OpenTelemetry: standard `OTEL_EXPORTER_OTLP_*`, `OTEL_SDK_DISABLED`,
  `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, sampler, and batch-processor
  variables.

Tracing is disabled by default. Configure an OTLP endpoint to enable it; setup
errors are non-fatal and Relay continues untraced. Relay uses W3C TraceContext
and Baggage propagation. See [Operations](operations.md#observability).
