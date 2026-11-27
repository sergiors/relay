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
fallback. See [Operations](operations.md#node-identity-loss).

## Optional

| Variable                      | Default                                                   | Purpose and accepted values                                                                  |
| ----------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `LOG_LEVEL`                   | `INFO`                                                    | `DEBUG`, `INFO`, `WARN`, or `ERROR`, case-insensitive; `WARNING` is not accepted.            |
| `MAX_CONCURRENT_INVOCATIONS`  | `8`                                                       | Positive integer; maximum concurrently executing invocations per worker.                     |
| `MAX_CONCURRENT_BUILDS`       | `2`                                                       | Positive integer; concurrent image builds per worker.                                        |
| `MAX_WARM_CONTAINERS`         | `8`                                                       | Positive integer; worker-wide cap on pooled execution containers.                            |
| `MAX_BUFFERED_EVENTS`         | `16`                                                      | Positive integer; maximum messages held locally by one worker.                               |
| `MAX_EVENT_BYTES`             | `256KiB`                                                  | Raw Redis `value` size limit; accepts binary sizes up to `1MiB`.                             |
| `WARM_CONTAINER_IDLE_TIMEOUT` | `5m`                                                      | Positive Go duration.                                                                        |
| `HOT_RELOAD`                  | `false`                                                   | Boolean; mount live source for supported runtimes instead of baking it into the image.       |
| `PYTHON_ZUVLOOP`              | `false`                                                   | Boolean; use `zuvloop` for Python event and schedule handlers.                               |
| `METRICS_ADDR`                | unset                                                     | Listen address for Prometheus metrics; empty disables metrics.                               |
| `GIT_WEBHOOK_ADDR`            | unset                                                     | Listen address for the optional GitHub webhook. Requires a configured webhook secret.        |
| `DISCOVERY_ADDR`              | unset                                                     | Listen address for the read-only discovery API (Traefik HTTP provider). Empty disables it.   |
| `NETWORKS`                    | unset                                                     | Optional comma-separated existing, operator-managed Docker networks for app connectivity.    |
| `REDIS_STREAM_RETENTION`      | `24h`                                                     | Age window for trimming acknowledged stream entries; see below.                              |
| `DLQ_RETENTION`               | `168h`                                                    | Age window for settled PostgreSQL DLQ rows; see below.                                       |
| `REDIS_INVOCATION_RETENTION`  | `48h`                                                     | Retention for terminal invocation state after ACK.                                           |
| `STATS_RETENTION`             | `168h`                                                    | Retention for stale node statistics.                                                         |
| `ROUTING_HOST_OVERRIDE`       | unset                                                     | Sets one exact routable hostname for every routed service; unset/blank keeps declared hosts. |
| `SERVICE_BIND_ADDR`           | unset                                                     | Static host IP routed services publish their port on; unset keeps them internal-only.        |
| `SERVICE_ADVERTISE_ADDR`      | `SERVICE_BIND_ADDR` when unset, empty, or whitespace-only | Static IP advertised as the backend address; defaults to bind.                               |
| `SECRETS_PROVIDER`            | `local`                                                   | `local` or `infisical`; selects the worker-wide secret provider.                             |
| `INFISICAL_SITE_URL`          | `https://app.infisical.com`                               | Infisical HTTPS origin; used only with `SECRETS_PROVIDER=infisical`.                         |
| `INFISICAL_PROJECT_ID`        | Required with Infisical                                   | Infisical project; required only with `SECRETS_PROVIDER=infisical`.                          |
| `INFISICAL_ENVIRONMENT`       | Required with Infisical                                   | Infisical environment; required only with `SECRETS_PROVIDER=infisical`.                      |
| `INFISICAL_SECRET_PATH`       | `/`                                                       | Secret path; used only with Infisical and must begin with `/`.                               |
| `INFISICAL_CLIENT_ID`         | Required with Infisical                                   | Universal Auth Machine Identity client ID.                                                   |
| `INFISICAL_CLIENT_SECRET`     | Required with Infisical                                   | Universal Auth client secret.                                                                |

Set `INFISICAL_SITE_URL` to the self-hosted HTTPS origin when applicable. Other
Infisical settings are ignored with the local provider. When Infisical is
selected, missing required values or an invalid HTTPS URL fail startup; Relay
never falls back to local files.

With a configured Git source, Relay deploys apps from Git; otherwise, it uses
the local `/apps` directory. See [Git-managed apps](operations.md#git-managed-apps).

Invalid values for required and tuning settings fail startup. Invalid retention
values are logged and disable the corresponding retention behavior. Networks
listed in `NETWORKS` must already exist; Relay does not create or remove these
workload networks. `NETWORKS` is optional, and an empty value is valid.

Relay automatically manages the connectivity used to execute handlers; users
do not configure an internal execution network. `NETWORKS` is optional
additional connectivity for app containers, and every listed network must
already exist. Networks marked `relay.network.role=execution-control` are
reserved and rejected for `NETWORKS`. This marker does not prove ownership or
isolate containers. Most worker settings require a restart to change.
App-level concurrency and resources are documented in [Apps](apps.md).

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

`LOG_LEVEL` filters Relay's own logs. Handler stdout and stderr use native
Docker container logs, not Relay forwarding, and are unaffected by this
setting. Warm containers can run different handlers over time; Relay does not
enrich their output with per-invocation context.

### Retention windows

Retention settings are independent and do not change retry or ACK behavior:

| Setting                      | Default         | What expires                                                                                                                                                         |
| ---------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REDIS_STREAM_RETENTION`     | `24h`           | Acknowledged entries in the source stream. Requires Redis 8.2 or newer; pending entries are protected, and trims are skipped while Relay's consumer group is absent. |
| `DLQ_RETENTION`              | `168h` (7 days) | Settled PostgreSQL DLQ entries. Unsettled or actively replayed entries are retained.                                                                                 |
| `REDIS_INVOCATION_RETENTION` | `48h`           | Terminal invocation state and DLQ settlement receipts after ACK.                                                                                                     |
| `STATS_RETENTION`            | `168h` (7 days) | Statistics for retired nodes; a worker never deletes its own row.                                                                                                    |

Retention values use Go duration syntax (`168h`, not `7d`). Unset values use the
defaults. An explicitly empty value disables retention; zero, negative, or
malformed values also disable it and log a warning. Retention configuration
does not fail startup. An empty `REDIS_INVOCATION_RETENTION` leaves terminal
state persistent. See
[Operations](operations.md#state-paths-and-persistence) for stored data and
[Events](events.md#dead-letter-queue) for DLQ behavior.

### Service routing

Routing is handled by an external proxy. Relay provides service discovery
through its HTTP API, allowing the proxy to discover available services and
route traffic to their published host ports.

The proxy independently manages its entrypoints, TLS, certificates, and
routing configuration.

A service that declares a `host` is published in Relay's service registry and
in the optional discovery API. The discovery API is enabled only when
`DISCOVERY_ADDR` is set (for example `:8082`) and exposes:

- `GET /discovery/traefik` — a Traefik HTTP-provider dynamic configuration.

Other paths return `404`; other methods, including `HEAD`, return `405` with
`Allow: GET`. The endpoint has no authentication and exposes internal host and
replica topology, so bind it to a private address or restrict access at the
network boundary. A registry read failure returns `503` (rather than an empty
configuration), which lets Traefik
keep its last successful routes. Configure Traefik to poll the endpoint, for
example:

```yaml
providers:
  http:
    endpoint: "http://relay:8082/discovery/traefik"
    pollInterval: "5s"
    pollTimeout: "5s"
```

A provider reaches each service replica through its published host port, so
`SERVICE_BIND_ADDR` must be set and the advertised address (which defaults to
the bind address) must be reachable from the proxy. See
[Service publication](#service-publication).

`ROUTING_HOST_OVERRIDE` sets one exact hostname for every routed service
instead of its declared `host`. Unset, empty, or whitespace-only keeps each
service's declared host. With several routed services, setting one override
makes their routes share the same hostname, so use distinct `path` values or
separate deployments. A duplicate `host`/`path` within one template is
rejected at parse time; across apps, discovery omits ambiguous routes.

Distinct paths on the same host are supported, and a path route strips its
prefix before forwarding. Overlapping prefixes are ordered with explicit
priorities, so the longest matching prefix wins.

### Service publication

`SERVICE_BIND_ADDR` optionally publishes the TCP port of every routed service
(one declaring a `host`) on a single static host IP. Each replica gets a
Docker-allocated host port, so several replicas can publish at once. Unrouted
services remain internal-only, and leaving the setting unset preserves
internal-only behavior for every service.

The value must be one static IPv4 or IPv6 address literal. Relay does not
resolve hostnames, select an interface, or guess a family, and it binds exactly
the configured address rather than widening publication to every interface.
Wildcard (`0.0.0.0`, `::`), multicast, IPv4 broadcast
(`255.255.255.255`), IPv4-mapped IPv6 (`::ffff:192.0.2.1`), and zone-scoped
IPv6 forms are rejected. Relay cannot detect a subnet broadcast address without
a prefix, so always supply a unicast host address. Loopback and link-local
unicast addresses are accepted, but they limit reachability to the local host
or link; Relay does not verify that the address exists on the host.

`SERVICE_ADVERTISE_ADDR` is the address shown to operators and clients; it
defaults to `SERVICE_BIND_ADDR` when unset, empty, or whitespace-only, and may
differ, for example when the host is reached through another address. Setting a
non-empty value without `SERVICE_BIND_ADDR` is a configuration error because
nothing is published. It is discovery/presentation metadata: changing it never
replaces a container, but it does change the backend addresses in the discovery
response.

Docker still listens only on `SERVICE_BIND_ADDR`. If `SERVICE_ADVERTISE_ADDR`
differs, the operator must ensure traffic to `advertisedIP:hostPort` reaches the
bound interface and port through forwarding or NAT; advertising a different
mesh IP alone does not make the port listen there. In a multi-node deployment,
set each node's bind address to a host IP that exists on that node and is
usable/reachable from the routing provider. The advertise address must identify
that node. Both settings are read at worker startup, so restart the worker after
changing either one.

Publishing a host port does not by itself make a service reachable outside the
host. Open the chosen ports in the host firewall, and configure any routing,
VPN, or NAT between clients and the host yourself; Relay does not manage those.
Binding to a specific IP selects the interface Docker listens on but is not a
complete network isolation boundary, because anything that can reach the host
address can reach the published port. The discovery API advertises the
`SERVICE_ADVERTISE_ADDR` with each replica's Docker-allocated host port, so the
proxy must be able to reach that address and port range.

## Environment owned by other libraries

Relay passes these settings to its clients; it does not parse them as Relay
configuration:

- Docker: `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`.
- OpenTelemetry: standard `OTEL_EXPORTER_OTLP_*`, `OTEL_SDK_DISABLED`,
  `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, sampler, and batch-processor
  variables.

Tracing is disabled by default. Configure an OTLP endpoint to enable it; setup
errors are non-fatal and Relay continues untraced. Relay uses W3C TraceContext
and Baggage propagation. An allowlisted, non-secret subset of these variables is
also inherited by managed Python and Node.js invocation containers. Relay's own
service name and resource attributes, and exporter authentication values, are
not inherited from the worker; app-supplied `OTEL_*` values remain on the normal
per-invocation environment frame. `RELAY_OTEL_PROVIDER` is an app-only
provider-ownership setting, not a worker setting. See
[Operations](operations.md#tracing).

Relay's managed provider is enabled by an effective OTLP endpoint and does not
use SDK auto-configuration for exporter selection. As a result,
`OTEL_TRACES_EXPORTER=none` alone does not disable Relay-managed export when an
endpoint is configured. To disable the managed provider, remove both
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_ENDPOINT` from the
effective worker/app configuration, or set `OTEL_SDK_DISABLED=true`. Disabling
the SDK does not disable W3C context propagation.
