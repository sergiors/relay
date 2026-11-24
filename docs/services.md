# Services

A `services` list declares **persistent long-running containers** kept running
and reconciled continuously — an HTTP server, for example. A service is distinct
from an ephemeral **function**: event functions are selected by classification
and pattern matching, while a schedule-triggered function invocation is selected
by schedule-specific dispatch for a particular occurrence. Each function
attempt executes its handler and then finishes; retry/recovery may launch another
attempt. Services remain running independently of individual events or
occurrences.

## Source model

Every service declares a **mandatory `name`** — its stable identity within the
app — and **exactly one source**:

```yaml
runtime: nodejs24.x

services:
  - name: web # stable identity: keys containers, routing, state
    entrypoint: service.js # a runtime-managed application entrypoint file
    port: 3000
    replicas: 2

  - name: gateway
    image: nginxinc/nginx-unprivileged:1.27-alpine # an external image reference
    port: 8080
    replicas: 1
```

`name` is the service's stable, unique identity within the app. Changing its
source under the same name updates that service; renaming it removes the old
service and adds a new one.

- `entrypoint` is an application entrypoint **file** started as the long-lived
  process (e.g. `service.js`, `app/main.py`) — not the `module.function` event
  handler form. It must be a relative path inside the application directory, with
  no whitespace, no absolute paths, and no `..`. Node runs it directly
  (`node <entrypoint>`); Python runs it as a module (`python -m app.main`), so
  package-relative imports work. An `entrypoint` service needs a `runtime`.
- `image` is an external image reference (e.g.
  `nginxinc/nginx-unprivileged:1.27-alpine`, `ghcr.io/acme/api@sha256:…`). Relay
  inspects the local image and pulls from its registry when the image is missing
  locally or its hourly freshness window has elapsed; the image's own
  `ENTRYPOINT`/`CMD` and `USER` are preserved. An `image` service does not need a
  `runtime`. Relay does not remove external images.

## Port, host, path, replicas

- `port` (optional) is the internal TCP port the application listens on.
  Default `80`, range `1`–`65535`. Relay injects it as the `PORT` environment
  variable (it cannot be overridden by template env or secrets) and exposes it
  as container metadata. When host publication is configured, a routed service
  publishes this port on the host for the routing provider to reach; see
  [Host publication](#host-publication-optional).
- `host` (optional) is a hostname (e.g. `api.example.com`) exposing the service
  through the routing provider. Empty/omitted means an unrouted service.
  Validated as a hostname at parse time.
- `path` (optional) is a URL path prefix (e.g. `/v2`) under which the service is
  exposed on its host. It requires a `host` (`path` alone is rejected). A
  configured path must start with `/`; whitespace, query, fragment, backslash,
  and empty segments are rejected, as is a backtick (it would break a proxy
  rule string). It is canonicalized (trailing slashes
  removed, except `/`), so `/v2` and `/v2/` are the same configured path. An
  omitted/empty path is host-only; an explicit `/` is a root prefix and keeps
  its PathPrefix + StripPrefix behavior. For duplicate-route detection the two
  are equivalent. Two services in one app may not claim the same effective
  host and path; that is rejected at parse time.
- `replicas` (optional) is the desired replica count Relay maintains. Default
  `1`; must be a positive integer. There is **no autoscaling** — the count is
  exactly what the template declares.

## Host publication (optional)

Set `SERVICE_BIND_ADDR` to a static host IP to publish the `port` of every
routed service (one with a `host`) on the host. Each replica receives a
Docker-allocated host port, so all replicas of a service can be reached.
Unrouted services remain internal-only, and with the setting unset every service
stays internal-only. The address must be a static IPv4 or IPv6 literal; Relay
does not resolve names, and wildcard, multicast, broadcast, mapped, and
zone-scoped forms are refused. `SERVICE_ADVERTISE_ADDR` optionally names the
address operators and clients should use; it defaults to `SERVICE_BIND_ADDR`
when unset, empty, or whitespace-only. It changes the discovery response's
backend addresses but never replaces a container. See
[Configuration](configuration.md#service-publication).

Binding to a specific IP selects the interface Docker listens on, but it is not
complete network isolation: anything able to reach that host address can reach
the published port. Open the ports in the host firewall and arrange any VPN or
NAT yourself; Relay does not manage them. The discovery API advertises the
advertise address and each replica's Docker-allocated host port, so the routing
provider must be able to reach them. If the advertise address differs from the
bind address, traffic to `advertisedIP:hostPort` must be forwarded or NATed to
the bound interface and port; advertising a different address does not move the
Docker-published port. In a multi-node deployment, set each node's bind address
to that node's host IP and leave the advertise address matching it unless you
have such routing.

Changing `SERVICE_BIND_ADDR` updates the affected containers through the normal
start-before-stop replacement. Changing only `SERVICE_ADVERTISE_ADDR` does not
replace any container. Use `relay app inspect` to see the bind and advertise
addresses and each replica's current local Docker endpoint. See
[CLI](cli.md#relay-app).

## Health checks

An `entrypoint` service may declare an HTTP health check. Relay configures it
as Docker's native `HEALTHCHECK`, so the result is visible with
`docker inspect` and Relay only treats a replica as ready once Docker reports it
healthy:

```yaml
services:
  - name: web
    entrypoint: service.js
    port: 3000
    healthcheck:
      path: /health
      interval: 10s
      timeout: 3s
      retries: 3
      start_period: 10s
```

All fields are optional:

- `path` defaults to `/`. It is an HTTP path on `127.0.0.1:<port>`; it must start
  with `/`, and may not contain whitespace, a query, a fragment, or a backslash.
  The path is used exactly as configured: a trailing slash is significant and is
  not stripped (`/health` and `/health/` are distinct probe endpoints).
- `interval` defaults to `10s`. Accepted range `1s`–`1h`.
- `timeout` defaults to `3s`. Accepted range `1s`–`1h`, and it must not exceed
  `interval`; an omitted timeout is capped at a shorter configured interval.
- `retries` defaults to `3`. Accepted range `1`–`10`.
- `start_period` defaults to `10s`. Accepted range `0s`–`10m`.

The mapping is strict: any other key (for example a misspelled `interval`) is a
template error that names the unknown key and the accepted ones, so a typo never
silently falls back to Relay's defaults.

The probe performs one HTTP GET and treats a **2xx or 3xx** response as healthy.
A connection error, timeout, `4xx`, or `5xx` response is unhealthy. Redirects are
not followed. The probe runs inside the container with the runtime's own
`node` or `python` executable — no shell, `curl`, or `wget` is assumed — using
only standard-library HTTP support.

Health checks are supported only for `entrypoint` services. An `image` service
that declares `healthcheck` is rejected at template parse time: Relay cannot
assume probe tooling inside an image it does not build, and it will not report a
readiness state it cannot prove.

All services use Relay-managed Docker volumes to record the committed replica
generation; services with a healthcheck additionally retain readiness and retry
state there across worker restarts. The Docker Engine access Relay uses must
therefore permit volume create, list, and remove. If that metadata cannot be read
or written, Relay fails closed: it keeps the previous generation serving and does
not treat the service as converged. Host-port publication requires the committed
marker, so a replica is not advertised until Relay can record it.

### Rollout behavior

A replacement replica is started, then observed until it is running and, when
Relay applies a healthcheck, healthy, before the replica it supersedes is
stopped. The previous generation keeps serving throughout. While a candidate is
still starting, Relay observes it at a bounded one-second cadence (plus an exact
scheduled wake at its startup deadline), so a readiness transition prompts
convergence promptly without starting a duplicate candidate.

A candidate that Docker reports unhealthy, that exits, or that is still starting
past `start_period + (retries + 1) × interval` **measured from the instant Docker
actually started it** is a failed attempt. Create time is not used: a container
can be created well before it starts, so measuring the grace from creation would
fail a freshly started replica early. If the start instant cannot be read, Relay
does not declare the window elapsed; it keeps observing and relies on Docker's own
unhealthy transition.

A failed attempt is not retried immediately: Relay records the failure instant in
Docker-owned state and waits for a bounded exponential retry window. The delay
starts at the configured `interval` and doubles with each failed attempt, capped
at one minute — or at the configured `interval` when that is longer, so a service
probed less often than once a minute is never retried faster than its own cadence.
There is no attempt cutoff. Both the attempt and the retry instant are durable
across Relay restarts. If Relay cannot persist the failure instant at all — the
write was unavailable, or the process stopped during the operation — it treats the
unfinished attempt as failed and waits a full bounded window measured from
recovery, keeping the same attempt number: it may wait longer, but it never retries
sooner or resets to the first attempt. If Relay cannot persist state at all it
reports the failure and keeps the previous generation serving rather than starting
a replacement on a retry window it could not record.

A committed, already-active replica is different from an uncommitted candidate.
Relay records the committed generation in Docker-owned metadata; an active
replica that later becomes unhealthy is **not** replaced for health alone. It is
replaced when it exits, when its desired configuration changes, or when Relay
otherwise needs a new generation. If the slot has no other usable generation,
the failed replica is kept until a new replacement is viable, so a service is
never taken to zero replicas.

A healthcheck configuration change replaces the service containers without
rebuilding the image. Under `HOT_RELOAD`, Relay healthchecks are bypassed
entirely: no Docker probe or health metadata is applied, a healthcheck-only
template edit does not replace the container, and a source-only edit still
restarts the same container in place. A service without `healthcheck` keeps
Relay's previous behavior: a running container is considered ready. An
externally supplied image that declares its own Docker `HEALTHCHECK` is left to
Docker and does not change Relay's convergence decisions.

### Preparation failures versus healthcheck retries

The same bounded exponential retry policy applies to a healthcheck-enabled
service that fails before a candidate exists: a source or image that cannot be
resolved, an environment value or secret that cannot be resolved, or a
create/start error. Relay records the admitted attempt in
Docker-owned state before preparing, and the next eligible instant when it fails;
it keeps the existing replicas serving and retries when the window elapses — not
only on the app's normal reconciliation cadence. The first retry is at most one
minute away (sooner when `interval` is shorter) and the delay doubles from there
toward the configured `interval`, capped at max(one minute, `interval`). There is
no attempt cutoff. If Relay restarts while an attempt is unaccounted for and no
matching durable completion record exists, it treats that attempt as failed and
waits out the same bounded window — measured from recovery, because the exact
failure instant was not persisted — before preparing again. A matching durable
completion record shows that preparation succeeded even if retry-state cleanup
was interrupted, so Relay can continue without imposing a recovery delay. In
either case, a restart cannot reset the attempt sequence or retry sooner. When
the state cannot be written at all, Relay does not begin preparing, reports the
failure, and keeps the previous generation serving.

A service that declares no `healthcheck` keeps the historical behavior: a
preparation failure is reported for that pass and retried on the app's normal
reconciliation cadence. The same holds for a service whose probe Relay does not
apply: under `HOT_RELOAD` the live source is mounted and Relay's healthcheck is
bypassed entirely, so the declared probe neither gates a container replacement nor
drives the preparation retry policy, and a healthcheck-only template edit does not
reset that retry window.

## Environment, secrets, resources, networks

The environment each replica gets, in order: the runtime's plan environment
(e.g. `PYTHONDONTWRITEBYTECODE=1` for Python; empty for image sources), the
template's `env` values, resolved `secrets` values, then `PORT`.

Unlike ephemeral event/schedule functions, a persistent service needs its
environment at process start. Its effective environment is visible through
`docker inspect` to anyone with Docker daemon access. Relay does not write raw
environment values to logs, metrics, or traces. Treat Docker access as access to
service secrets.

Per-container `resources` apply to service containers too. The hardening is the
same as invocation containers: non-root (from the managed runtime image or the
external image's own `USER`; Relay never forces a UID on an external image),
all capabilities dropped, `no-new-privileges`, Docker's default seccomp profile,
private PID/IPC/network namespaces, read-only rootfs, bounded `/tmp`, and
memory/CPU/pids limits. A service whose declared `port` is below 1024 gets only
`NET_BIND_SERVICE` added on top of the dropped set, so managed non-root services
can bind a privileged port even when the daemon sets a nonzero unprivileged-port
floor. Workload containers never receive device access, arbitrary host mounts,
writable app source, or a container-runtime socket.

External images must run under those constraints. Prefer images that already run
as a non-root user and listen on an unprivileged port (for example
`nginxinc/nginx-unprivileged`, not the official `nginx`, which expects to chown
cache directories and setuid its workers). An image whose config declares Docker
`VOLUME` destinations is refused before create by default: the daemon would
materialize those as implicit anonymous writable volumes even with a read-only
rootfs. A declared destination is accepted only when the service explicitly
covers it with an additional `tmpfs` at the exact path (see below); Relay never
adds one automatically.

Services use the optional worker-wide `NETWORKS` list for additional
connectivity. These operator-managed networks must already exist. See
[Configuration](configuration.md).

## Additional writable tmpfs

A read-only rootfs leaves `/tmp` as the only writable path by default. A service
that also needs an ephemeral cache or runtime directory can declare a `tmpfs`
list:

```yaml
services:
  - name: web
    image: nginxinc/nginx-unprivileged:1.27-alpine
    port: 8080
    tmpfs: [/var/cache/nginx, /run]
```

Each entry must be a canonical absolute container path under `/run` or
`/var/cache`; `/tmp` is already writable and cannot be redeclared. Duplicate or
overlapping paths are rejected. Relay rejects targets that are symlinks or
non-directories in the image. Additional tmpfs is available only to services;
execution containers retain their `/tmp`-only configuration.

Each additional mount is capped at 16 MiB, and the total must not exceed one
quarter of the service's memory limit (default `128MiB`). The memory limit
includes tmpfs usage. An image-declared `VOLUME` destination is accepted only
when the service explicitly configures tmpfs at that exact path; Relay does not
add one automatically.

## Convergence and lifecycle

Relay keeps each service near its declared image, configuration, and replica
count. A failed image resolution, secret lookup, or replacement start leaves
healthy existing containers running. Relay starts and checks a replacement
before stopping the old container, so a failed update does not take the service
to zero replicas.

Changing a service's image, environment (including a rotated secret), resources,
networks, port, host bind address, tmpfs settings, or healthcheck updates its
containers. A routing `host`/`path` change does not replace a container: the
route is read from the current template when discovery builds its response.
Scaling starts
or stops only the replicas needed to reach the declared count; there is no
autoscaling. A crashed replica is recreated during reconciliation. Removing a
service stops its containers; renaming it is treated as removal plus addition,
with the old healthy service retained until the replacement is running.

External image services use the image as supplied and are checked for updates at
most once per hour per app and image reference. A changed image is adopted
during reconciliation. Relay never removes external images.


## Routing provider

Routing is provided by an external proxy. Relay publishes the effective routes
through the optional discovery API; it does not generate proxy labels or manage
entrypoints, TLS, or certificate resolvers. Routed service containers no longer
need to join a proxy network solely for routing: the proxy reaches them through
their published host ports. The Relay worker container may still join an
operator-managed private network so the proxy can reach the discovery API (as
the Compose production example does), and operator `NETWORKS` still apply to
service containers.

A service with a `host` is published only while this node has a live registry
lease, the committed replica is running (and, when Relay applies a healthcheck,
healthy), and its host port is verified. Relay also verifies Docker's actual host
binding before committing a routed replica; a missing or misdirected binding
keeps the previous generation serving and leaves the replica pending. A
replacement is not advertised before Relay commits it, and a probe configuration
change waits for the new generation to be committed before it is advertised. A
service with no healthcheck is advertised as soon as its committed replica is
running. Under `HOT_RELOAD`, Relay's probe is bypassed, so a running committed
replica is published without a health result.

Set `DISCOVERY_ADDR` (for example `:8082`) to enable the read-only
endpoint `GET /discovery/traefik`. The response groups replicas of the same
app/service across nodes, strips a `path` prefix before forwarding, and gives
overlapping prefixes explicit priorities so the longest prefix wins. Distinct
paths on the same host are valid; if different services claim the same effective
host and path, every route at that key is omitted and Relay logs the conflict.

The endpoint has no authentication and exposes internal topology. Bind it to a
private address or restrict access at the network boundary, and configure the
proxy to poll it. `ROUTING_HOST_OVERRIDE` sets one exact hostname for every
routed service instead of each service's declared `host`; unset or
whitespace-only keeps the declared hosts. The override is not a domain suffix:
services on different declared hosts share the override hostname, so give them
distinct `path` values or run separate deployments.

See [Configuration](configuration.md#service-routing) for the provider setup and
[Host publication](#host-publication-optional) for backend reachability.

## Out of scope

Autoscaling and request-level handler invocation are not implemented. Relay does
not create or manage an ingress proxy of its own, and it does not manage host
firewalls, VPNs, or NAT for published ports.
