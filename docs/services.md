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
  as container metadata only — **no host port is published**. A routed service's
  Traefik rule targets this port.
- `host` (optional) is a hostname (e.g. `api.example.com`) exposing the service
  through Traefik. Empty/omitted means an unrouted service. Validated as a
  hostname at parse time.
- `path` (optional) is a URL path prefix (e.g. `/v2`) under which the service is
  exposed on its host. It requires a `host` (`path` alone is rejected). A
  configured path must start with `/`; whitespace, query, fragment, backslash,
  and empty segments are rejected. It is canonicalized (trailing slashes
  removed, except `/`), so `/v2` and `/v2/` are the same configured path.
- `replicas` (optional) is the desired replica count Relay maintains. Default
  `1`; must be a positive integer. There is **no autoscaling** — the count is
  exactly what the template declares.

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

Healthcheck-enabled services also use Relay-managed Docker volumes to retain
readiness and retry state across worker restarts. The Docker Engine access Relay
uses must therefore permit volume create, list, and remove. If that metadata
cannot be read or written, Relay fails closed: it keeps the previous generation
serving and does not treat the service as converged.

### Rollout behavior

A replacement replica is started, then observed until Docker reports it healthy,
before the replica it supersedes is stopped. The previous generation keeps
serving throughout. While a candidate is still starting, Relay observes it at a
bounded one-second cadence (plus an exact scheduled wake at its startup
deadline), so a readiness transition prompts convergence promptly without
starting a duplicate candidate.

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
service that fails before a candidate exists: an unavailable routing network, a
source or image that cannot be resolved, an environment value or secret that
cannot be resolved, or a create/start error. Relay records the admitted attempt in
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

Deterministic routing-configuration problems — missing or invalid worker
`TRAEFIK_*` configuration, or an effective host that cannot be a valid hostname —
are reported as failures but are not retried on a backoff: the same configuration
can never succeed until the template or worker configuration changes.

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
connectivity; a routed service also requires `TRAEFIK_NETWORK`. These
operator-managed networks must already exist. See
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
networks, port, routing, tmpfs settings, or healthcheck updates its containers.
Scaling starts
or stops only the replicas needed to reach the declared count; there is no
autoscaling. A crashed replica is recreated during reconciliation. Removing a
service stops its containers; renaming it is treated as removal plus addition,
with the old healthy service retained until the replacement is running.

External image services use the image as supplied and are checked for updates at
most once per hour per app and image reference. A changed image is adopted
during reconciliation. Relay never removes external images.


## Traefik routing (optional)

A service with a `host` is routed through operator-provided Traefik. Set
`TRAEFIK_NETWORK` to an existing Docker network used by Traefik; Relay does not
create it. `TRAEFIK_ENTRYPOINTS`, `TRAEFIK_CERTRESOLVER`, and
`TRAEFIK_PRIORITY` add the corresponding router settings when configured.
`TRAEFIK_HOST_OVERRIDE` changes the domain used for a route while preserving the
left-most hostname label. Without these settings, Relay does not assume a
secure entrypoint, certificate resolver, or fallback priority.

A `path` requires a `host`; Traefik routes requests under that prefix and removes
the prefix before forwarding them to the service. Changing routing settings
updates the service container configuration.


## Out of scope

Host port publishing, autoscaling, and request-level handler invocation are not
implemented. Routing is Traefik-only.
