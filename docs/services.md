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
networks, port, routing, or tmpfs settings updates its containers. Scaling starts
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
