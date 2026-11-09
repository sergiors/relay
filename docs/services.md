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

`name` is the service's identity: it keys the container grouping, the Traefik
router/service id, and the persisted snapshot. It follows the same conservative
rule as an app name (`[a-z0-9][a-z0-9._-]*`, ≤ 63 chars, no trailing `.`) and
must be unique among the app's services. Two services **may** share the
same source — they are distinct services running the same implementation. A
changed source under the same name is the same logical service: it replaces the
same replica slots (start-before-stop), so it is **not** a removal plus an
addition. Renaming a service **is** a removal plus an addition (see
Convergence below).

- `entrypoint` is an application entrypoint **file** started as the long-lived
  process (e.g. `service.js`, `app/main.py`) — not the `module.function` event
  handler form. It must be a relative path inside the application directory, with
  no whitespace, no absolute paths, and no `..`. Node runs it directly
  (`node <entrypoint>`); Python runs it as a module (`python -m app.main`), so
  package-relative imports work. An `entrypoint` service needs a `runtime`.
- `image` is an external image reference (e.g.
  `nginxinc/nginx-unprivileged:1.27-alpine`, `ghcr.io/acme/api@sha256:…`). Relay
  inspects the local image and pulls from its registry when the image is missing
  locally or the hourly freshness window has elapsed; the image's own
  `ENTRYPOINT`/`CMD` and `USER` are preserved. An `image` service does not need a
  `runtime`. **Relay never removes external images** — cleanup only ever touches
  its own `relay-app-*` / `relay-dep-*` namespaces.

## Port, host, path, replicas

- `port` (optional) is the internal TCP port the application listens on.
  Default `80`, range `1`–`65535`. Relay injects it as the `PORT` environment
  variable (it cannot be overridden by template env or secrets) and exposes it
  as container metadata only — **no host port is published**. A routed service's
  Traefik rule targets this port.
- `host` (optional) is a hostname (e.g. `api.example.com`) exposing the service
  through Traefik. Empty/omitted means an internal unrouted service with no
  routing labels. Validated as a hostname at parse time.
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
environment at process **start**, so the effective environment is written to the
service container's Docker `Config.Env`. Anyone with Docker daemon access can read it via
`docker inspect`. Relay does not write raw environment values into labels, logs,
metrics, or spans; the `relay.environment` fingerprint is reconciliation
metadata, not a confidentiality boundary.

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

Service networking: every service container joins the worker-global `NETWORKS`
set (the same set execution containers join); a **routed** service additionally
joins `TRAEFIK_NETWORK`. An unrouted service with no global networks joins no
extra network. The set is de-duplicated and order-independent, so a service
listed on both `NETWORKS` and `TRAEFIK_NETWORK` joins that network exactly once.
Each service container carries a `relay.networks` label recording the canonical
(sorted) set so the reconciler can detect a network change and replace the
container. Relay never creates or removes any of these networks; the global set
is verified at startup and the routing network before each routed container
starts (see [configuration.md](configuration.md)). A network labelled
`relay.control=true` is Relay's control plane and is refused for workload
containers before create. The reserved `host` and `container:<id>`
pseudo-networks are also refused. A network deleted from the
daemon **after** that verification is not re-created: a container that needs
creating (a replacement or a new replica) fails, the failure is reported for that
service and retried on the next reconcile, and an already-converged running
container is left untouched.

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

Rules:

- Each entry is an absolute **container** path. The accepted roots are `/run`
  and `/var/cache` (the root itself or any descendant). Everything else,
  including `/`, `/tmp`, `/etc`, `/proc`, `/sys`, `/dev`, and `/var/run`, is
  rejected. Entries must be canonical (no trailing slash, empty segment, or
  `.`/`..` segment), and duplicate or overlapping destinations are rejected.
- `/tmp` always exists exactly as before and cannot be redeclared.
- Relay builds each additional mount with fixed options
  `rw,nosuid,nodev,noexec,size=16m,mode=1777`. The `1777` (sticky,
  world-writable) mode lets a non-root service process write. Callers cannot
  supply mount options.
- Before create, Relay inspects the exact immutable image content and rejects a
  target (or any existing parent component) that is a symlink or a non-directory
  in the image. Docker follows container-rootfs symlinks when applying
  `HostConfig.Tmpfs`, so without this check an image with
  `/var/cache -> /etc` could make a tmpfs at `/var/cache` writable over `/etc`
  despite the read-only rootfs. A missing target directory is allowed: the
  daemon creates it beneath the checked parents. An image whose filesystem
  cannot be inspected fails the create closed.
- Each mount is capped at 16 MiB, and the aggregate upper bound must not exceed
  one quarter of the app's effective per-container `resources.memory` (default
  `128MiB`, so two mounts by default). tmpfs pages count against the container
  memory limit; raise `resources.memory` to allow more mounts. Exceeding the
  budget is a template error.
- Additional tmpfs is a service-only capability: execution and warm containers
  keep their `/tmp`-only configuration.

The configured destinations are part of the service's container identity
(`relay.tmpfs`, the canonical sorted list). Changing the list replaces the
running replicas through the normal start-before-stop path.

### Image-declared VOLUME destinations

An image config that declares Docker `VOLUME` destinations is still refused by
default, because the daemon would materialize them as implicit anonymous
writable volumes even under a read-only rootfs. The exception is a declared
destination that is **exactly** covered by one of the service's configured
`tmpfs` entries: an image declaring `/var/cache/nginx` is accepted when the
service lists `/var/cache/nginx`. Relay never adds a tmpfs automatically for an
image volume, and any destination that is not explicitly configured remains an
error.

## Convergence and lifecycle

Both source kinds share **one cohesive reconciler and lifecycle**; the only
difference is how the desired image is resolved:

- an `entrypoint` service runs the app image prepared exactly as for
  invocations, with its entrypoint overridden per container;
- an `image` service runs the external reference (inspected locally, pulled when
  due).

At startup and on every reconcile of the owning app (including the periodic
pass, default every 30s), Relay lists its service containers and converges them
to the template.

- The desired image is resolved **before any container action**. If a source
  cannot be resolved (failed pull, missing local image, unlaunchable entrypoint,
  unresolved secret), the pass reports the failure and **preserves the existing
  healthy containers** rather than tearing them down. A transient registry
  outage therefore never degrades a working service.
- Containers whose source descriptor, image (or, for an external tag, image
  **content**), port, effective environment (`relay.environment`), per-container
  resources (`relay.resources`), additional tmpfs configuration
  (`relay.tmpfs`), or routing labels no longer match are **replaced**. Others
  are preserved — no unnecessary restarts. The joined
  network **set** (global `NETWORKS` + routing network for a routed service)
  participates in this comparison order-independently: reordering or repeating a
  network never replaces a container, but adding/removing/switching one does.
- A replacement is **start-before-stop**: for each replica slot Relay starts the
  new container and requires Docker to confirm it running before stopping the
  superseded one, so a failed replacement leaves the old generation serving and
  is retried on the next pass. A service is never taken to zero running replicas
  by a failed replacement.
- Changing only the **source** under the same `name` (a different entrypoint
  file, or a different image reference) is the same logical service: the new
  generation replaces the same replica slots via start-before-stop.
- **Renaming** a service (a new `name`) is a removal plus an addition: the old
  name's containers are treated as removed and are stopped **last**. If the new
  service does not converge — pull/resolution, routing or network, create/start,
  or a started-but-not-running container — the old name's containers are
  **preserved**, because they can be the only usable generation for the service
  the new name replaces. The removal is retried by a later reconcile once the
  desired set converges.
- Environment comparison is what makes a changed template `env` value or a
  **rotated secret value** replace a service's container: the image reference and
  fingerprint do not change for either, but a long-lived container would
  otherwise keep serving its old environment. Resource comparison makes a
  resource-only edit replace the container while the image is reused.
- Scaling up starts missing replica slots; scaling down stops exactly the excess
  containers (lowest-numbered replicas kept).
- A replica whose process exits (a crash) is recreated on the next reconcile, so
  a service self-heals within the periodic cadence — no event-style
  retry/DLQ semantics.
- Removing a service, or its whole app, stops and removes that worker's
  containers for it (scoped by `relay.node_id`).
  Relay then retires obsolete `relay-app-<name>` images, but only after no active
  container references them, and ordered after service convergence. Image
  removal is never forced.
- On graceful shutdown, Relay stops and removes the service containers owned by
  that node (scoped by `relay.node_id`); containers left by a crashed process
  of the **same** `NODE_ID` are swept at the next startup, while a different
  worker's service containers are never adopted, converged, or removed.

Changes to an app's services are serialized per app and only the latest
desired state is applied. A pass that a newer desired state superseded (a live
reload, or a periodic self-heal arriving mid-pass) does not stop the old
generation it was about to replace: it removes only the replacement containers it
started itself and leaves the old generation running, so the newer desired state
always finds a usable generation to replace. The coalesced newer request then
converges and commits the replacement.

**External image freshness:** for an `image` service Relay checks the registry
**at most once per hour per independent source** (per app + image
reference). A successful remote check is recorded in memory; the window is not
persisted, and changing the configured source reference is checked immediately.
A failed check does not advance the window, so it retries at the next reconcile.

Containers are identified by deterministic Relay-owned labels
(`relay.type=service`, `relay.app`, `relay.service`, `relay.identity`, plus
image content id, port, replica slot, `relay.environment`, `relay.resources`,
`relay.tmpfs`, `relay.networks`), never by name alone. `relay.service` is the
service's stable name (the grouping key); `relay.identity` is its configured
source descriptor, used only to resolve the image/entry command and to detect a
changed implementation. `relay.tmpfs` is the canonical sorted list of configured
additional tmpfs destinations (omitted when there are none) and is configuration
identity, not a security marker. Service containers carry no `relay.handler`
label. The Docker container name is greppable and derived from the
app/name/replica, but carries a per-start uniqueness token so a replacement can
be created while the container it replaces is still running; ownership,
grouping, and the replica slot always come from the labels.

## Traefik routing (optional)

A service that declares a `host` is routed through Traefik, which is
operator-provided infrastructure outside Relay. Relay attaches labels so
Traefik's Docker provider picks up the container:

```
traefik.enable                                      = true
traefik.docker.network                              = <TRAEFIK_NETWORK>
traefik.http.routers.<id>.rule                      = Host(`api.example.com`)
traefik.http.services.<id>.loadbalancer.server.port = <port>
```

When set, these optional labels are added (nothing is defaulted — no implicit
`websecure`, `letsencrypt`, or fallback priority):

```
traefik.http.routers.<id>.entrypoints          = <TRAEFIK_ENTRYPOINTS>
traefik.http.routers.<id>.tls                  = true                    (only when TRAEFIK_CERTRESOLVER set)
traefik.http.routers.<id>.tls.certresolver     = <TRAEFIK_CERTRESOLVER>
traefik.http.routers.<id>.priority             = <TRAEFIK_PRIORITY>
```

Requirements:

- `TRAEFIK_NETWORK` is **required** for a routed service. Relay never creates the
  network: it verifies it exists before starting routed containers. Unset or a
  missing network is reported per service and skipped (not half-reconciled).
  It is joined in addition to the global `NETWORKS` set; a network named in both
  is joined once. A network removed from the daemon after startup verification
  surfaces as a container-create failure on the next replacement — Relay never
  re-creates it.
- `TRAEFIK_HOST_OVERRIDE` changes only the effective host in the rule, replacing
  the domain while keeping the left-most label (`issuer.example.com` →
  `issuer.localhost`). The template host is not modified.
- When a service also declares a `path`, the rule is
  `Host(...) && PathPrefix(...)` with a StripPrefix middleware, so the upstream
  sees the path without the prefix. The middleware name is distinct and
  per-service.

`<id>` is a deterministic Traefik-safe router/service id derived from the
app name + **service name** (never the host, path, or source):
`relay-<app>-<name>-<hash>`, sanitized to `[a-z0-9-]`, capped at 100
characters, where `<hash>` is a 64-bit suffix hashed from the full untruncated
app and service name. Stable ids mean reconciliation produces stable labels
and a source change under the same name keeps the same route.

Changing `host`, `path`, `port`, or any routing value makes the running container
stale and it is replaced with updated labels. Removing `host` replaces the routed
container with an internal (unlabeled) one; clearing an optional value converges
its labels away the same way.

## Out of scope

Host port publishing, autoscaling, and request-level handler invocation are not
implemented. Routing is Traefik-only.
