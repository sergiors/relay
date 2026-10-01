# Apps

An app is one direct subdirectory of `/apps` containing a
`template.yaml`. The directory name is the app name.

```
/apps
  user-events-python/
    template.yaml
    events/created.py …
```

Names must match `[a-z0-9][a-z0-9._-]*`, be at most 63 characters, and not end
in a dot. Validation happens at load time — names are never sanitized — so valid
names are already safe as image tags. A directory without a `template.yaml` is
ignored; an invalid name or invalid template is logged and skipped without
stopping the worker.

## Template schema

```yaml
runtime: python3.14 # required for events/schedules and entrypoint services
concurrency: 2 # optional, default 2

resources: # optional, per-container
  memory: 256MiB # binary suffix required: KiB, MiB, or GiB
  cpus: 0.5 # finite number > 0; fractional CPUs allowed
  pids: 256 # positive integer

env: # optional, literal values
  API_URL: https://api.example.com
secrets: # optional, reference names only
  DATABASE_URL: database-url

events: # rules that consume stream events
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
    timeout: 6s # optional, default 6s, max 5m
    retries: 4 # optional, additional attempts, default 4

schedules: # cron-triggered handlers
  - name: nightly-cleanup # mandatory stable identity, unique per app
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    timezone: Europe/Rome # optional, default UTC
    timeout: 20s
    retries: 4

services: # persistent long-running containers
  - name: web # mandatory stable identity, unique per app
    entrypoint: service.js
    port: 3000
    replicas: 2
```

A template must declare **at least one event rule or one service**. `schedules`
do not satisfy this requirement, so a template whose only trigger is `schedules`
(or one with no `events` and no `services` at all) is rejected at parse time;
combine schedules with at least one event rule or service.

### Runtime

`runtime` is `python3.14` or `node24`; any other value fails validation. It is
required whenever Relay must launch work through a runtime:

- any `events` or `schedules` entry, or
- any service using an `entrypoint` source.

A services-only template whose services **all** use the external `image` source
needs no `runtime` at all — those images carry their own `ENTRYPOINT`/`CMD`. An
explicitly configured runtime is always validated, so a typo in an otherwise
image-only template is still a parse error.

The managed runtimes and dependency handling are:

| Runtime      | Base image         | Dependencies                                                                                                                               |
| ------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `python3.14` | `python:3.14-slim` | `uv.lock` + `pyproject.toml` (native uv project, installed locked); else `requirements.txt` via `uv pip install --system`.                 |
| `node24`     | `node:24-alpine`   | `package-lock.json` → `npm ci --omit=dev`; else `package.json` → `npm install --omit=dev`; else none. TypeScript is bundled at build time. |

Python dependencies are installed with uv, never pip. A `uv.lock` without
`pyproject.toml` (or vice versa) is an error rather than a guess. `node24`
accepts JavaScript **and** TypeScript handlers; TypeScript is transpiled and
bundled by Relay at build time (esbuild is never needed in your `package.json`).

### Handlers

`handler` has the form `module.function`, split at the **last** dot:
`events.created.handler` → module `events.created`, app `handler`.
Handlers may live in nested modules (for example the `events/` package), not
only top-level files. On `node24` a module may be `.js`/`.mjs` or
`.ts`/`.mts`; the handler string never carries an extension.

The handler contract is the same for events and schedules: the container gets
`RELAY_HANDLER` set to the rule's handler and the event JSON on stdin; the exit
code decides success (`0`) or failure (non-zero). See [events.md](events.md) and
[schedules.md](schedules.md).

Event handler names must be **unique across an app's event rules**,
regardless of pattern, timeout, or retries: the invocation identity is
`(app, handler)`, so a repeated handler is a template error (it would race
for the same per-invocation retry/DLQ state). Schedule handler names are not
constrained by this rule, but every schedule and every service must carry a
unique **name** (see [schedules.md](schedules.md) and [services.md](services.md)):
those names are their stable identities. A schedule or service name follows the
same conservative rule as an app name (`[a-z0-9][a-z0-9._-]*`, ≤ 63 chars,
no trailing `.`).

### Timeouts and retries

- `timeout` bounds one invocation of that rule (default `6s`). It must be
  positive and at most `5m`; a larger, zero, negative, or unparseable value
  fails template validation.
- `retries` is the number of **additional** executions after the initial one
  (default `4`, so `1 + retries = 5` total attempts). It must be a non-negative
  integer. The backoff schedule is fixed (1m/2m/5m/10m capped) and not
  template-configurable.

### Concurrency

`concurrency` bounds how many of this app's invocations run at once
**within a single worker** (per app, per worker). It is clipped to the
worker-global `MAX_CONCURRENCY`, so the effective limit is
`min(concurrency, MAX_CONCURRENCY)` (for example `15` with the default cap of
`8` is capped at `8`). It must be a positive integer; omitted templates default
to `2`. A hot-swapped change is applied live without a restart;
`MAX_CONCURRENCY` is startup configuration.

The same effective value sizes both the app's runner semaphore and its warm
container pool, so admission, the pool capacity gauge, and `app inspect`
agree.

### Environment and secrets

- `env` maps an env-var name to a **literal** value injected into every
  container. Values are never masked. Empty values are allowed.
- `secrets` maps an env-var name to a **secret reference** (`relay secret`
  store). The reference is resolved to a value immediately before each
  execution.
- Env-var names must match `[A-Za-z_][A-Za-z0-9_]*`; secret references must be
  lowercase letters/digits/`.`/`_`/`-`, ≤ 63 chars. A variable may not appear in
  both maps. `RELAY_HANDLER` is reserved and cannot be set.
- **Never put secret values in `template.yaml`** — the template participates in
  the fingerprint and is read at reconcile time. (The template is deliberately
  **never** copied into the runtime image build context, so env values and secret
  references cannot leak into an image layer.)

Injection and confidentiality — be precise about the boundaries:

- For **event/schedule invocations**, template env values and resolved secrets
  travel in the per-invocation request frame applied by the reused bootstrap
  process. They are never written to the execution container's Docker
  `Config.Env`, a Docker label, a metric, a log, a trace, or the state database.
  That is a Relay-surface guarantee, **not** confidentiality from your own
  handler: the handler runs in the container and can read and print the values
  it was given.
- For **persistent service containers**, a long-lived process needs its
  environment at start, so the effective environment (plan env + template env +
  resolved secrets + `PORT`) **is** written to the service container's Docker
  `Config.Env`. Anyone with Docker daemon access can read it via
  `docker inspect`. Relay only ever writes the one-way `relay.env_hash` digest
  into a label.

`relay app inspect` always shows env **names** with values redacted and
secret **reference** names only.

### Resource limits

`resources` declares per-container memory/CPU/PID limits for every container the
app runs (event, schedule, manual invocation, and both service source
kinds). All three keys are optional and resolved **independently**; omitted or
empty `resources` yields the defaults `128MiB`, `1` CPU, `128` PIDs.

- `memory` must be a binary size string with exactly one of `KiB`, `MiB`, or
  `GiB` (`256MiB`). Decimal suffixes (`MB`), bare numbers, zero, negatives, and
  fractions are rejected.
- `cpus` must be a finite number greater than zero; integers and fractions are
  accepted (`2`, `0.5`). It maps to Docker `NanoCPUs`.
- `pids` must be a positive integer, mapped to Docker's `PidsLimit`.

Limits are **per container, not per app**: there is no aggregate app
budget. An app with effective concurrency `N` may run `N` containers each
bounded by these values, so its aggregate ceiling is the limit multiplied by the
number of concurrently running containers.

### Builds and image reuse

Relay builds **one image per app**, versioned by source fingerprint:

- The fingerprint hashes the app's selected source (files after applying
  the app's `.gitignore` rules) plus `template.yaml` **verbatim**; applicable
  `.gitignore` files are hashed too, so a rule edit counts as a source change.
  The fingerprint and the image build context are derived from **one immutable
  snapshot** of the selected source, so the image tag can never describe bytes
  other than the ones baked into the image; `template.yaml` participates in the
  fingerprint but is never copied into the image.
- `resources` is deliberately **excluded** from the fingerprint. Editing only
  `resources` does **not** rebuild or retag the image; Relay rotates containers
  to the new limits instead (idle ones discarded immediately, busy ones drained
  after their current invocation). The same applies to a resource change on a
  persistent service: the container is replaced, the image is not.
- A rebuild produces a new immutable image; the old version keeps serving until
  the new one is prepared and swapped in.
- A template that needs no runtime (its only services use the `image` source)
  builds **no app image at all**; its fingerprint is computed over
  `template.yaml` alone, and Relay does not scan the source tree for it.

Relay manages only its own labeled images in the `relay-app-*` / `relay-dep-*`
namespaces (ownership is the strict `relay.type=app`/`dependency` label, not
the name) and never prunes other images or layers. An unlabeled image that merely
looks like a Relay image is left alone. Dependency images are content-addressed
and shared across apps; the fingerprint keys on the base image **tag**, not
its digest, so a newer pull of the same tag reuses the cached layer (operators
wanting a refresh must remove those images).

### Warm execution containers

Each app keeps a bounded warm pool of reused containers, up to its
effective concurrency, per image version. Containers are created lazily on
concurrent demand and reused for later invocations. Concurrent invocations of
the same app lease distinct containers; each container processes one
invocation at a time.

Because the interpreter process persists, module-level state may survive between
invocations. Per-invocation env values are applied exactly per request: a key a
new request no longer carries is restored to its pre-Relay value (or removed), so
a rotated or removed secret never leaks into a later invocation.

A container is pooled only while healthy; timeouts, process exits, protocol
errors, image changes, and shutdown invalidate it. A healthy idle container is
evicted after `WARM_CONTAINER_IDLE_TIMEOUT` (default `5m`). A container's
**version** is its resolved image content, so the same tag whose content changed
is a new generation that drains the old one.

Every execution container is hardened: non-root (uid 10001), all Linux
capabilities dropped, read-only root filesystem with a bounded `/tmp` tmpfs,
per-container memory/CPU/PID limits, outbound networking enabled.

### Docker networks

`NETWORKS` attaches every **execution** container this worker creates to
operator-provided Docker networks (comma-separated, verified at startup; Relay
never creates them). The same set is applied to every **persistent service**
container too; a routed service additionally joins `TRAEFIK_NETWORK`, and an
unrouted service with no global networks joins no extra network. A network
removed from the daemon after startup verification surfaces as a create failure
(execution or service) — Relay never re-creates it. Changing `NETWORKS` requires
a worker restart.

### Hot reload

Relay watches `/apps` and reconciles changes live: a new directory is
built and starts matching; edits to template/source/dependencies rebuild only
that app; a failed rebuild keeps the previous working version and retries on
the next change or the 30s periodic pass. An app whose image cannot be built
is marked unavailable but is still **matched**: an event matching only an
unavailable app counts as matched and stays pending (never DLQ'd for
unavailability alone) until the app is rebuilt. `/apps` is read-only
to Relay.
