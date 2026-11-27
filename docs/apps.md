# Apps

An app is one direct subdirectory of `/apps` containing a
`template.yaml`. The directory name is the app name.

Relay distinguishes the trigger, execution target, and workload: an external
event is classified and pattern-matched; a **schedule** is a trigger whose each
firing creates a uniquely identified **occurrence**; a **handler** is the target
selected for execution; and a **function** is the ephemeral, one-shot workload
that runs that handler. A **service** is a separate persistent, long-lived
workload. Event and schedule functions share runtime and delivery/recovery
machinery, but schedule occurrences bypass ordinary event classification and
pattern matching. Services do not use that one-shot message lifecycle.

```
/apps
  user-events-python/
    template.yaml
    events/created.py …
```

Names must match `[a-z0-9][a-z0-9._-]*`, be at most 63 characters, and not end
in a dot. Validation happens at load time — names are never sanitized — so valid
names are already safe as image tags. A directory without a `template.yaml` is
not an app: discovery ignores it (it may hold unrelated files), and if a known
app loses its template it is removed like any other removed app. An invalid name
or invalid template is logged and skipped without stopping the worker.

An app's source is scoped to its own directory. Only the app's `.gitignore` and
`.gitignore` files in its non-ignored subdirectories decide which files are
copied, fingerprinted, and built. Ignore files above the app directory — at the
repository root or a monorepo parent — never affect the app, so they cannot hide
an app or exclude files from it. `template.yaml` is always part of the app's
source, even if a pattern matches it.

## Template schema

The event rule below shows MongoDB CDC's `collectionName`; for PostgreSQL CDC,
use `tableName` instead. The other normalized event fields are the same.

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
      recordType: [INSERT]
      collectionName: [users]
    timeout: 6s # optional, default 6s, max 5m
    retries: 4 # optional, additional attempts, default 4, max 100

schedules: # triggers that publish uniquely identified occurrences
  - name: nightly-cleanup # mandatory stable identity, unique per app
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    timezone: Europe/Rome # optional, default UTC
    timeout: 20s
    retries: 4 # optional, additional attempts, default 4, max 100

services: # persistent long-running containers
  - name: web # mandatory stable identity, unique per app
    entrypoint: service.js
    port: 3000
    replicas: 2
    healthcheck: # optional, entrypoint services only
      path: /health
      interval: 10s
  - name: cache
    image: nginxinc/nginx-unprivileged:1.27-alpine
    tmpfs: [/run, /var/cache/nginx] # optional additional writable tmpfs
```

A template must declare **at least one event rule or one service**. `schedules`
do not satisfy this requirement, so a template whose only trigger is `schedules`
(or one with no `events` and no `services` at all) is rejected at parse time;
combine schedules with at least one event rule or service.

### Runtime

Supported runtimes are `python3.14`, `python3.15`, `nodejs24.x`, and
`nodejs26.x`. A runtime is required for event and schedule handlers or services
that use an `entrypoint`. A template containing only external-image services
can omit it.

Apps use locked dependencies: Python uses `uv.lock` with `pyproject.toml`, or
`requirements.txt`; Node.js requires `package.json` and `pnpm-lock.yaml`. Node
supports JavaScript and TypeScript handlers; Relay bundles TypeScript, so apps
do not need to add a bundler. Mismatched or missing lock/manifests are rejected.

By default, app source is included in the runtime image. With `HOT_RELOAD`,
Python and Node use the live app source read-only; source edits take effect
without rebuilding the image. Other runtimes continue using baked source. See
[Configuration](configuration.md#hot-reload).

For Python event and schedule handlers, `PYTHON_ZUVLOOP` selects the event loop
used for invocations. It does not affect services. See
[Configuration](configuration.md#python-event-loop).

### Handlers

`handler` has the form `module.function`, split at the **last** dot:
`events.created.handler` → module `events.created`, app `handler`.
Handlers may live in nested modules (for example the `events/` package), not
only top-level files. On a Node runtime a module may be `.js`/`.mjs` or
`.ts`/`.mts`; the handler string never carries an extension.

Event and schedule handlers run as one-shot functions through the managed
runtime. Event handlers receive the matching event; scheduled handlers receive
the schedule payload and are selected by the schedule rather than event matching.
A zero exit status is success; a non-zero status is failure. Handler output is
available in Docker container logs. See [Events](events.md) and
[Schedules](schedules.md).

#### Invocation context

The invocation metadata has the same values for event and schedule functions,
with language-specific property names and types:

| TypeScript property    | Python property         | TypeScript type       | Python type               | Meaning                                                |
| ---------------------- | ----------------------- | --------------------- | ------------------------- | ------------------------------------------------------ |
| `requestId`            | `request_id`            | `string`              | `str`                     | Unique identifier for this handler invocation.         |
| `attempt`              | `attempt`               | `number`              | `int`                     | One-based attempt number.                              |
| `maxAttempts`          | `max_attempts`          | `number`              | `int`                     | Total allowed attempts, including the initial attempt. |
| `deadline`             | `deadline`              | ISO-8601 `string`     | timezone-aware `datetime` | Invocation deadline.                                   |
| `messageId` (optional) | `message_id` (optional) | `string \| undefined` | `str \| None`             | Stream message ID, when the invocation has one.        |
| `traceId` (optional)   | `trace_id` (optional)   | `string \| undefined` | `str \| None`             | Trace ID, when tracing context is available.           |
| `app`                  | `app`                   | `string`              | `str`                     | App name.                                              |
| `handler`              | `handler`               | `string`              | `str`                     | Configured handler name (`module.function`).           |

Node.js and TypeScript handlers receive the event first and a fresh context object
second: `handler(event, context)`. Its fields use the names shown above, and
`context.getRemainingTimeMs()` returns the non-negative number of milliseconds
until `deadline` (zero after the deadline). The official Node.js library
publishes the `HandlerContext` declaration. Install it as an application
dependency with `pnpm add @sergiors/relay-runtime` when handlers import it;
Relay's Node.js bootstrap constructs the actual context object. The library
supports Node.js 24 and newer.

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";

export async function handler(
  event: Record<string, unknown>,
  context: HandlerContext,
): Promise<void> {
  void event;
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
```

Python handlers also receive the event and context as their first and second
arguments: `handler(event, context)`. The `relay-runtime` package provides the
public `HandlerContext` interface; install it as an application dependency when
handlers import it. The library supports Python 3.14 and newer. Relay's Python
bootstrap constructs the actual context. Its
`get_remaining_time_ms()` method returns the non-negative number of milliseconds
until `deadline` (zero after the deadline):

```python
from relay_runtime.typing import HandlerContext


async def handler(event: dict, context: HandlerContext) -> None:
    print(
        f"request {context.request_id} "
        f"has {context.get_remaining_time_ms()}ms left"
    )
```

Python handlers may optionally use `Depends` and `@bind` from the same library
to inject sync or async providers, with per-invocation or per-container
lifetimes and generator cleanup. This is opt-in; handlers that do not import it
keep the plain function form. See the
[Python library documentation](../packages/python/README.md#dependency-injection)
for the rules, scopes, and limitations.

OpenTelemetry trace propagation remains available ambiently during the handler
call in both runtimes. Python and Node.js invocation images can also run a
Relay-managed trace provider configured from the app's standard `OTEL_*`
environment and the worker's allowlisted tracing configuration. Node.js
`nodejs24.x` and `nodejs26.x` invocation images supply OpenTelemetry API 1.10.0,
the SDK, and OTLP trace exporters. Node.js service-only images supply API
1.10.0 but not the managed SDK or provider; a service process does not initialize
Relay's managed provider. Standalone Node.js applications must install API
1.10.0 and configure their own SDK/provider. See
[Operations](operations.md#tracing).

#### Node.js spans

Use the public `Tracer` facade from the Node runtime package. Add the runtime
package to the app's dependencies and commit the updated manifest and lockfile:

```sh
pnpm add @sergiors/relay-runtime
```

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";
import { Logger } from "@sergiors/relay-runtime/logging";
import { Tracer } from "@sergiors/relay-runtime/tracing";

const log = new Logger({ service: "orders" });
const tracer = new Tracer("orders");

export async function handler(
  event: { orderId: string; type?: string },
  context: HandlerContext,
): Promise<void> {
  await tracer.span("orders.process", async (span) => {
    log.info("Processing order", { order_id: event.orderId });
    span.addEvent("order.processing.started", {
      event_type: event.type ?? "unknown",
    });
    await processOrder(event.orderId);
  });
}
```

The handler's active span context is propagated from the incoming event or
schedule when available. Span options default to `SpanKind.INTERNAL`; prefer
stable `<domain>.<action>` names and low-cardinality, non-sensitive attributes.
Avoid putting request IDs or other high-cardinality values on spans. The
`Tracer` facade owns span completion, and `Logger` automatically correlates
records with the active span without Grafana-specific behavior. See the
[Node SDK Tracing documentation](../packages/node/README.md#tracing) for API
ownership, lifecycle, and detached-work guidance.

Relay-managed Node images supply `@opentelemetry/api` 1.10.0, so managed apps
need not add the API package themselves. Standalone applications must also
install exactly `@opentelemetry/api@1.10.0` and configure their own SDK/provider;
provider and context-manager registrations must use that version because
OpenTelemetry's global registry requires an exact version match.

#### Python logging and tracing

Add `relay-runtime` to a Python app's dependencies to use its logger. Relay's
managed Python handler images supply the OpenTelemetry API, SDK, and OTLP
exporters; standalone applications can install the optional tracing extra:

```sh
uv add relay-runtime
# Standalone tracing:
uv add "relay-runtime[tracing]"
```

```python
from relay_runtime.logging import Logger
from relay_runtime.tracing import Tracer
from relay_runtime.typing import HandlerContext

log = Logger(service="orders")
tracer = Tracer("orders")


async def handler(
    event: dict[str, str],
    context: HandlerContext,
) -> None:
    with tracer.span("orders.process") as span:
        log.info(
            "Processing order",
            extra={
                "order_id": event["order_id"],
                "remaining_ms": context.get_remaining_time_ms(),
            },
        )
        span.set_attribute("order.kind", event["kind"])
        await process_order(event["order_id"])
```

See the Python package documentation for
[structured logging](../packages/python/README.md#structured-logging) and
[tracing](../packages/python/README.md#tracing).

Event handler names must be unique across an app's event rules. Every schedule
and service also needs a unique, stable name within its app; names follow the
same format as app names (`[a-z0-9][a-z0-9._-]*`, up to 63 characters, no
trailing dot). See [Schedules](schedules.md) and [Services](services.md).

### Timeouts and retries

- `timeout` bounds one function invocation (default `6s`). It must be
  positive and at most `5m`; a larger, zero, negative, or unparseable value
  fails template validation.
- `retries` is the number of **additional** executions after the initial one
  (default `4`, so `1 + retries = 5` total attempts; maximum `100`, so at most
  `101` total attempts). It must be an integer between `0` and `100`; a
  negative, non-integer, or greater-than-`100` value fails template
  validation. The backoff schedule is fixed (1m/2m/5m/10m capped) and not
  template-configurable.

### Concurrency

`concurrency` bounds how many of this app's invocations run at once
**within a single worker** (per app, per worker). It is clipped to the
worker-global `MAX_CONCURRENT_INVOCATIONS`, so the effective limit is
`min(concurrency, MAX_CONCURRENT_INVOCATIONS)` (for example `15` with the default
cap of `8` is capped at `8`). It must be a positive integer; omitted templates
default to `2`. A hot-swapped change is applied live without a restart;
`MAX_CONCURRENT_INVOCATIONS` is startup configuration.

### Environment and secrets

- `env` maps an env-var name to a **literal** value injected into every
  container. Values are never masked. Empty values are allowed.
- `secrets` maps an env-var name to a **secret reference**. The active secret
  provider resolves it when needed.
- Env-var names must match `[A-Za-z_][A-Za-z0-9_]*`; secret references must be
  lowercase letters/digits/`.`/`_`/`-`, ≤ 63 chars. A variable may not appear in
  both maps. `RELAY_HANDLER` is reserved and cannot be set.
- **Never put secret values in `template.yaml`.** The template is not copied
  into the runtime image.

Injection and confidentiality — be precise about the boundaries:

- For **event and schedule functions**, values are provided to the handler but
  are not written to Docker container configuration, labels, metrics, logs,
  traces, or Relay state. The handler can read and expose any value it receives.
- For **persistent services**, the effective environment (including resolved
  secrets and `PORT`) is set when the container starts. Anyone with Docker
  daemon access can read it with `docker inspect`; Docker access therefore
  includes access to service secrets.

`relay app inspect` always shows env **names** with values redacted and
secret **reference** names only.

### Resource limits

`resources` declares per-container memory/CPU/PID limits for every container the
app runs (event/schedule/manual function, and both persistent service source
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

Relay prepares managed runtime images from an app's source and dependencies.
Changes to source, dependencies, or the template may require a new image;
resource-only changes replace containers without rebuilding it. Relay keeps the
previous working version available until a replacement is ready. External
service images are run as supplied and are not removed by Relay. See
[Services](services.md) for external image requirements.

### Warm execution containers

Relay reuses a bounded pool of containers for one-shot functions. Each container
runs one invocation at a time; module-level state can persist between requests,
but container reuse is an optimization, not a durability guarantee. Relay
replaces unhealthy or outdated containers and evicts idle containers after
`WARM_CONTAINER_IDLE_TIMEOUT` (default `5m`). The worker-wide
`MAX_WARM_CONTAINERS` cap applies across apps; persistent services are not
included. When all pooled containers are busy at the cap, work remains pending
until capacity is available. See
[Configuration](configuration.md#warm-container-bound).

Python handlers in a reused process share its event loop. Async handlers are
supported; a synchronous handler must not try to drive a coroutine with
`asyncio.run()` while that loop is running. Use an async handler for asynchronous
work. Container replacement ends the process and any in-memory state.

Handlers share the execution container's trust boundary and are not isolated
from that container's runtime bootstrap. Treat handlers within an app as trusted
code.

Execution containers run as non-root with a read-only root filesystem, bounded
`/tmp`, and per-container memory, CPU, and PID limits. Images declaring Docker
`VOLUME` destinations are rejected because they would add writable storage
outside those limits.

### Docker networks

`NETWORKS` adds optional connectivity for execution and service containers;
listed networks must already exist. Changing `NETWORKS` requires a worker
restart. See [Configuration](configuration.md) for networking behavior.

### Hot reload

Relay watches `/apps` and applies valid app changes live. If a build or update
fails, the previous working version remains available. Matching events and
valid schedule occurrences remain pending until an unavailable app recovers;
unavailability alone does not acknowledge or dead-letter them. Invalid
templates are reported in app status. See
[Events](events.md#execution-retries-and-delivery) for delivery behavior.

With `HOT_RELOAD`, Python and Node read source from `/apps` instead of baking it
into the image. Source edits restart managed services and recycle execution
containers so new invocations use the updated code. The source directory is
read-only inside those containers. Relay does not modify `/apps`. See
[Configuration](configuration.md#hot-reload).
