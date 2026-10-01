# Relay

Relay is an event-driven runtime for declarative apps — with serverless functions, schedules, and persistent services. Run it on infrastructure you control.

It executes short-lived functions in isolated runtimes, publishes scheduled work through the same event pipeline, and continuously reconciles long-running services from the same declarative model.

For event-driven workloads, Relay consumes events from a broker, matches them against declarative rules, executes matched functions, retries failures, and dead-letters exhausted invocations.

Persistent services use the same runtime, configuration, secrets, networking, resource controls, and container infrastructure without requiring them to participate in the event pipeline.

**An app is the unit of source, configuration, and deployment.** Within an app, event-driven **functions**, schedules, and optional persistent services share one declarative model. Each function targets a handler, while schedules publish work into the same event pipeline and services run as long-lived containers outside it.

Functions remain a first-class Relay workload. Start with a function. Add a service when you need one.

Relay does not care where events originate:

```text
Producer → Redis Stream → Relay → App → Function (handler)
```

Functions, schedules, and persistent services are declared within an app and share the same deployment model while keeping their execution semantics independent.

## What you get

- **Event-driven execution** — Redis Streams, declarative matching, retries, reclaim, dead-lettering, and at-least-once delivery.
- **Managed runtimes** — Python 3.14 and Node 24, including TypeScript support, with reusable warm containers and bounded concurrency.
- **Cron schedules** — minute-precision cron with IANA timezones, deterministic occurrence identity, cluster-wide publication deduplication, bounded retries with a durable local outbox, and startup catch-up.
- **Persistent services** — run APIs, workers, gateways, consumers, and other long-running processes from managed-runtime entrypoints or external container images, with replicas, Docker networks, resource limits, environment configuration, secrets, and optional Traefik routing.
- **Resource controls** — per-container memory, CPU, and PID limits.
- **Configuration and secrets** — environment values and secret references shared across apps, schedules, and services.
- **Observability** — persisted statistics, Prometheus metrics, structured logs, and OpenTelemetry tracing.
- **Operations** — health checks, manual invocation, DLQ inspection/replay, Git synchronization, and ordered teardown in which an aggregate deadline caps best-effort cleanup while dependency barriers have their own per-step bound.
- **One runtime process** — `relay start` runs in the foreground and leaves supervision to Docker, systemd, Kubernetes, or another process manager.

## How it works

Relay has two workloads: **functions** (triggered by events or schedules) and **services** (always long-lived):

```text
Functions    Events       → matched invocation
             Schedules    → published occurrence → matched invocation
Services     → continuously reconciled containers
```

Both triggers land in the same event pipeline; services never do. An app may declare any mix of these.

### Events

For event-driven workloads, Relay follows this path:

```text
Redis Stream
    ↓
event matcher
    ↓
invocation state / retries
    ↓
runner
    ↓
isolated container
```

A stream message may match one or more handlers across one or more apps. Relay only acknowledges the message after every matched invocation reaches a terminal state.

### Schedules

Schedules follow the same execution path after publication:

```text
cron
  ↓
deduplicated occurrence publication
  ↓
Redis Stream
  ↓
normal Relay execution
```

### Services

Persistent services do not pass through the event execution pipeline.

Instead, Relay continuously reconciles their declared configuration:

```text
service declaration
       ↓
service reconciler
       ↓
desired replicas / configuration
       ↓
long-running containers
```

Relay keeps those containers aligned with the declared service configuration, restarting or replacing them when necessary.

Relay watches `/apps` for changes. When an app changes, Relay reconciles only the affected app and rebuilds its managed image only when build inputs actually change. Container-only changes such as resource limits do not require a new image.

## Guarantees

Relay is designed around explicit delivery, recovery, and convergence semantics.

- **Handler execution is at-least-once, not exactly-once.** A crash after a handler performs a side effect but before completion is recorded may cause the handler to run again. Handlers should be idempotent where side effects require it.
- **Matched work is not acknowledged while unresolved.** Running invocations, retry backoff, and other non-terminal states keep the Redis message pending.
- **Unavailable apps do not turn matched work into unmatched work.** Their invocations remain recoverable rather than being acknowledged as if nothing matched.
- **Retries preserve invocation coordination.** Stale claim owners cannot overwrite newer invocation state.
- **Exhausted invocations enter the DLQ.** DLQ state is tracked per invocation rather than per whole stream message.
- **Schedule publication is deduplicated cluster-wide.** Multiple workers may evaluate the same cron occurrence, but only one stream entry is admitted for that logical occurrence.
- **Schedule deduplication does not imply exactly-once execution.** Once published, scheduled handlers follow the same at-least-once execution model as any other event.
- **Redis pending work is recoverable.** Unacknowledged messages remain subject to normal PEL/reclaim handling.
- **Persistent services converge toward declared state.** Relay continuously reconciles service containers against their configured replicas and runtime configuration.
- **Warm container generations converge safely.** Idle stale containers are retired while busy old-generation containers are allowed to drain.
- **Resource limits are per container.** Increasing app concurrency or service replicas multiplies the possible aggregate resource usage.
- **Shutdown uses a cleanup budget and strict dependency barriers.** Relay performs ordered graceful teardown under an aggregate deadline that caps each best-effort cleanup step (for example metrics, webhook, service-container cleanup, and the stats flush) by the budget remaining at that point; when a best-effort step misses its bound the timeout is logged, its context is cancelled, and the registry proceeds without waiting for that operation to finish — an uncooperative operation may therefore continue in the background — though every later step is still attempted. Steps that gate a shared dependency (scheduler, reconciler, startup housekeeping, the service coordinator, and the background loops) are quiescence barriers: each has a per-step timeout used only to log and cancel the step, after which the registry waits for the operation to actually exit before advancing. Cancellation requests a stop but does not instantly terminate in-flight work, so those strict joins can extend total shutdown beyond the aggregate deadline — a wedged dependency-holding operation is waited out rather than used to close a resource another operation is still using.

## Quick start

Relay requires:

- Redis
- access to a Docker Engine

The bundled `compose.yaml` starts Relay with Redis and a Docker socket proxy and mounts the example apps at `/apps`.

```sh
docker compose up -d
docker compose ps
docker compose logs -f relay
```

Publish an event that matches the `order-confirmation-typescript` example:

```sh
docker compose exec redis redis-cli XADD events '*' \
  event '{"type":"order.created","order_id":"ord_42","customer_email":"jane@example.com","total":99.5}'
```

Then watch Relay execute the matching handler:

```sh
docker compose logs -f relay
```

Stop the stack with:

```sh
docker compose down -v
```

> **Security warning:** access to the Docker Engine is privileged.
>
> The example Compose setup exposes the Docker API through a dedicated socket proxy with a restricted endpoint allow-list. Relay still requires mutating Docker operations such as building or pulling images and creating containers, so access should be treated as highly privileged.

### Building from source

Building the Relay binary itself requires Go 1.27:

```sh
go build -o relay ./cmd
```

Run it against your own Redis and Docker Engine:

```sh
REDIS_URI=localhost:6379 \
REDIS_STREAM=events \
REDIS_GROUP=relay \
./relay start
```

Relay reads apps from the fixed `/apps` root.

## Apps

Each app is a direct child of `/apps` and contains a `template.yaml`.

A small event-driven app might look like:

```yaml
runtime: node24
concurrency: 4

resources:
  memory: 256MiB
  cpus: 0.5
  pids: 256

events:
  - handler: handler.handler
    pattern:
      event_name: [INSERT]
      table_name: [users]
    timeout: 10s
    retries: 2
```

Relay-managed runtime apps are prepared as reusable container images and executed through the runtime pool.

The same template can also declare:

- environment variables
- secret references
- schedules
- persistent services
- resource limits
- routing

See [docs/apps.md](docs/apps.md) for the full app model and [docs/events.md](docs/events.md) for matching, retries, ACK, and DLQ behavior.

## Persistent services

Relay can manage long-running workloads alongside event-driven functions and schedules.

A service is continuously reconciled toward its declared state rather than invoked by an event. This makes services suitable for HTTP APIs, background workers, gateways, consumers, and other processes expected to remain running.

Services can either reuse Relay's managed runtime or run an external container image directly.

### Managed runtime services

A managed service uses the same source tree and runtime image model as an event-driven app.

For example:

```yaml
runtime: python3.14

env:
  APP_ENV: production

resources:
  memory: 512MiB
  cpus: 1

services:
  - name: api
    entrypoint: app/main.py
    port: 8000
    replicas: 2
```

Relay prepares the runtime image and keeps the declared service replicas running.

This is useful when the same app contains both event-driven functions and long-running processes:

```text
app
├── event functions
├── scheduled functions
└── persistent API / worker
```

They can share the runtime, source tree, environment, secrets, resources, and deployment model while still having different execution semantics.

### External image services

A service may also run an existing container image:

```yaml
services:
  - name: gateway
    image: nginx:latest
    port: 80
```

These are intentionally different ownership models:

```text
Managed runtime entrypoint
  Relay prepares the runtime image
  Relay owns the managed image lifecycle
  Relay runs and reconciles the service

External image
  Relay pulls and runs the image as-is
  Relay reconciles the service containers
  Relay does not build or own that image
```

Services may use app-level environment variables, secrets, the worker-global `NETWORKS`, resources, replicas, and routing configuration.

Routed services may additionally use `TRAEFIK_NETWORK`.

In short:

```text
Functions are invoked.
Services are converged.
```

Start with a function. Add a service when you need one.

See [docs/services.md](docs/services.md).

## Schedules

Schedules are evaluated locally by every Relay worker, but publication is deduplicated by logical occurrence before entering the Redis stream.

```text
worker A ─┐
worker B ─┼─→ same occurrence → atomic publish-if-new → Redis Stream
worker C ─┘
```

This avoids leader election while preserving a single published stream entry per occurrence cluster-wide.

Publication retries — including a durable local retry of a tick whose immediate
publish did not resolve — and startup catch-up reuse the same occurrence
identity, so duplicates remain harmless.

Handler execution after publication is still at-least-once.

See [docs/schedules.md](docs/schedules.md).

## Managed images and external images

Relay distinguishes between image identity and container configuration.

Managed runtime images may be shared by event-driven apps and managed-runtime services.

They are rebuilt only when their build inputs change.

Changes such as:

- memory limits
- CPU limits
- PID limits
- networks
- runtime environment
- service replicas
- routing
- other container-only configuration

may require new containers without requiring a new image.

External service images are not built or garbage-collected as Relay-owned runtime images.

This separation keeps build lifecycle and runtime convergence independent.

## Commands

```text
relay start

relay health

relay stats
relay stats reset

relay app ls
relay app inspect <name>
relay app invoke <name>

relay secret ls
relay secret set <name>
relay secret rm <name>

relay git keygen
relay git set <repository>
relay git sync
relay git status
relay git remove

relay dlq ls
relay dlq inspect <id>
relay dlq replay <id>
relay dlq rm <id>
```

Use:

```sh
relay <command> --help
```

for command-specific flags.

See [docs/cli.md](docs/cli.md) for the detailed CLI reference.

## Documentation

| Document                                       | Covers                                                                                 |
| ---------------------------------------------- | -------------------------------------------------------------------------------------- |
| [docs/configuration.md](docs/configuration.md) | Relay environment variables, defaults, and process-level configuration                 |
| [docs/apps.md](docs/apps.md)                   | App layout, templates, runtimes, environment, secrets, resources, and warm containers  |
| [docs/events.md](docs/events.md)               | Event matching, dispatch, retries, ACK semantics, invocation state, and DLQ            |
| [docs/schedules.md](docs/schedules.md)         | Cron syntax, timezones, occurrence identity, publication deduplication, and recovery   |
| [docs/services.md](docs/services.md)           | Persistent services, external images, routing, networks, replicas, and resources       |
| [docs/operations.md](docs/operations.md)       | Lifecycle, state, recovery, metrics, tracing, logs, Git sync, and operational behavior |
| [docs/cli.md](docs/cli.md)                     | Commands, flags, and prerequisites                                                     |

## Development

Run the standard validation suite before submitting changes:

```sh
gofmt -w <changed-go-files>
go vet ./...
go test ./...
go test -race ./...
```

Integration tests may require Redis and Docker.

## License

Relay is licensed under the GNU General Public License v3.0.

See [LICENSE.md](LICENSE.md).
