# Relay

Relay is a self-hosted, event-driven runtime for running functions, schedules, and persistent services on infrastructure you already control.

It consumes events from Redis Streams, matches them against declarative rules, executes handlers in isolated Docker containers, retries failures, dead-letters exhausted invocations, and keeps long-running services converged — without a hosted control plane.

Relay does not care where events originate:

```text
Producer → Redis Stream → Relay → Function Handler
```

Scheduled workloads and persistent services use the same runtime and container infrastructure, so event-driven functions, cron jobs, and long-running services can live under one deployment model.

## What you get

- **Event-driven execution** — Redis Streams, declarative matching, retries, reclaim, dead-lettering, and at-least-once delivery.
- **Managed runtimes** — Python 3.14 and Node 24, including TypeScript support, with reusable warm containers and bounded concurrency.
- **Cron schedules** — minute-precision cron with IANA timezones, deterministic occurrence identity, cluster-wide publication deduplication, bounded retries, and startup catch-up.
- **Persistent services** — long-running services from managed-runtime entrypoints or external container images, with ports, replicas, Docker networks, resources, and optional Traefik routing.
- **Resource controls** — per-container memory, CPU, and PID limits.
- **Configuration and secrets** — environment values and secret references shared across functions, schedules, and services.
- **Observability** — persisted statistics, Prometheus metrics, structured logs, and OpenTelemetry tracing.
- **Operations** — health checks, manual invocation, DLQ inspection/replay, Git synchronization, and graceful bounded shutdown.
- **One runtime process** — `relay start` runs in the foreground and leaves supervision to Docker, systemd, Kubernetes, or another process manager.

## How it works

For events, Relay follows this path:

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

A stream message may match multiple functions or handlers. Relay only acknowledges the message after every matched invocation reaches a terminal state.

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

Persistent services are reconciled separately and kept aligned with their declared configuration.

Relay watches `/functions` for changes. When a function changes, Relay reconciles only the affected function and rebuilds its managed image only when build inputs actually change. Container-only changes such as resource limits do not require a new image.

## Guarantees

Relay is designed around explicit delivery and recovery semantics.

- **Handler execution is at-least-once, not exactly-once.** A crash after a handler performs a side effect but before completion is recorded may cause the handler to run again. Handlers should be idempotent where side effects require it.
- **Matched work is not acknowledged while unresolved.** Running invocations, retry backoff, and other non-terminal states keep the Redis message pending.
- **Unavailable functions do not turn matched work into unmatched work.** Their invocations remain recoverable rather than being acknowledged as if nothing matched.
- **Retries preserve invocation coordination.** Stale claim owners cannot overwrite newer invocation state.
- **Exhausted invocations enter the DLQ.** DLQ state is tracked per invocation rather than per whole stream message.
- **Schedule publication is deduplicated cluster-wide.** Multiple workers may evaluate the same cron occurrence, but only one stream entry is admitted for that logical occurrence.
- **Schedule deduplication does not imply exactly-once execution.** Once published, scheduled handlers follow the same at-least-once execution model as any other event.
- **Redis pending work is recoverable.** Unacknowledged messages remain subject to normal PEL/reclaim handling.
- **Warm container generations converge safely.** Idle stale containers are retired while busy old-generation containers are allowed to drain.
- **Resource limits are per container.** Increasing function concurrency or service replicas multiplies the possible aggregate resource usage.
- **Shutdown is bounded.** Relay performs ordered graceful shutdown without allowing one non-cooperative component to block termination indefinitely.

## Quick start

Relay requires:

- Redis
- access to a Docker Engine

The bundled `compose.yaml` starts Relay with Redis and a Docker socket proxy and mounts the example functions at `/functions`.

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

Relay reads functions from the fixed `/functions` root.

## Functions

Each function is a direct child of `/functions` and contains a `template.yaml`.

A small event-driven function might look like:

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

Relay-managed runtime functions are prepared as reusable container images and executed through the runtime pool.

The same template can also declare:

- environment variables
- secret references
- schedules
- persistent services
- Docker networks
- resource limits
- routing

See [docs/functions.md](docs/functions.md) for the full function model and [docs/events.md](docs/events.md) for matching, retries, ACK, and DLQ behavior.

## Persistent services

A function may also declare long-running services.

Using a managed runtime entrypoint:

```yaml
runtime: python3.14

services:
  - name: api
    entrypoint: app/main.py
    port: 8000
```

Or an external image:

```yaml
services:
  - name: gateway
    image: nginx:latest
    port: 80
```

These are intentionally different ownership models:

```text
Managed runtime entrypoint
  Relay prepares the runtime image and runs the service

External image
  Relay pulls and runs the image as-is
  Relay does not build or own that image
```

Services may also use function-level environment variables, secrets, Docker
networks (the worker-global `NETWORKS` plus `TRAEFIK_NETWORK` for routed
services), resources, replicas, and routing configuration.

See [docs/services.md](docs/services.md).

## Schedules

Schedules are evaluated locally by every Relay worker, but publication is deduplicated by logical occurrence before entering the Redis stream.

```text
worker A ─┐
worker B ─┼─→ same occurrence → atomic publish-if-new → Redis Stream
worker C ─┘
```

This avoids leader election while preserving a single published stream entry per occurrence cluster-wide.

Publication retries and startup catch-up reuse the same occurrence identity, so duplicates remain harmless.

Handler execution after publication is still at-least-once.

See [docs/schedules.md](docs/schedules.md).

## Managed images and external images

Relay distinguishes between image identity and container configuration.

Managed runtime images are rebuilt only when their build inputs change.

Changes such as:

- memory limits
- CPU limits
- PID limits
- networks
- runtime environment
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

relay function ls
relay function inspect <name>
relay function invoke <name>

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

| Document                                       | Covers                                                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [docs/configuration.md](docs/configuration.md) | Relay environment variables, defaults, and process-level configuration                     |
| [docs/functions.md](docs/functions.md)         | Function layout, templates, runtimes, environment, secrets, resources, and warm containers |
| [docs/events.md](docs/events.md)               | Event matching, dispatch, retries, ACK semantics, invocation state, and DLQ                |
| [docs/schedules.md](docs/schedules.md)         | Cron syntax, timezones, occurrence identity, publication deduplication, and recovery       |
| [docs/services.md](docs/services.md)           | Persistent services, external images, routing, networks, replicas, and resources           |
| [docs/operations.md](docs/operations.md)       | Lifecycle, state, recovery, metrics, tracing, logs, Git sync, and operational behavior     |
| [docs/cli.md](docs/cli.md)                     | Commands, flags, and prerequisites                                                         |

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
