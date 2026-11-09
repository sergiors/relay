# Relay

Relay is an event-driven runtime for declarative apps — with serverless functions,
schedules, and persistent services. Run it on infrastructure you control.

It executes short-lived functions in isolated runtimes, publishes scheduled
occurrences to the same Redis Stream and consumer group as external events, and
continuously reconciles long-running services from the same declarative model.

For external events, Relay consumes stream messages, classifies each event
against declarative patterns, executes the matching functions, retries failures,
and dead-letters exhausted invocations. Scheduled occurrences use a separate
dispatch path that resolves the schedule's handler directly, without ordinary
event classification or pattern matching.

Persistent services use the same runtime, configuration, secrets, networking,
resource controls, and container infrastructure without requiring them to
participate in the event pipeline.

**An app is the unit of source, configuration, and deployment.** Within an
app, event-driven **functions**, schedules, and optional persistent services
share one declarative model. A handler is the execution target for a one-shot
function; an event pattern or schedule trigger determines when that function
runs. Services are separate persistent workloads.

Functions remain a first-class Relay workload. Start with a function.
Add a service when you need one.

Relay does not care where events originate:

```text
Producer → Redis Stream → Relay → App → Function (handler)
```

Functions, schedules, and persistent services are declared within an app and share
the same deployment model while keeping their execution semantics independent.

## Getting Started

You need Docker Engine and Docker Compose.

```sh
mkdir relay && cd relay
curl -fsSLO https://raw.githubusercontent.com/sergiors/relay/main/compose.yaml
mkdir apps
docker compose up -d
docker compose exec relay relay health
```

The stack publishes unauthenticated Redis and development-credential PostgreSQL
on host ports, and Relay mounts the host Docker socket read-only to manage
containers. The read-only mount does not make the Docker API read-only: a
compromised Relay process can exercise the daemon's full authority (often
equivalent to host root). Treat Docker Engine access as highly privileged, and
do not expose this stack to untrusted networks.

## What you get

- **Event-driven execution** — Redis Streams, declarative matching, retries, reclaim, dead-lettering, and at-least-once delivery.
- **Managed runtimes** — Python 3.14/3.15 (`python3.14`/`python3.15`) and
  Node.js 24/26 (`nodejs24.x`/`nodejs26.x`), with TypeScript support, reusable
  warm containers, and bounded concurrency.
- **Cron schedules** — minute-precision cron with IANA timezones, deterministic occurrence identity, cluster-wide publication deduplication, bounded retries with a durable local outbox, and startup catch-up.
- **Persistent services** — run APIs, workers, gateways, consumers, and other long-running processes from managed-runtime entrypoints or external container images, with replicas, Docker networks, resource limits, environment configuration, secrets, and optional Traefik routing.
- **Resource controls** — per-container memory, CPU, and PID limits.
- **Configuration and secrets** — environment values and secret references shared across apps, schedules, and services.
- **Observability** — persisted statistics, Prometheus metrics, structured logs, and OpenTelemetry tracing.
- **Operations** — health checks, manual invocation, DLQ inspection/replay, and Git synchronization. Dependency barriers may extend graceful shutdown while in-flight work finishes.
- **One runtime process** — `relay start` runs in the foreground and leaves supervision to Docker, systemd, Kubernetes, or another process manager.

## How it works

Relay supports event-driven **functions**, scheduled functions, and persistent
**services**. Event messages contain a JSON object in Redis Stream `value`; Relay
matches decoded events against app patterns and runs every matching handler.
Schedule occurrences use a separate dispatch path and bypass event matching.
Services are long-running containers reconciled to their app configuration.

See [Events](docs/events.md), [Schedules](docs/schedules.md), and
[Services](docs/services.md) for the public behavior of each workload. Relay
applies app changes from `/apps` live; see
[Configuration](docs/configuration.md#hot-reload) for source handling.

## Guarantees

- Event and schedule delivery is **at least once**, not exactly once. Handlers
  that cause side effects should be idempotent.
- Relay keeps messages pending while matched work is unresolved. It acknowledges
  only after all matched invocations succeed or are durably dead-lettered.
- Unavailable apps and temporary capacity limits do not cause matching work to
  be acknowledged or consume a retry.
- Schedule publication is deduplicated cluster-wide; handler execution still
  follows at-least-once delivery.
- Persistent services are reconciled toward their declared replicas and
  configuration.
- Resource limits apply per container; adding concurrency or replicas increases
  aggregate resource use.

See [Events](docs/events.md) and [Schedules](docs/schedules.md) for delivery and
recovery details, and [Operations](docs/operations.md) for deployment guidance.

## Apps

Each app is a direct child of `/apps` and contains a `template.yaml`.

A small event-driven app might look like:

```yaml
runtime: nodejs24.x
concurrency: 4

resources:
  memory: 256MiB
  cpus: 0.5
  pids: 256

events:
  - handler: handler.handler
    pattern:
      recordType: [INSERT]
      collectionName: [users]
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
├── functions triggered by events or schedules
└── persistent API / worker
```

They can share the runtime, source tree, environment, secrets, resources, and deployment model while still having different execution semantics.

### External image services

A service may also run an existing container image:

```yaml
services:
  - name: gateway
    image: nginxinc/nginx-unprivileged:1.27-alpine
    port: 8080
```

External images keep their own `USER` and `ENTRYPOINT`; Relay never forces a
UID on them. The shared hardening still drops capabilities, sets
`no-new-privileges`, and uses a read-only rootfs, so prefer images that already
run as a non-root user and listen on an unprivileged port. The official
`nginx` image expects to chown and setuid as root, which the hardened baseline
does not permit.

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

Schedule publication is deduplicated cluster-wide by occurrence. A durable
outbox retries unresolved publications across restarts; handler execution remains
at least once. See [docs/schedules.md](docs/schedules.md).

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
relay app invoke <name> [json|-]

relay runtime ls
relay runtime inspect <runtime>
relay runtime check [runtime]
relay runtime update <runtime>
relay runtime update --all

relay secret ls
relay secret set <name> [-]
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

Manual invocation takes a JSON object, or `-` to read it from stdin. Secret
values can be entered at a hidden prompt or read from stdin; they are never
command-line arguments. For command details, see [CLI](docs/cli.md).

```sh
relay app invoke my-app '{"recordType":"INSERT"}'
relay app invoke my-app - < event.json

# -T keeps stdin a pipe instead of an allocated pseudo-TTY.
docker compose exec -T relay app invoke my-app - < event.json

relay secret set API_KEY                          # hidden prompt (requires a terminal)
printf '%s' 'value' | relay secret set API_KEY -  # read all of stdin
docker compose exec -T relay secret set API_KEY - < value.txt
```

## Documentation

| Document                                       | Covers                                                                                 |
| ---------------------------------------------- | -------------------------------------------------------------------------------------- |
| [docs/installation.md](docs/installation.md)   | Self-hosted deployment, configuration, and operations                                  |
| [docs/configuration.md](docs/configuration.md) | Relay environment variables, defaults, and process-level configuration                 |
| [docs/apps.md](docs/apps.md)                   | App layout, templates, runtimes, environment, secrets, resources, and warm containers  |
| [docs/events.md](docs/events.md)               | Event matching, dispatch, retries, ACK semantics, invocation state, and DLQ            |
| [docs/schedules.md](docs/schedules.md)         | Cron syntax, timezones, occurrence identity, publication deduplication, and recovery   |
| [docs/services.md](docs/services.md)           | Persistent services, external images, routing, networks, replicas, and resources       |
| [docs/operations.md](docs/operations.md)       | Lifecycle, state, recovery, metrics, tracing, logs, Git sync, and operational behavior |
| [docs/cli.md](docs/cli.md)                     | Commands, flags, and prerequisites                                                     |

## Contributing

See [development and testing](docs/development.md) for build and validation
instructions.

## License

Relay is licensed under the GNU General Public License v3.0.

See [LICENSE.md](LICENSE.md).
