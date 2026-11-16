# Relay

Relay is an event-driven serverless runtime for apps that combine functions,
scheduled jobs, and persistent services. It runs on infrastructure you control
and uses Redis Streams[^1] for external events and Docker[^2] for workload
execution.

Functions execute handlers triggered by event patterns or schedules, with
automatic retries and dead-letter queues for failed executions. Persistent
services run continuously and are reconciled to their declared configuration.

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

- **Event-driven functions** — Redis Streams, declarative event matching,
  at-least-once delivery, automatic retries, pending-message recovery, and
  dead-letter queues.
- **Managed runtimes** — Python 3.14/3.15 and Node.js 24/26, with JavaScript,
  TypeScript support, reusable warm containers, and bounded concurrency.
- **Cron schedules** — Minute-precision scheduling, IANA timezones,
  cluster-wide deduplication, publication recovery, and startup catch-up.
- **Persistent services** — Run APIs, workers, gateways, and other long-running
  processes using managed runtimes or container images, with replicas,
  networking, and optional Traefik routing.
- **Resource controls** — Per-container memory, CPU, and PID limits for
  functions and services.
- **Configuration and secrets** — Environment variables and secret references
  for applications, functions, and services.
- **Observability** — Prometheus metrics, structured logs, persisted
  statistics, and OpenTelemetry tracing.
- **Operations** — Health checks, manual invocation, DLQ inspection and replay,
  Git synchronization, and graceful shutdown.
- **Flexible deployment** — Run the Relay process under Docker, systemd,
  Kubernetes, or another process manager.

See [Apps](docs/apps.md), [Events](docs/events.md),
[Schedules](docs/schedules.md), and [Services](docs/services.md) for workload
behavior.

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

An app is a direct child of `/apps` with a `template.yaml`. A template can
combine event handlers, schedules, and persistent services. This MongoDB CDC
rule matches inserts to the `users` collection (use `tableName` for PostgreSQL):

```yaml
runtime: nodejs24.x

events:
  - handler: handler.handler
    pattern:
      recordType: [INSERT]
      collectionName: [users]
```

See [Apps](docs/apps.md) for the template reference, [Events](docs/events.md)
for matching and delivery, [Schedules](docs/schedules.md) for cron triggers,
and [Services](docs/services.md) for long-running workloads. Changes under
`/apps` are applied live; see
[Configuration](docs/configuration.md#hot-reload).

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
docker compose exec -T relay relay app invoke my-app - < event.json

relay secret set API_KEY  # hidden prompt (requires a terminal)
printf '%s' 'value' | relay secret set API_KEY -
docker compose exec -T relay relay secret set API_KEY - < value.txt
```

## Documentation

| Document                               | Covers                                      |
| -------------------------------------- | ------------------------------------------- |
| [Installation](docs/installation.md)   | Deployment and setup                        |
| [Configuration](docs/configuration.md) | Environment variables and defaults          |
| [Apps](docs/apps.md)                   | Templates, runtimes, secrets, and resources |
| [Events](docs/events.md)               | Event matching, retries, delivery, and DLQ  |
| [Schedules](docs/schedules.md)         | Cron, timezones, and recovery               |
| [Services](docs/services.md)           | Services, replicas, networking, and routing |
| [Operations](docs/operations.md)       | Monitoring, recovery, logs, and Git sync    |
| [CLI](docs/cli.md)                     | Commands, flags, and prerequisites          |

## Contributing

See [development and testing](docs/development.md) for build and validation
instructions.

## License

Relay is licensed under the GNU General Public License v3.0.

See [LICENSE.md](LICENSE.md).

[^1]:
    Support for additional event brokers is planned
    ([#9](https://github.com/sergiors/relay/issues/9)).

[^2]:
    Support for additional runtime backends, including Kubernetes, is planned
    ([#10](https://github.com/sergiors/relay/issues/10)).
