# Relay

Relay is a self-hosted, event-driven runtime that brings a
managed-serverless-inspired developer experience to the infrastructure you
already run. You declare functions, schedules, and persistent services; Relay
builds your runtime-backed functions from source, runs external service images
as-is, routes events to the right handlers, retries failures, dead-letters
exhaustion, and keeps long-running services converged — with no hosted control
plane.

It consumes events from a Redis Stream, matches each event against declarative
patterns, and executes the matching handlers in isolated Docker containers. A
message is acknowledged only after every matching invocation is terminal —
succeeded, or exhausted through retries into a dead-letter stream.

Relay does not care where events originate. It only reads from Redis and runs
functions:

```
Producer → Redis Stream → Relay (match) → runner → container → XACK
```

## What you get

- **Event matching and delivery** — Redis Stream consumption with a declarative
  pattern language (`equals`, `prefix`, `suffix`, `exists`, numeric and
  `now()`-relative comparisons), per-rule timeouts and retries, at-least-once
  acknowledgment, and a dead-letter stream.
- **Runtimes and containers** — Python 3.14 and Node 24 (JavaScript and
  TypeScript); a managed image per runtime-backed function; bounded warm
  container pools and per-container memory/CPU/PID limits.
- **Cron schedules** — minute-precision cron with IANA timezones; every worker
  evaluates schedules locally, but publication is deduplicated so exactly one
  stream entry exists per occurrence cluster-wide.
- **Persistent services** — long-running `entrypoint` or external `image`
  containers with ports and replicas, optionally routed through Traefik and
  attached to operator-provided Docker networks.
- **Operations** — env values and secret references, manual Git sync plus an
  optional GitHub webhook, `health`/`stats` CLI commands, and Prometheus metrics
  with OTLP tracing.
- **One binary** — `relay start` loads, watches, builds, and consumes; the other
  subcommands are admin/inspection only.

## How it works

At startup Relay discovers the functions under `/functions` (one directory per
function), builds an image for each runtime-backed function, and ensures the
Redis consumer group exists. For every stream message it:

1. evaluates every function's rules (patterns in `template.yaml`);
2. runs each matching handler in turn on the runtime image — one at a time per
   message, but concurrently across messages and workers;
3. acknowledges the message (XACK) only after every matching invocation is
   terminal.

It watches `/functions` and reconciles each function live, rebuilding only the
changed function and keeping the previous version serving if a build fails.
`relay start` runs in the foreground and never daemonizes; supervision belongs
to Docker, systemd, or Kubernetes.

## Guarantees

- **Delivery is at-least-once, never exactly-once.** A crash between a handler's
  side effect and its recorded completion re-runs the handler. Handlers must be
  idempotent.
- A message is **never acknowledged** while any matched invocation is running,
  protected by a retry backoff, or otherwise unresolved.
- An event matching only an unavailable function is still **matched**; its
  invocation stays pending and is never dead-lettered for unavailability alone.
- Schedule **publication** is deduplicated cluster-wide; handler **execution**
  remains at-least-once. Exactly-once handler execution is not claimed.
- Unmatched and malformed messages are acknowledged; malformed messages are
  dead-lettered first. A reclaimed message whose stream body is gone is counted
  as data loss, never treated as success.

## Quick start

Relay needs a reachable Redis, a Docker daemon it can build and run containers
with, and Go 1.27 to build the binary from source. The bundled `compose.dev.yaml`
builds and runs Relay alongside Redis and a Docker-socket proxy, and mounts
`examples/functions` read-only at `/functions`:

```sh
docker compose -f compose.dev.yaml up -d --build
docker compose -f compose.dev.yaml ps          # relay is healthy once Redis + Docker respond
docker compose -f compose.dev.yaml logs -f relay
```

Relay reaches the host daemon through the socket proxy
(`DOCKER_HOST=tcp://socket-proxy:2375`); the Docker socket is mounted read-only
and **only** into the proxy. See the security note below.

Publish an event that matches the `order-confirmation-typescript` example
(TypeScript, no secrets required) and watch it run:

```sh
docker compose -f compose.dev.yaml exec redis redis-cli XADD events '*' \
  event '{"type":"order.created","order_id":"ord_42","customer_email":"jane@example.com","total":99.5}'
docker compose -f compose.dev.yaml logs -f relay
```

The handler prints an order confirmation; Relay forwards container stdout/stderr
verbatim, each line prefixed with the function/handler. Tear down with:

```sh
docker compose -f compose.dev.yaml down -v
```

> **Security warning: the Docker socket is privileged.** The socket is mounted
> read-only into a dedicated proxy that forwards only a curated allow-list of
> Engine API categories. That list includes mutating operations (image
> build/pull/remove, container create/start/remove), so it is an **endpoint
> allow-list, not read-only access**. Building images and running containers is
> effectively root-equivalent on the host. This tradeoff is accepted so Relay can
> build and run function images against the host daemon.

**Sample caveats.** Not every example works under stock Compose:

- `user-events-python` declares a `secrets.POSTGRES_DSN` reference and its
  created handler prints `POSTGRES_DSN`. It does **not** succeed until you run
  `relay secret set postgres-dsn`; do not copy that print into real handlers.
- `fastapi-service` declares a `host`, which needs Traefik and a routing network
  (`TRAEFIK_NETWORK`) that Compose does not provide.
- `users-api-node`, `external-image-service`, and the other examples run without
  extra infrastructure.

Without Compose, build and run the binary directly against your own Redis and
Docker:

```sh
go build -o relay ./cmd
REDIS_URI=localhost:6379 REDIS_STREAM=events REDIS_GROUP=relay ./relay start
```

For local development against `examples/functions`, mount that directory at
`/functions` (Relay reads a fixed `/functions` root; the path is not
configurable).

## A function

Each function is a direct subdirectory of `/functions` with a `template.yaml`.
A representative events template:

```yaml
runtime: node24
concurrency: 4

resources:
  memory: 256MiB # binary suffix: KiB, MiB, or GiB
  cpus: 0.5 # finite number > 0; fractional CPUs allowed
  pids: 256 # positive integer

events:
  - handler: handler.handler
    pattern:
      event_name: [INSERT]
      table_name: [users]
    timeout: 10s
    retries: 2
```

`handler` is `module.function`; the container gets `RELAY_HANDLER` set and the
event JSON on stdin, and its exit code decides success (`0`) or failure. The
same file also supports `env`/`secrets`, `schedules`, and persistent `services`.
See [docs/functions.md](docs/functions.md) for the full schema and
[docs/events.md](docs/events.md) for pattern syntax, retries, ACK, and the DLQ.

## Commands

```
relay start                                # run the runtime (foreground)
relay health                               # check Redis + Docker reachability
relay stats [reset]                        # show / reset persisted statistics
relay function ls | inspect <name>         # inspect loaded functions
relay function invoke <name> --event JSON  # run matching handlers on the live worker
relay dlq ls | inspect <id> | replay <id> | rm <id>
relay secret ls | set <name> | rm <name>
relay git keygen | set <repo> | sync | status | remove
```

Run `relay <command> --help` for flags. See [docs/cli.md](docs/cli.md) for
prerequisites per command.

## Documentation

| Document                                       | Covers                                                                                 |
| ---------------------------------------------- | -------------------------------------------------------------------------------------- |
| [docs/configuration.md](docs/configuration.md) | Every Relay environment variable, defaults, and validation behavior.                   |
| [docs/functions.md](docs/functions.md)         | Directory/template schema, runtimes, env/secrets, resources, warm containers.          |
| [docs/events.md](docs/events.md)               | Pattern syntax, dispatch, retries, ACK, and the DLQ.                                   |
| [docs/schedules.md](docs/schedules.md)         | Cron forms, timezone/identity, publication dedup, bounded recovery.                    |
| [docs/services.md](docs/services.md)           | Persistent entrypoint/image services and Traefik routing.                              |
| [docs/operations.md](docs/operations.md)       | Lifecycle, state paths, health/stats, metrics/tracing/logging, secrets, git, recovery. |
| [docs/cli.md](docs/cli.md)                     | Command hierarchy, flags, and prerequisites.                                           |

## License

Relay is licensed under the GNU General Public License v3.0. See [LICENSE.md](LICENSE.md) for details.
