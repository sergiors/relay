# Relay

Relay is an event-driven function runner. It consumes events from a Redis
Stream, matches each event against declarative patterns, selects the matching
handlers, executes them with managed runtimes in isolated containers, and
acknowledges successfully processed messages.

Relay does not know or care where events originate. It only reads from Redis
Streams and runs functions:

```
Event Producer → Redis Streams → Relay → Function Handler
```

## How it works

At startup Relay discovers the functions under `/functions` (one directory
per function), builds one image per function, and creates (if missing) the Redis
consumer group. It then watches `/functions` for changes and reconciles each
function on the fly. It then blocks
on the stream with a consumer group, decodes each message, and for every event:

1. evaluates every function's rules (declarative patterns in `template.yaml`),
2. executes each matching handler **sequentially** in an isolated container,
3. acknowledges the message (XACK) only after **all** matching invocations
   succeed.

## How to run

```sh
go build -o relay ./cmd && ./relay
# or
go run ./cmd
```

Requirements: Go 1.27+, a reachable Redis, and a local Docker daemon (see
below). No events are processed until a producer XADDs to the stream.

## Docker requirement

Relay talks to the **Docker Engine API** directly (via the moby client) to
build images and run containers; it does not require the Docker CLI to be
installed. A running Docker daemon must be reachable on the host. At startup
Relay pings the daemon and fails fast with a clear error if it cannot connect.
Image builds and handler invocations use the **local Docker daemon** on the
host that runs Relay. The client is configured from the environment
(`DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH`) and negotiates the API
version automatically.

The daemon must be permitted to execute arbitrary containers, so Relay needs
full host-level Docker permission (e.g. the user running Relay must be a member
of the `docker` group or otherwise have access to the Docker socket).

When Relay itself runs inside a container, do **not** mount the host's Docker
socket into Relay. Instead, run Relay against a Docker-socket proxy that
allow-lists the Engine API endpoints Relay actually uses
(`DOCKER_HOST=tcp://socket-proxy:2375` over an internal network):

```yaml
services:
  socket-proxy:
    image: tecnativa/docker-socket-proxy
    environment:
      - PING=1
      - VERSION=1
      - BUILD=1
      - CONTAINERS=1
      - POST=1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro

  relay:
    image: relay:latest
    depends_on:
      - socket-proxy
    environment:
      - DOCKER_HOST=tcp://socket-proxy:2375
      - REDIS_ADDR=redis:6379
```

> **Security warning: the Docker socket is privileged.**
> The socket is mounted read-only into a dedicated proxy container, and the
> proxy forwards only a curated allow-list of Docker Engine API requests (an
> endpoint allow-list plus read-only access) instead of granting Relay the full
> socket. This reduces Relay's attack surface against the daemon, but it is
> **not** a strong security boundary and it does **not** make Docker execution
> unprivileged: Relay still builds images and creates/runs containers, which is
> effectively root-equivalent on the host. This tradeoff is accepted at this
> stage of the project so Relay can build and run function images against the
> host daemon. Separating the executor into a remote/privileged sidecar or a
> properly isolated build+run service is out of scope for this iteration.

### Development with Compose

`compose.dev.yaml` runs Relay in a container alongside a Redis service and a
Docker-socket proxy, so you can develop against the same containerized
deployment the README above describes without installing Go or Redis locally:

```sh
docker compose -f compose.dev.yaml up --build -d
docker compose -f compose.dev.yaml logs -f relay
```

Relay does **not** mount the Docker socket. Instead the topology is:

```
Relay → socket-proxy → Docker daemon
```

Relay reaches the host daemon through the proxy over the internal compose
network with `DOCKER_HOST=tcp://socket-proxy:2375` (Relay's moby client honors
`DOCKER_HOST` via `FromEnv`). The socket itself is mounted **read-only and only
into the `socket-proxy` container**, which forwards just the Engine API
endpoints Relay needs (ping/version, image build, and container create/attach/
start/wait/kill/remove). Port `2375` is **not** exposed to the host, so the
proxy is reachable only from the compose network. `./functions` is still
mounted read-only into Relay at `/functions`. The socket mount is privileged (see the security
warning above); this is a dev-only convenience. Tear down with
`docker compose -f compose.dev.yaml down -v`.

## Configuration

| Env var          | Default          | Description                     |
| ---------------- | ---------------- | ------------------------------- |
| `REDIS_ADDR`     | `localhost:6379` | Redis address.                  |
| `REDIS_STREAM`   | `events`         | Redis stream to consume.        |
| `REDIS_GROUP`    | `relay`          | Consumer group name.            |
| `REDIS_CONSUMER` | `worker-1`       | Consumer name within the group. |

`DOCKER_HOST` (and the other Docker client variables `DOCKER_TLS_VERIFY`,
`DOCKER_CERT_PATH`) are consumed by Relay through the Docker client at startup
(see _Docker requirement_); Relay itself does not parse them.

Relay's reliability settings — retry/delivery limits and the recovery loop — are
fixed internals, not env-configurable. See _Reliability defaults_ below.

Multiple Relay instances may share the same `REDIS_GROUP` with different
consumer names to scale out consuming. The consumer group is created
automatically (with `MKSTREAM`) if the stream or group does not exist; the
group is created at position `0`, so only messages added after startup are
consumed.

## Functions

Each direct subdirectory of `/functions` is one function. The directory name
is the function name. Each function directory must contain a `template.yaml`
that declares which runtime to use and which events it handles.

- A directory without a `template.yaml` is ignored.
- An invalid `template.yaml` is logged and skipped — it never prevents Relay
  from starting.
- A function whose image cannot be built is logged and marked unavailable; the
  other functions continue to be served.

## Template format

```yaml
runtime: python3.14

events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
      table_name: [users]

  - handler: events.updated.handler
    pattern:
      event_name: [MODIFY]
      table_name: [users]
    timeout: 20s

  - handler: events.deleted.handler
    pattern:
      event_name: [REMOVE]
      table_name: [users]
```

- `runtime` (required) selects the execution runtime. Only `python3.14` and
  `node24` are supported; any other value fails validation.
- `events` is a list of rules. Each rule has a required `handler` (of the form
  `module.function`), a required `pattern`, and an optional `timeout`.
- `timeout` (optional, per rule) is a Go duration string bounding a single
  invocation of that rule's handler (e.g. `20s`, `1m30s`). It must be positive.
  Zero, negative, or unparseable values fail the function's template validation
  (the function is logged and skipped). Omitted rules use a `6s` default.
- `handler` is split at the **last** dot: `events.created.handler` → module
  `events.created`, function `handler`. Handlers may live in nested modules
  (for example the `events/` package), not only in top-level files.

A pattern is a tree of field conditions:

- A plain YAML list is implicit equality: `status: [COMPLETED, FAILED]`.
- A map with only operator keys (`equals`, `prefix`, `suffix`) holds operators,
  each with a list of values.
- A nested map without operator keys holds nested field conditions.

### Operators

| Operator | Semantics                                                |
| -------- | -------------------------------------------------------- |
| `equals` | Value equals any of the listed values (type-preserving). |
| `prefix` | String value starts with any of the listed prefixes.     |
| `suffix` | String value ends with any of the listed suffixes.       |

### Semantics

- Values within one operator's list are **OR**.
- Different fields (sibling keys) are **AND**.
- Multiple operators on the same field are **OR** (alternatives).
- Each rule's pattern is evaluated independently; multiple rules may match the
  same event, and no deduplication is performed (two matching rules referencing
  the same handler are both invoked).
- A missing event field fails that condition.
- Extra event fields are ignored.
- `prefix` and `suffix` only match string values; non-string values never match.

For example, a rule with `status: [COMPLETED, FAILED]` matches while
`id: { prefix: ["user_"] }` matches `user_123` but not `123`. Only
`equals`/`prefix`/`suffix` are implemented.

## Supported runtimes

| Runtime      | Base image         | Dependency handling                                                                                  |
| ------------ | ------------------ | ---------------------------------------------------------------------------------------------------- |
| `python3.14` | `python:3.14-slim` | `requirements.txt` → `pip install --no-cache-dir -r requirements.txt`                                |
| `node24`     | `node:24-alpine`   | `package-lock.json` → `npm ci --omit=dev`; else `package.json` → `npm install --omit=dev`; else none |

Base images are fixed; arbitrary base images are not allowed. Dependencies are
installed **inside** the image at build time, never on the host.

For Node functions, if no `package.json` exists a minimal `{"type":"module"}`
package.json is injected so `.js` files are treated as ESM; if a user
`package.json` exists, its `type` field is respected.

Both runtimes support **sync and async** handlers. The Python bootstrap invokes
the handler and, if the result is awaitable, runs it with `asyncio.run`; the
Node bootstrap resolves the handler module by filesystem lookup (not by
attempting an import), imports it, and awaits a returned Promise.

Adding a future runtime (e.g. `python3.15` or `node26`) requires only a new
entry in the runtime registry map; the engine is reused.

## Handler contract

Each execution runs a container with the environment variable
`RELAY_HANDLER` set to the rule's handler (e.g. `events.created.handler`) and
the event JSON written to the container's stdin. An embedded bootstrap resolves
the module and function from `RELAY_HANDLER`, reads the event from stdin, and
invokes the function. The container's exit code decides the result: `0` is
success, non-zero is failure. The container's stdout and stderr are forwarded
to Relay's logs.

## Execution

- **One image per function**, never per handler or event. A function's single
  image is built at startup and, afterwards, rebuilt only when its directory
  changes (see *Hot reload* below); the rebuilt image serves all of its
  handlers.
- **Sequential execution**: for each event, functions are iterated in order,
  then rules in order, and each matching handler runs one at a time (no
  concurrency).
- **Timeout**: each invocation is bounded by the matching rule's `timeout`
  (default `6s`). A timeout kills the invocation and is treated as an execution
  failure. Multiple matching rules each use their own rule's timeout.
- **Failure**: any non-zero container exit is a failure; errors include the
  function and handler names. **Abort on first failure**: a failed invocation
  stops the remaining rules for that event and returns the message to the
  pending entries list (no XACK). See _Acknowledgment semantics_ below.

For example, `functions/user-events-python/` declares three handlers
(`events.created.handler`, `events.updated.handler`,
`events.deleted.handler`), all served by the same function image.

## Hot reload

Relay watches `/functions` (with `fsnotify`) and reconciles each function on the
fly, without a restart:

- **Auto-discovery**: a new directory under `/functions` is detected and its
  image built, then it starts matching events.
- **Per-function rebuild on change**: edits to a function's template, source, or
  dependency files trigger a rebuild of *that function's* image only. Events are
  debounced (750ms) so a burst of editor saves coalesces into one rebuild.
- **Fingerprinting**: each function's content is hashed (`SHA-256` over file
  paths + bytes); an unchanged function is skipped, so a rebuild happens only
  when its inputs actually changed.
- **Failure safety**: if a rebuild fails (invalid template or failed image
  build), the previous, still-working version is retained and keeps serving
  events. It is retried on the next change or periodic pass.
- **Removal**: deleting a function's directory removes it from matching.
  In-flight invocations are never interrupted; they finish against the snapshot
  they started with.
- **Missing template**: a directory present but with no `template.yaml` yet is
  treated as "not ready" — Relay waits for more events rather than dropping a
  previously-active function.
- **Periodic fallback**: a 30s reconciliation pass re-scans `/functions` as a
  backstop for watch events that were missed.
- **Nested directories**: the watcher covers files in nested subdirectories of a
  function, so sources split into packages are tracked too.

Functions are read-only to Relay (the directory is mounted read-only in the
container); all rebuilds happen in temporary build contexts, so Relay never
writes into `/functions`.

## Acknowledgment semantics

A message is acknowledged (XACK) only after **all** matching invocations
succeed:

```
event → handler A ✓ → handler B ✓ → ... → XACK
```

If any invocation fails (or times out), the message is **not** acknowledged and
remains pending for redelivery (at-least-once semantics):

```
event → handler A ✗ → STOP → no XACK (message stays pending)
```

An event that matches no rules is a success and is acknowledged. Because
delivery is at-least-once, handlers should tolerate duplicate delivery.

### Recovery and retries

Relay consumes with a consumer group, so every delivered message records an
entry in the group's Pending Entries List (PEL) until it is acknowledged. A
message a consumer reads but never acknowledges — a crash, an outage, or a
handler failure — stays in the PEL.

- **Recovery loop** (`XAUTOCLAIM`): a background goroutine runs every
  `DefaultReclaimInterval` (1m) and reclaims messages that have sat pending for
  longer than `DefaultMinPendingIdle` (1m). Reclaiming takes ownership for the
  current consumer and replays the message through the same processing path as a
  fresh read. This makes Relay survive restarts: a message left pending by a
  dead consumer is picked up and retried by a live one.
- **Retry counting**: the per-message delivery count is read from Redis
  (`XPENDING` full form / retry counter), not kept in process memory, so the
  count survives restarts. Each reclaim of an idle message increments the count.
- **Max attempts → DLQ**: once a message fails `DefaultMaxAttempts` (5) times,
  it is no longer re-processed. It is written to the dead-letter stream
  `<stream>:dlq` and the original is then acknowledged, removing it from the
  PEL.
- **DLQ entry format** (flat fields): `original_stream`, `original_id`,
  `group`, `consumer`, `event` (the original payload string), `reason`,
  `attempts`, `timestamp` (RFC 3339).
- **DLQ write ordering**: the DLQ is written _before_ the original is
  acknowledged. If the DLQ write fails, the original is left pending so the next
  recovery cycle retries the DLQ write instead of losing the message.
- **Non-retryable failures**: a message whose `event` field is missing, is not a
  string, or is not a JSON object can never succeed. It is routed straight to
  the DLQ on first encounter — without running any handler — and acknowledged.
- **Malformed input** never consumes retry cycles.

These recovery defaults are a fixed part of the stream package and cannot be
overridden by environment variables. A zero-valued `ConsumerConfig` field
falls back to them in `NewConsumer`.

The at-least-once contract from the ACK table above is unchanged: XACK happens
only after all matching invocations succeed or the message is successfully
routed to the DLQ. Because redelivery is now a live mechanism (not a stranding
hole), a partial failure that is redelivered re-runs _every_ matching handler —
including ones that already succeeded — so handlers must be idempotent.

## Example functions

The root-level `functions/` directory contains development / example functions
to copy and adapt. Each direct subdirectory is one function, deployed as
described above.

- `functions/user-events-python/` (python3.14): three rules on the `users`
  table, each mapping an event to a module inside the `events/` namespace
  package (no `__init__.py`):
  - `events.created.handler` on `event_name: INSERT`.
  - `events.updated.handler` on `event_name: MODIFY` (with an explicit
    `timeout: 20s` demonstrating the per-rule timeout).
  - `events.deleted.handler` on `event_name: REMOVE`.

  Each module defines a single `handler(event)` function. `created`/`updated`
  read `event["new_image"]`; `deleted` reads `event["old_image"]`. Plain stdlib
  only, no `requirements.txt`.

- `functions/welcome-email-node/` (node24): a single rule
  `handler.handler` on `event_name: INSERT` / `table_name: users`. The handler
  reads `event.new_image` and logs a welcome email. No `package.json` is
  provided, so Relay injects the ESM `package.json`. It omits `timeout`, so it
  exercises the `6s` default.

A single generic, cross-engine event matches both functions:

```json
{
  "event_id": "evt_123",
  "event_name": "INSERT",
  "table_name": "users",
  "new_image": {
    "id": "user_123",
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

It matches `user-events-python` → `events.created.handler` (Python, prints the
user id) **and** `welcome-email-node` → `handler.handler` (Node, logs the
welcome email).

## Try it

With Relay running against the defaults (stream `events`, group `relay`), add
the example event with `redis-cli`:

```
redis-cli XADD events '*' event '{"event_id":"evt_123","event_name":"INSERT","table_name":"users","new_image":{"id":"user_123","name":"John Doe","email":"john@example.com"}}'
```

Relay then logs the matched rules and the forwarded handler output:

```
relay: function "user-events-python" rule "events.created.handler" matched event "<msg-id>"
relay: function "user-events-python" handler "events.created.handler": User created: user_123
relay: function "user-events-python" handler "events.created.handler" executed for event "<msg-id>"
relay: function "welcome-email-node" rule "handler.handler" matched event "<msg-id>"
relay: function "welcome-email-node" handler "handler.handler": Sending welcome email to john@example.com
relay: function "welcome-email-node" handler "handler.handler" executed for event "<msg-id>"
```

## Out of scope

Custom images/Dockerfiles, other runtimes, pyproject/uv/poetry/pnpm/yarn/bun,
concurrency, warm containers, build caching, source hashing, git,
registries, k8s, retry *policies per rule* (delays/attempt counts — only a global
max-attempts is implemented), idempotency, exactly-once, per-function
env/secrets/resource limits/networking, HTTP API, UI, metrics, tracing, and
additional operators (numeric/exists/anything-but/regex/glob/scripts) are not
implemented in this iteration.
