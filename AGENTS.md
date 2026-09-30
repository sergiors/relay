# AGENTS.md

Context file for coding agents. It describes the current architecture, not its
history. Update it only when an architectural boundary or an invariant changes;
ordinary changes (a handler, a default, a test) do not warrant an update. Prefer
the code, `README.md`, and `internal/*/doc.go` when this file disagrees.

Relay is an event-driven function runner: Go 1.27, module `relay`, one binary
built from `./cmd`, GPL-3.0.

## Purpose and data flow

Relay consumes events from a Redis Stream with a consumer group, matches each
event against declarative patterns in each function's `template.yaml`, executes
matching handlers in isolated Docker containers, and acknowledges a message only
after every matching invocation is terminal (succeeded, or exhausted to the DLQ).
`relay start` runs the long-running runtime in the foreground; other subcommands
are admin-only.

```
Producer -> Redis Stream -> Relay (match) -> runner -> container -> XACK
gocron (every worker) -> atomic publish-if-new -> same stream -> one worker
```

## Package map (`internal/`)

- `cli` command tree (urfave/cli/v3), parsing/help only, errors returned;
  `config` resolves env into one `Config`; `worker` the runtime lifecycle
  (external-dependency preflight, then resources) and Unix control socket.
- `function` pure decision layer (discovery, validation, matching,
  fingerprinting) with `source` the shared `.gitignore` policy; `runtime` Docker
  client, image build/GC, warm pool, engines (`runtime/python`, `runtime/node`).
- `runner` match -> attempt/retry/exhaustion -> outcome aggregate; `stream`
  consumption, recovery, per-invocation state, DLQ.
- `schedule` identity/publish; `cron` timing/catch-up; `reconciler` live reload
  and services; `routing` Traefik; `secrets`, `state`, `processlock`,
  `git`/`git/webhook`, `observability/*`, `testutil`.

## Ownership and invariants

- `/functions` is the source of truth; the SQLite state DB is a view that never
  drives matching, building, or reconciliation.
- Relay touches only its own namespaces (`relay-fn-*` / `relay-dep-*` images,
  `relay.`-labelled containers and keys); ownership is the strict `relay.type`
  label, never a name, with no global pruning.
- Matching includes currently-unavailable functions: the event is still
  `matched`, its invocation stays pending, and it is never DLQ'd for
  unavailability alone. Effective concurrency is
  `min(template concurrency, MAX_CONCURRENCY)`, bounding the runner semaphore and
  the warm pool.
- Delivery is at-least-once, never exactly-once: handlers must be idempotent.
- State, metrics, and cleanup failures are logged, never fatal. Redis/transport
  errors leave messages pending (fail open); a claim error (`TryStart`) fails
  closed so a duplicate cannot race a winning replica.

## Template concepts

- `runtime` is `python3.14` or `node24`; required for events/schedules and
  `entrypoint` services, optional for image-only service templates.
- `events[]` `handler`/`pattern`/optional `timeout`/`retries`; `schedules[]`
  mandatory unique `name` + `handler` + minute-granularity `cron` + optional
  `timezone`/`timeout`; `services[]` mandatory unique `name` + exactly one of
  `entrypoint` or `image`, optional `port`/`replicas`/`host`/`path` (path
  requires host). Event identity is the handler; service/schedule identity is the
  name.
- `concurrency`, `resources` (memory/CPU/PIDs), `env`, `secrets` (references
  only; a name may not be in both; `RELAY_HANDLER` reserved); operators
  `equals`, `prefix`, `suffix`, `exists`, `gt`/`gte`/`lt`/`lte`. Names
  `[a-z0-9][a-z0-9._-]*`, <=63 chars, no trailing `.`.

## Runtime identity and generation

- One image per function, versioned by source fingerprint (selected source plus
  applicable `.gitignore` files; `template.yaml` verbatim; `resources` excluded).
  A rebuild is a new immutable image; the old version keeps serving until the new
  one is prepared and swapped in; resource-only edits instead reuse the image and
  rotate containers.
- Dependency layers are content-addressed, shared, and never auto-pruned; Relay
  removes only its own images, and only once unreferenced. A container's version
  is its resolved image content, not its tag: a version change drains the old
  generation (idle containers discarded, busy ones finish then discarded) with no
  new invocation leased to it.
- Warm containers are created lazily up to effective concurrency and evicted on
  idle timeout; execution containers are hardened (non-root, dropped caps,
  read-only rootfs, per-container limits) with Docker AutoRemove.

## Stream, retry, DLQ, claims

- Consumer group created with `MKSTREAM` at position `0`; consumer name is the
  hostname. Per-invocation state is a Redis hash keyed by message and
  `<function>/<handler>`, with forms complete, running-until-deadline,
  next-attempt-until-deadline, exhausted, and exhausted-and-DLQ-persisted. While
  the message is recoverable (pending in the PEL) the hash is persistent with no
  TTL; only after it leaves the PEL (a successful XACK, or a cleared
  missing-payload reference) is it switched to terminal retention (a reserved
  terminal marker plus a ~7-day TTL), after which every lifecycle transition is
  inert and only expiry removes it.
- Deadlines are integer Unix milliseconds; a marker is protected exactly while
  `now_ms < deadline_ms`. Every transition is one atomic Lua script, and
  active-claim transitions CAS both attempt and claim token, so a stale claim can
  never overwrite a newer claim or a terminal marker.
- Retry backoff is fixed (1m/2m/5m/10m capped); the attempt count is the real
  execution count, distinct from the diagnostic PEL delivery count. Recovery
  reclaims idle pending messages (XAUTOCLAIM) as a message-level backstop, not
  the retry timer; whether a handler runs is decided per invocation from state.
- A message is acked only after all matched invocations are terminal. Exhaustion
  writes one DLQ entry per exhausted invocation to the Relay-owned
  `relay:<stream>:dlq` before the XACK, and the persisted DLQ marker makes the
  write idempotent on redelivery; malformed/unmatched messages are acked.
- A reclaimed entry whose stream body is gone is data loss, not success: counted
  and its dangling PEL reference cleared, never a fabricated payload or DLQ entry.
- Backpressure is per worker (bounded local read buffer plus bounded global
  concurrency); a blocked slot leaves the message pending without charging a retry.

## Schedules: identity vs execution

- Every worker evaluates schedules locally, but publication is deduplicated
  atomically, so exactly one stream entry exists per logical occurrence.
- Occurrence identity derives from function, schedule NAME, and the absolute
  scheduled instant normalized to UTC; timezone affects when a schedule fires,
  never the identity, so DST cannot split or merge occurrences. The handler is not
  part of the identity. Dedup keys expire by TTL.
- Schedule names are mandatory and unique per function; multiple schedules may
  share a handler. Cron jobs, occurrence identity, and runner config resolution
  are all keyed by the stable name, so editing a schedule under the same name
  replaces only that job and removing one name never obsoletes another sharing its
  handler.
- Only minute-granularity schedules are accepted; sub-minute and relative forms
  are rejected because their identity is not deterministic across workers.
- Publication failures retry the same occurrence with a bounded backoff; startup
  catch-up republishes only the latest missed occurrence within a bounded horizon.
- Once published it reuses the stream retry/claim/DLQ machinery, so handler
  execution stays at-least-once; a delivery resolves the schedule by NAME and runs
  its CURRENT handler, so a handler change is not obsolete. Removing a function or
  a schedule NAME makes its pending occurrences obsolete (acked, never retried or
  dead-lettered).

## Services and routing boundaries

- Both service source kinds share one reconciler and lifecycle; the desired
  source is resolved before any container action, so an unresolvable source
  preserves the existing healthy containers.
- Service names are mandatory and unique per function; the name is the identity
  (relay.service) that keys container grouping, routing ids, and the persisted
  snapshot. The SourceRef (relay.identity) is used only for image/entry
  resolution and desired-implementation comparison; two names may share one
  source, and a source change under the same name is the same service (in-place
  replacement), not a removal plus an addition.
- Replacement is start-before-stop per replica slot. A NAME change (rename) is a
  removal plus an addition: the old name's containers are stopped LAST, and only
  after the desired set fully converged — if any desired service failed
  (resolution, routing/network, create/start, non-running) they are preserved as
  the last usable generation. A pass superseded by a newer desired state never
  stops the old generation it was about to replace; it removes only its own
  provisional replacement and leaves convergence to the newer request.
- Containers are replaced when the source descriptor, image content, port,
  effective environment, resources, or routing labels change, otherwise
  preserved; external image freshness is checked at most hourly per source
  reference, in memory only.
- Persistent service containers receive their environment (including resolved
  secrets) at process start — the one documented place a secret value reaches the
  Docker daemon; invocation secrets never enter Docker config, labels, metrics,
  logs, traces, or state.
- Routing (Traefik) is optional, only for services declaring a `host`; the
  routing network is required, verified, and never created by Relay. `NETWORKS`
  is verified at startup and applies to execution containers and, as one
  order-independent set, to persistent service containers (a routed service adds
  `TRAEFIK_NETWORK`); it is startup configuration.

## Lifecycle and command hierarchy

- Startup runs an explicit external-dependency preflight BEFORE function
  loading/fingerprinting, `state.Open`, the runtime socket/services, sweeps and
  preparation, listener starts, background loops, the scheduler, and the
  reconciler. The fixed order is Redis stream/group readiness → Docker
  runtime-manager readiness → configured `NETWORKS` verification → the runtime
  manager's deferred warm-container maintenance loop is started; a failure at
  any step short-circuits every later phase, and a lifecycle cancellation during
  the preflight is a graceful shutdown, not an error. The manager is opened with
  deferred maintenance so no manager background loop runs while `NETWORKS` is
  still unverified; a verification failure closes the manager with no loop ever
  started.
- `relay start` runs in the foreground, holds a process lock, and never
  daemonizes, forks, or writes a PID file. Shutdown cancels the lifecycle first,
  then runs bounded, ordered steps; the process lock and Redis are released last.
- A worker-owned readiness flag (`internal/worker/readiness.go`) starts false,
  is set true only at the ready-to-consume boundary (after the preflight,
  function load/prepare, and socket/listener/loop and consumer/schedule/
  reconciler/scheduler wiring, immediately before `Consume`), and is cleared as
  the first instruction of the shutdown defer, before lifecycle cancellation.
  It is bound to the worker lifecycle context, so a lifecycle cancellation that
  precedes that clear also reports not-ready. `relay health` queries it over the
  existing control socket; a false flag is not-ready, and in steady state the
  query reflects live Redis consumer health plus a bounded Docker ping and
  `NETWORKS` verification. Per-function status, SQLite, tracing, and
  asynchronous service/housekeeping convergence do not gate readiness.
- Tree: `start`; `health`; `stats` (`reset`); `function` (`ls`,
  `inspect`, `invoke`); `dlq` (`ls`, `inspect`, `replay`, `rm`); `secret` (`ls`,
  `set`, `rm`); `git` (`keygen`, `set`, `sync`, `status`, `remove`). Grouping
  commands show help when bare and return a usage error on an unknown
  subcommand.
- `health` is worker health over the socket: it asks the RUNNING worker whether
  it is ready with its live dependencies healthy, and needs no Redis/Docker
  configuration or clients in the CLI process (with no running worker it fails).
  `stats`/`function ls|inspect` read SQLite only; `health`, `invoke`,
  `dlq replay`, and a running `stats reset` use the worker socket; `dlq` is the
  one command needing Redis. Persistent state is under `/var/lib/relay`,
  ephemeral lock/socket state under `/run/relay`; `/functions` is written only by
  `git sync`.

## Conventions

- Return wrapped errors (`%w`); print and set the exit code exactly once at the
  `cmd/main.go` boundary, and do not also log an error the caller will log.
- Thread `context.Context`; long operations are bounded by worker-lifecycle
  contexts, not caller deadlines, and cancellation must propagate.
- Inject dependencies (loggers, clients, builders, config) and keep interfaces
  narrow; a required logger is a constructor argument, never a nil fallback.
- Log with `slog`: fixed message plus structured attributes, never string
  concatenation, never high-cardinality IDs as labels. Never put secret values in
  logs, labels, metrics, traces, or state.
- Comments explain invariants and reasons, not obvious implementation or history.
- Prefer pragmatic, existing abstractions; extend the runtime registry and
  existing seams rather than adding frameworks or parallel implementations.
- Internal APIs and persisted internal state have no compatibility guarantee
  unless a documented public contract; correctness before optimization.

## Validation

`make fmt` (gofmt), `make vet`, `make test-unit` (`go test -short ./...`),
`make test-unit-race`, `make test-integration` (needs Docker + Redis,
`-tags=integration`), `make test-all`; CI runs vet, unit, unit+race, and the
integration suite with Redis and Node.

## High-risk invariants

- Never XACK before handlers succeed or required DLQ entries are persisted, and
  never ack a message with a protected or claimed invocation.
- Never weaken claim atomicity: CAS attempt+token, keep terminal markers
  monotonic, never downgrade a completed or exhausted marker.
- Never silently drop a pending/PEL reference; a missing payload is an anomaly.
- Never change schedule occurrence identity (function + schedule name + absolute
  instant) or add sub-minute granularity.
- Never identify a service or schedule by anything but its mandatory name (never
  a source descriptor, handler, or index); never make a name optional.
- Never move secret values outside their documented surfaces.
- Never widen image/container GC beyond Relay-owned labels; retire images only
  after service convergence and reference guards.

## References

- `README.md` (Configuration, Concurrency and backpressure, Image/Execution
  container lifecycle, Template format, Schedules, Services and Traefik routing,
  Supported runtimes, Handler contract, Hot reload, Local state database, Runtime
  paths, Secrets, Git, Observability, Acknowledgment semantics, Recovery and
  retries); `internal/*/doc.go` briefs; `Makefile`; `.github/workflows/tests.yml`.
- Not asserted on purpose: no build/version metadata is plumbed in, and OTel
  exporter health metrics and `OTEL_PROPAGATORS` are intentionally unsupported.
