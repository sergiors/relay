# AGENTS.md

Context file for coding agents. It describes the current architecture, not its
history. Update it only when an architectural boundary or an invariant changes;
ordinary changes (a handler, a default, a test) do not warrant an update. Prefer
the code, `README.md`, and `internal/*/doc.go` when this file disagrees.

Relay is an event-driven app runner: Go 1.27, module `relay`, one binary
built from `./cmd`, GPL-3.0.

## Purpose and data flow

Relay consumes events from a Redis Stream with a consumer group, matches each
event against declarative patterns in each app's `template.yaml`, executes
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
- `app` pure decision layer (discovery, validation, declarative model,
  fingerprinting) with `source` the shared `.gitignore` policy; `event`
  event-pattern matching and the candidate index (consumes `app` types,
  never the reverse); `runtime` Docker client, image build/GC, warm pool,
  engines (`runtime/python`, `runtime/node`).
- `runner` match -> attempt/retry/exhaustion -> outcome aggregate; `stream`
  consumption, recovery, per-invocation state, DLQ.
- `schedule` identity/publish; `cron` timing/catch-up and the durable
  publication-retry worker; `reconciler` live reload and services; `routing`
  Traefik; `secrets`, `state`, `processlock`, `git`/`git/webhook`,
  `observability/*`, `testutil`.

## Ownership and invariants

- `/apps` is the source of truth; the SQLite state DB is a view that never
  drives matching, building, or reconciliation.
- Relay touches only its own namespaces (`relay-app-*` / `relay-dep-*` images,
  `relay.`-labelled containers and keys); ownership is the strict `relay.type`
  label, never a name, with no global pruning.
- Matching includes currently-unavailable apps: the event is still
  `matched`, its invocation stays pending, and it is never DLQ'd for
  unavailability alone. Effective concurrency is
  `min(template concurrency, MAX_CONCURRENT_INVOCATIONS)`, bounding the runner
  semaphore and the warm pool. Independently, a per-worker preparation cap
  (`MAX_CONCURRENT_BUILDS`, default 2) bounds concurrent runtime-backed image
  preparations — source snapshot, dependency snapshot, and Docker builds — one
  immutable permit held for a preparation's whole duration; it is per worker, not
  a host-wide quota, and does not bound invocations or events. Independently
  again, a worker-global hard bound on warm execution containers
  (`MAX_WARM_CONTAINERS`, default 8) caps the aggregate pooled population across
  all apps (idle + busy, plus in-flight creates; persistent service containers
  and stale-version throwaways excluded). It is applied before an invocation
  claims a handler attempt: at the bound the globally oldest IDLE container is
  LRU-evicted; when every container is busy the invocation is left pending
  (backpressure) — never charged a retry, never DLQ'd — rather than evict a busy
  container or exceed the bound. A container's slot is returned only once its
  physical removal is confirmed: a teardown whose Docker remove genuinely fails
  leaves the container poisoned but keeps its slot reserved for the worker's
  lifetime, so the bound counts live containers rather than tracked ones and a
  fresh container is never admitted on phantom capacity.
- Matching also includes a valid generation being PREPARED but not yet runnable.
  The reconciler publishes the desired generation's event rules as a pending,
  non-runnable entry in the registry (atomically with its candidate index)
  BEFORE the source fingerprint walk and image build, so a delivery during
  preparation is matched-but-unavailable and stays pending instead of being
  ACKed as unmatched. A pending match never executes, claims `TryStart`,
  exhausts, ACKs, or DLQs; it is deduped by `<app>/<handler>` against the active
  generation so a rule active in v1 executes without being blocked by an
  identical pending v2 rule. The active generation is retained throughout
  (`Registry.Replace` installs the new active generation and clears the pending
  entry in one locked step); an invalid/removed desired definition, an unchanged
  no-build path, and an unavailable-placeholder reconcile all clear pending so no
  removed rule keeps gating events.
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
- A pattern field is an ordered OR list of alternatives: a bare literal
  (equality), a single-key operator map (`prefix`, `suffix`, `exists`,
  `gt`/`gte`/`lt`/`lte`), or a nested map (ANDed children). Operators take a
  single scalar operand; there is no `equals` and no scalar field shorthand.
- `concurrency`, `resources` (memory/CPU/PIDs), `env`, `secrets` (references
  only; a name may not be in both; `RELAY_HANDLER` reserved). Names
  `[a-z0-9][a-z0-9._-]*`, <=63 chars, no trailing `.`.

## Runtime identity and generation

- One image per app, versioned by source fingerprint (selected source plus
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
  `<app>/<handler>`, with forms complete, running-until-deadline,
  next-attempt-until-deadline, exhausted, and exhausted-and-DLQ-persisted. While
  the message is recoverable (pending in the PEL) the hash is persistent with no
  TTL; only after it leaves the PEL (a successful XACK, or a cleared
  missing-payload reference) is it switched to terminal retention (a reserved
  terminal marker plus the configured `REDIS_INVOCATION_RETENTION` TTL, default
  `48h`; empty/0/negative disables the expiry but still writes the marker, leaving
  the hash persistent), after which every lifecycle transition is
  inert and only expiry removes it.
- Deadlines are integer Unix milliseconds; a marker is protected exactly while
  `now_ms < deadline_ms`. Every transition is one atomic Lua script, and
  active-claim transitions CAS both attempt and claim token, so a stale claim can
  never overwrite a newer claim or a terminal marker.
- Retry backoff is fixed (1m/2m/5m/10m capped); the persisted attempt count is
  the number of admitted claims (normally the real execution count, but a crash
  after a confirmed claim and before execution spends one without running the
  handler — the claim alone never exhausts, so a later real failure is what
  DLQs), distinct from the diagnostic PEL delivery count. Recovery
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
- Occurrence identity derives from app, schedule NAME, and the absolute
  scheduled instant normalized to UTC; timezone affects when a schedule fires,
  never the identity, so DST cannot split or merge occurrences. The handler is not
  part of the identity. Dedup keys expire by TTL.
- Schedule names are mandatory and unique per app; multiple schedules may
  share a handler. Cron jobs, occurrence identity, and runner config resolution
  are all keyed by the stable name, so editing a schedule under the same name
  replaces only that job and removing one name never obsoletes another sharing its
  handler.
- Only minute-granularity schedules are accepted; sub-minute and relative forms
  are rejected because their identity is not deterministic across workers.
- Publication failures retry the same occurrence with a bounded backoff; if that
  in-memory budget is spent (or the tick is cancelled), the complete immutable
  occurrence intent is persisted to the local `state` SQLite outbox
  (`schedule_pending`) and a `cron`-owned durable retry worker republishes it,
  across restarts, until a publication call resolves nil (published or clean
  duplicate), when the row is deleted, OR until its **7-day retention** (from
  first insertion, `state.PendingRetention`) expires, when the row is removed
  without any publish attempt and counted as an expiration. The Redis occurrence
  dedup key lives **14 days** on the authoritative first publish and is never
  refreshed, so the original key still protects every retry the outbox can make.
  The outbox is coordination state only — never history, never an execution
  source — and is untouched on a healthy first-attempt success/duplicate. Rows are
  claimed with a per-row DB lease, not a global mutex, and never deleted until a
  call resolves or the row expires. A record is republished only after its decoded
  intent is verified to reconstruct the occurrence ID stored in its row key; an
  undecodable or identity-mismatched row is logged, retained, and rescheduled
  under the bounded backoff, never published or deleted. Startup catch-up stays
  the bounded, latest-only 24h recovery for never-attempted misses and is
  unchanged.
- The durable outbox is REQUIRED for scheduler correctness but is NOT a global
  Relay dependency. The scheduler evaluates/publishes an occurrence only while a
  usable outbox is installed; when `state.Open` fails at startup the scheduler is
  marked unavailable (never starts gocron; one-hot `scheduler_state` gauge +
  `scheduler_degraded_total`/`scheduler_recoveries_total`) and a scheduler-owned,
  cancellable, joined bootstrap retries opening the store. It opens a
  scheduler-owned handle (closed only after scheduler work stops; the shared
  global handle is never touched), recovers rows, re-runs the same bounded
  latest-only 24h catch-up, then enables live jobs. A runtime outbox failure
  pauses live publication (degraded) and is never substituted by in-memory
  retries; a publish+persist double failure is logged as unresolved, not durably
  recoverable. Schedule firing is not a prerequisite for consumers, functions,
  services, metrics/logging/tracing, readiness, or health.
- Once published it reuses the stream retry/claim/DLQ machinery, so handler
  execution stays at-least-once. The execution contract is frozen at the
  occurrence's FIRST successful admission: before admission a delivery resolves
  the schedule by NAME and runs its CURRENT handler (so a handler change under the
  same name is not obsolete); the first claim atomically pins an immutable
  descriptor (schedule name, admitted handler, capped timeout, retry budget) in
  the message's invocation-state hash under the reserved `__schedule` field, so
  concurrent replicas cannot both admit and any other delivery adopts the winner.
  After admission that descriptor is authoritative — a later template change, or
  the schedule NAME's removal, can no longer reset the claim/attempts or cancel
  the invocation, which completes its retry/DLQ lifecycle under the admitted
  contract. The handler is not part of occurrence identity; the descriptor is
  provenance only and DLQ attribution stays handler-based.
- Removing an app (or a schedule NAME that was never admitted) makes its
  pending occurrences obsolete (acked, never retried or dead-lettered).

## Services and routing boundaries

- Both service source kinds share one reconciler and lifecycle; the desired
  source is resolved before any container action, so an unresolvable source
  preserves the existing healthy containers.
- Service names are mandatory and unique per app; the name is the identity
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

- Startup runs an explicit external-dependency preflight BEFORE app
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
  then runs ordered teardown steps; the process lock and Redis are released last.
  An aggregate budget bounds the best-effort cleanup steps; quiescence barriers
  (scheduler, reconciler, housekeeping, services-join, loops) are logged on a
  missed bound and then strictly joined, so the budget bounds cleanup work, not
  process exit.
- A worker-owned readiness flag (`internal/worker/readiness.go`) starts false,
  is set true only at the ready-to-consume boundary (after the preflight,
  app load/prepare, and socket/listener/loop and consumer/schedule/
  reconciler/scheduler wiring, immediately before `Consume`), and is cleared as
  the first instruction of the shutdown defer, before lifecycle cancellation.
  It is bound to the worker lifecycle context, so a lifecycle cancellation that
  precedes that clear also reports not-ready. `relay health` queries it over the
  existing control socket; a false flag is not-ready, and in steady state the
  query reflects live Redis consumer health plus a bounded Docker ping and
  `NETWORKS` verification. Per-app status, SQLite, tracing, and
  asynchronous service/housekeeping convergence do not gate readiness.
- Tree: `start`; `health`; `stats` (`reset`); `app` (`ls`,
  `inspect`, `invoke`); `dlq` (`ls`, `inspect`, `replay`, `rm`); `secret` (`ls`,
  `set`, `rm`); `git` (`keygen`, `set`, `sync`, `status`, `remove`). Grouping
  commands show help when bare and return a usage error on an unknown
  subcommand.
- `health` is worker health over the socket: it asks the RUNNING worker whether
  it is ready with its live dependencies healthy, and needs no Redis/Docker
  configuration or clients in the CLI process (with no running worker it fails).
  `stats`/`app ls|inspect` read SQLite only; `health`, `invoke`,
  `dlq replay`, and a running `stats reset` use the worker socket; `dlq` is the
  one command needing Redis. Persistent state is under `/var/lib/relay`,
  ephemeral lock/socket state under `/run/relay`; `/apps` is written only by
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
- Never change schedule occurrence identity (app + schedule name + absolute
  instant) or add sub-minute granularity.
- Never identify a service or schedule by anything but its mandatory name (never
  a source descriptor, handler, or index); never make a name optional.
- Never move secret values outside their documented surfaces.
- Never widen image/container GC beyond Relay-owned labels; retire images only
  after service convergence and reference guards.
- Never close the runtime manager or state DB while a reconcile, service pass,
  or startup sweep may still use it, and never close Redis while a scheduler
  publisher callback or the durable publication-retry worker may still be
  running: their shutdown steps are strict-join barriers, so a bound expiry
  cancels the step and then waits for its real operation before teardown
  advances. A durable-retry attempt cut short by that cancellation leaves its
  outbox row recoverable; an outbox row is never deleted until a publication
  call resolves nil.

## References

- `README.md` (Configuration, Concurrency and backpressure, Image/Execution
  container lifecycle, Template format, Schedules, Services and Traefik routing,
  Supported runtimes, Handler contract, Hot reload, Local state database, Runtime
  paths, Secrets, Git, Observability, Acknowledgment semantics, Recovery and
  retries); `internal/*/doc.go` briefs; `Makefile`; `.github/workflows/tests.yml`.
- Not asserted on purpose: no build/version metadata is plumbed in, and OTel
  exporter health metrics and `OTEL_PROPAGATORS` are intentionally unsupported.
