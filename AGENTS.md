# AGENTS.md

Compact repository guide for coding agents. Prefer code, `README.md`, and package
docs when details here disagree. Update this file only when an architectural
boundary or invariant changes.

Relay is an event-driven app runner (Go 1.27, module `relay`; main package
`./cmd`).
Producers publish to Redis Streams; Relay matches events to app handlers and
runs them in isolated Docker containers. `relay start` owns the long-running
worker; admin commands communicate with it or read persistent state.

## Boundaries

- `cmd/main.go` is the sole final error/exit boundary. `internal/cli` builds and
  parses commands and returns errors; `internal/worker` owns lifecycle,
  readiness, control socket, and dependency wiring.
- `config` resolves configuration. `app` owns discovery, validation, model,
  and fingerprinting; source selection policy is separate in `source`. `event`
  matches patterns; `runner` dispatches and manages attempts; `stream` owns
  consumption, recovery, invocation state, ACK, and DLQ.
- `schedule` owns occurrence identity/publication; `cron` owns timing,
  catch-up, and durable publication retry. `reconciler` owns reload and service
  convergence. `runtime` owns Docker integration; language engines live in
  `runtime/python` and `runtime/node`. `state` owns PostgreSQL; `observability`
  owns telemetry.

## Invariants to preserve

- `/apps` is the source of truth. PostgreSQL is required at worker startup, but
  app state is only a view: it must not drive matching, builds, or reconciliation.
  Post-start state-write failures are non-fatal. Staged Docker build contexts
  live under private temp storage, not `/apps` or state storage.
- Delivery is at-least-once; handlers may run again and should be idempotent.
  ACK only after every matched invocation is terminal and required DLQ entries
  are persisted. Claim transitions are atomic and fenced: an ambiguous
  `TryStart` result must not execute. Rejecting work for capacity before claim
  must not spend an attempt.
- Unavailable apps and valid pending generations remain matched and pending;
  availability alone must not cause ACK or DLQ. Keep the old active generation
  serving until its replacement is prepared.
- Schedule identity is app + stable schedule name + absolute scheduled instant.
  Persist the exact intent to the PostgreSQL outbox before Redis publication;
  the execution descriptor is pinned at the first successful claim
  (`TryStartScheduled`). See `schedule`, `cron`, and `stream` package docs for
  lifecycle details.
- Docker cleanup is restricted to strict Relay ownership labels, never names or
  global pruning. Keep invocation secret values out of Docker config, labels,
  logs, metrics, traces, and state; service environment is the documented
  exception.
- `WORKER_ID` is required and is the worker's single stable identity: the Redis
  consumer name, the `relay.hostname` container-ownership label, and the
  persisted-stats owner key. It must be unique per concurrent worker and stable
  across restarts; there is no hostname or generated fallback. `InstanceID` is a
  fresh per-process fencing token, never configured.
- Invocation concurrency, runtime image build concurrency, and worker-global
  warm execution containers are separate limits. Do not conflate their scope or
  backpressure semantics; consult `README.md` and runtime/runner docs.
- Shutdown must join work that can use shared dependencies before closing those
  dependencies.

## Working conventions

- Return wrapped errors (`%w`); do not print or log an error that a caller will
  report. Thread contexts and propagate cancellation. Inject dependencies and
  keep interfaces narrow.
- Use `slog` with fixed messages and structured attributes. Never expose secret
  values or use high-cardinality IDs as metric labels. Comments should explain
  invariants, not narrate code.
- Prefer existing abstractions and seams over parallel frameworks. Internal APIs
  and persisted internal state have no compatibility promise unless documented.
- `relay apps` aliases `relay app ls`.

## Validation

- `make fmt`, `make vet`
- `make test-unit` (`go test -short ./...`), `make test-unit-race`
  (`go test -race ./...`)
- `make test-integration` (integration tag, 20m timeout),
  `make test-integration-race`, `make test-all` (unit + integration)
- Focus a test with `go test ./internal/<package> -run '^TestName$'`; add
  `-tags=integration` when needed.

Integration tests fail rather than skip when required PostgreSQL, Redis, or
Docker infrastructure is unavailable. CI provides PostgreSQL for unit and
integration tests, plus Redis and Node 24 for integration. Tests require only
the relevant services: Docker (client uses `FromEnv`), Redis (`REDIS_TEST_ADDR`,
default `localhost:6379`), and PostgreSQL (`TEST_DATABASE_URL`, default CI DSN).

For behavior and detailed contracts, consult `README.md`, `docs/*.md`, relevant
`internal/*/doc.go` files, `Makefile`, and `.github/workflows/tests.yml`. In
particular, see the `app`, `source`, `event`, `runner`, `stream`, `schedule`,
`cron`, `runtime`, `reconciler`, and `state` package docs rather than duplicating
their implementation details here.
