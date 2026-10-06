# Schedules

A schedule is a **trigger**, not a workload: each firing creates a uniquely
identified **occurrence** and publishes it to the same Redis Stream and consumer
group used for external events. A schedule-specific dispatch path resolves the
configured **handler** by schedule name (or adopts the handler contract pinned at
admission) and bypasses ordinary event classification and pattern matching. The
handler is the execution target for a one-shot **function** invocation. That
function uses the common runtime, secrets, timeout cap, concurrency slots,
invocation-state, retry/recovery, and DLQ machinery. A persistent **service** is a
separate long-lived workload and does not run as an occurrence invocation.

```
gocron (every worker) → uniquely identified occurrence → atomic publish-if-new
→ Redis Stream → existing consumer group → schedule dispatch
→ resolve/adopt handler (bypass event pattern matching) → one-shot function
→ common runtime and invocation lifecycle
```

A template must still declare at least one event or service; `schedules` alone
is not a valid template (see [apps.md](apps.md)).

## Cron expressions

Each entry requires a stable `name`, a `handler`, and `cron`:

```yaml
schedules:
  - name: nightly-cleanup # stable identity (mandatory, unique per app)
    handler: jobs.cleanup.handler
    cron: "0 3 * * *" # minute hour day-of-month month day-of-week
    timezone: Europe/Rome # optional IANA timezone, default UTC
    timeout: 20s # optional, same rules as event rules
    retries: 2 # optional, same rules as event rules
```

`name` is the schedule's stable identity: it keys the cron job, the occurrence
identity, and the runner's configuration resolution. It follows the same
conservative rule as an app name (`[a-z0-9][a-z0-9._-]*`, ≤ 63 chars, no
trailing `.`) and must be unique among an app's schedules. **Multiple
schedules may share a handler** (the same job at different times); each name is
an independently addressable schedule. Editing a schedule's cron, handler,
timezone, timeout, or retries under the same `name` replaces only that job.

Accepted forms, at **minute granularity**:

- the standard 5-field expression
  `minute hour day-of-month month day-of-week`;
- the calendar descriptors `@hourly`, `@daily`/`@midnight`, `@weekly`,
  `@monthly`, `@yearly`/`@annually`.

Rejected by design:

- **6-field (seconds) expressions.** gocron's callback exposes no scheduled-due
  instant, so a per-second occurrence cannot be identified deterministically
  across workers and cluster-wide dedup would break.
- **`@every <duration>`.** It is a relative delay anchored to each worker's own
  job start, so workers do not agree on an occurrence.

An invalid cron expression or timezone fails template validation (the app
is logged and skipped). An embedded `TZ=`/`CRON_TZ=` prefix in the expression is
rejected — use the `timezone` field.

`relay app inspect` shows the raw expression plus a best-effort
human-readable description in 24-hour time (display only).

## Timezone and identity

`timezone` is an IANA location resolved with Go's `time.LoadLocation`
(`Europe/Rome`, `America/Sao_Paulo`, …); omitted means UTC. The scheduler runs in
UTC and encodes the zone in the expression, so DST and offset changes are
handled by Go and the cron parser. `Local` is rejected.

An occurrence's identity is deterministic —
`schedule:<app>:<schedule name>:<scheduled_at RFC3339 UTC>` — derived from
the absolute instant normalized to **UTC**, never from a timezone representation.
The configured timezone therefore affects **when** a schedule fires, never the
identity, so DST and offset changes cannot split or merge occurrences. The
handler is not part of the identity: a handler change under the same schedule
name keeps the same occurrence id, and the next **not-yet-admitted** delivery
runs the name's current handler (occurrences are at-least-once; see
[Live changes](#live-changes) for the admission boundary that freezes an
admitted occurrence's handler/timeout/retries).

The due instant is derived from the schedule as the latest occurrence at or
before the callback time (using the same parser semantics used for
registration), not by truncating the callback's wall clock. A delayed 03:00
callback observed at 03:01 still stamps 03:00, so on-time callbacks, delayed
callbacks, and startup catch-up all agree on one occurrence ID.

## Distributed publication

Every worker evaluates its schedules locally, but a worker does not execute the
handler itself. When gocron determines an occurrence is due, the worker attempts
an atomic **publish-if-new** into the same Relay event stream: a single Lua
script checks a dedup key and writes the stream entry in one step, so exactly one
worker wins and publishes one stream entry per logical occurrence. Every other
worker's simultaneous evaluation of the same tick is a clean no-op.

- Dedup keys live under `relay:schedule:<occurrence_id>` with a **7-day TTL**.
  They are history only and are never deleted on completion, so a worker whose
  callback runs later cannot re-publish an occurrence the fleet already
  completed. Dedup applies to **publication**, not to handler execution.
- Once the occurrence entry exists, the consumer group delivers it to one worker.
  It takes the schedule-specific dispatch path, not ordinary event
  classification/pattern matching; PEL / `XAUTOCLAIM` recovery, invocation state,
  retries, exhaustion, and DLQ are shared with external event work.
- A duplicate publication is a successful no-op.

Because a failed publish is persisted durably (see
[Publication recovery](#publication-recovery)) and retried with the same
occurrence identity, an ambiguous `XADD` (the call errored but the entry may have
been admitted) is eventually resolved as a clean duplicate rather than
republished or lost.

The scheduled handler receives the occurrence's deterministic payload on stdin.
It is still a one-shot function invocation, but the occurrence is dispatched
directly to its configured handler rather than matched against event patterns:

```json
{ "source": "relay.schedule", "scheduled_at": "2026-09-29T03:00:00Z" }
```

## Publication recovery

A tick is not a single best-effort publish. Schedule publication **requires a
usable durable store**: before a worker may fire a schedule it must have opened
the local state database, because a live tick has to be able to persist its
occurrence if Redis rejects the publish. The durable store is the scheduler's
local SQLite outbox — the same state database Relay already uses — and, when it
cannot be opened, the worker marks the scheduler **unavailable** (see
[Storage availability](#storage-availability)) rather than firing schedules it
could not recover.

A tick is also not a single best-effort publish:

- A failed publish retries the **same logical occurrence** (the ID is computed
  once and never recomputed) with a bounded exponential backoff — 100ms, 500ms,
  2s, 5s (five attempts total) — that observes the worker lifecycle, so shutdown
  aborts promptly. A success or a clean duplicate ends the loop; a duplicate is
  never retried.
- **The failure is durable.** On the FIRST failed attempt the complete immutable
  occurrence intent (its derived id plus app/schedule/handler/scheduled_at) is
  written to the local state database. If the bounded in-memory retries then
  recover it, the row is deleted only once a publication call resolves with a
  nil error. A healthy first-attempt success — or a clean duplicate — never
  touches the database.
- A background **durable retry worker** reclaims persisted occurrences and keeps
  republishing each one **indefinitely** (a capped backoff of 5s, 15s, 30s, 2m,
  5m, 10m) until it resolves. A record is claimed with a per-row database lease,
  so concurrent retriers in the same process cannot both work it; a record
  whose lease expires while unresolved is reclaimed automatically. Because the
  occurrence identity is unchanged, a durable retry that finds the occurrence
  already published is a clean duplicate and resolves the row. This survives
  restarts: on startup the worker scans the outbox and retries immediately.
- A row is deleted only after a publication call resolved (published or a clean
  duplicate). If the delete itself fails the row is retained and retried
  idempotently. An ambiguous `XADD` (the call errored but the entry may exist)
  is handled by retrying: the retry is a clean duplicate that deletes the row.
- A persisted record is republished only after its decoded fields are verified
  to reconstruct exactly the occurrence ID stored in the row key (the same
  derivation the publisher and consumer use). A row whose payload cannot be
  decoded, or whose decoded intent would name a different occurrence, is never
  published and never deleted: it is logged and rescheduled under the same
  bounded backoff so the durable row is retained and repairable.
- **A durable-store failure pauses publication.** If the persist step above
  fails (the outbox is wired but the write errors), the worker does not fall
  back to more in-memory retries: the occurrence is explicitly logged as
  **unresolved** (it is not claimed to be durably recoverable), the scheduler
  transitions to **degraded**, and live schedule publication stops until the
  durable store answers again. The same degraded transition is raised by any
  outbox operation failure in the retry worker (claim, reschedule, delete, or
  next-due query). Rows already persisted remain recoverable.
- On recovery the durable retry worker drains existing rows, re-runs the same
  bounded **latest-only 24h catch-up** to cover schedule time that passed while
  paused, and only then re-enables live ticks.
- On startup, before jobs begin, each worker also performs a bounded
  **catch-up**: for each schedule it republishes the latest missed occurrence
  within a **24-hour horizon**, using the same bounded retry routine. Only the
  latest occurrence per schedule is recovered — older misses are intentionally
  dropped (bounded recovery, not unbounded backlog replay) — and future
  occurrences are never synthesized. A catch-up another worker already published
  is a harmless duplicate. The durable outbox is a separate, unbounded retry of
  occurrences that were **actually attempted**; catch-up remains latest-only.

The local state database therefore records a small, bounded outbox of
unresolved publications — not execution history and never an execution source.
`/apps` stays authoritative, and the row disappears as soon as publication
resolves.

## Storage availability

Schedule firing depends on the local state database, but that dependency is
**scoped to the scheduler**: it is required for correct schedule publication and
is deliberately not a global worker dependency. Consumers, functions, services,
metrics, logging, and tracing stay fully operational when the store is
unavailable, and worker readiness (`relay health`) deliberately excludes it.

- **Startup.** If the state database cannot be opened at startup, the worker
  does **not** fail, does not start gocron, and marks the scheduler
  **unavailable** (logged and exposed as a one-hot `scheduler_state` gauge). A
  scheduler-owned, cancellable bootstrap then retries opening the store on a
  fixed cadence. Once it opens, the worker initializes the outbox, recovers
  existing rows, runs the bounded latest-only catch-up, and enables live jobs.
  The recovered handle is owned by the scheduler and closed only after all
  scheduler work has stopped — the worker's shared state handle is never
  disturbed.
- **Runtime.** If an outbox operation fails while running, the scheduler
  transitions to **degraded**: callbacks arriving while degraded return before
  computing or publishing an occurrence, so no attempt is made against a store
  that cannot record it. The durable retry worker keeps probing the store,
  backing off on error, and detects recovery; it never terminates the worker.
- **Metrics.** `scheduler_state{state=running|degraded|unavailable}` is a
  one-hot gauge, and `scheduler_degraded_total` /
  `scheduler_recoveries_total` count entries into degraded and recoveries back
  to running. `/apps` remains the source of schedule configuration: SQLite is
  only the scheduler's durable publication-coordination store.

## Live changes

Adding, changing, or removing schedules converges live through the reconciler:
the worker's cron jobs are replaced in place under their stable names, so
**future** occurrences use the current definition. Editing one schedule (its
handler, cron, timezone, timeout, or retries) replaces only that name's job, even
when another schedule shares its handler; removing one name never obsoletes
another. Already published occurrences are not purged from Redis; they expire via
the dedup TTL and stream retention.

An occurrence still pending when its app or schedule is removed is treated as
**obsolete** if it has not yet been admitted: it is acknowledged as terminal
rather than retried forever or dead-lettered, because the referenced schedule
configuration was intentionally removed.

A schedule is resolved by its stable **name**, and its execution contract is
frozen at the occurrence's **first successful admission**:

- **Before admission** (the message has never been claimed), every delivery
  resolves the **current** template by name. A handler change under the same
  name is **not** obsolete — the next delivery uses the schedule's current
  handler, timeout, and retry configuration. If the schedule name no longer
  exists and the occurrence has never been admitted, the occurrence is
  obsolete and is acknowledged.
- **At admission**, the first claim atomically pins an immutable execution
  descriptor — schedule name, admitted handler, capped timeout, and retry
  budget — alongside the per-invocation state. Concurrent replicas cannot
  independently admit different executions: exactly one descriptor wins, and
  subsequent deliveries adopt it.
- **After admission**, the pinned descriptor is authoritative for that
  invocation. Later changes to the handler, timeout, or retry budget — or
  removal of the schedule name entirely — do not reset the claim or attempts
  and do not cancel the invocation. It completes its retry, backoff, ACK, or
  DLQ lifecycle under the contract with which it was admitted.

The handler is not part of the schedule occurrence identity. Occurrences are
identified by the schedule resource, while the one-shot function executes the
`app/handler` target that was actually admitted. The pinned descriptor preserves
the admitted schedule context and execution contract; shared per-invocation state
and DLQ attribution remain keyed by that `app/handler`.

### Occurrence validation

A schedule message is a **claim of schedule identity**: the envelope names the
app, the schedule, the handler, the scheduled instant, and the derived
occurrence id. Relay validates that claim structurally and semantically before
anything executes. This is integrity, not provenance — there is no signature or
key; the validation only establishes that a claim is well-formed and
consistent, never who sent it.

- **Structural.** A message whose `source` is `relay.schedule` must carry a
  non-empty app, schedule, handler, and occurrence id, and an RFC3339
  `scheduled_at` that is whole-second. Cron evaluation is whole-second
  granularity, so a timestamp carrying a fractional second (e.g.
  `12:34:00.500Z`) is malformed even though its derived id truncates to
  `12:34:00Z`: it is rejected rather than accepted as the second it falls in. A
  nonzero whole second is **not** structurally malformed: historical IANA
  offsets can themselves carry a seconds component, so a legitimate local-minute
  firing may have a nonzero UTC second; whether it actually fires is decided
  semantically (see Firing). A message that carries the marker but fails the
  structural check is **invalid**: it is deliberately **not** treated as an
  ordinary event (so it can never be pattern-matched and ACKed as unmatched) and
  it is **dead-lettered** as a non-retryable failure. A message without the
  marker is an ordinary external event and is untouched.
- **Identity.** `occurrence_id` must equal the id derived from
  `app`/schedule/`scheduled_at`; identity is derived and cross-checked, never
  trusted from the wire. A mismatch is invalid and dead-lettered.
- **Firing.** For a schedule whose name still exists, `scheduled_at` must be a
  real firing of the schedule's **current** cron in its effective timezone
  (including DST), computed with the same parser gocron uses. A timestamp that
  is not a firing is invalid and dead-lettered. A schedule whose own cron no
  longer parses is surfaced the same way.
- **Target.** The envelope handler never selects the execution target: only the
  app and the stable schedule **name** do, and the current template's handler is
  what runs (or the handler pinned at admission). This is why a handler change
  under the same name is a valid live change, not an invalid claim.

Dispositions stay distinct:

- **Obsolete** (app or schedule NAME removed, never admitted): acknowledged as
  terminal, never retried and never dead-lettered.
- **Unavailable** (app configured but not runnable): retryable, left pending.
- **Invalid** (the name exists but the claim is not a real firing, or its cron
  is unparseable): non-retryable, dead-lettered.
- **Handler failure**: the ordinary per-invocation retry/exhaustion lifecycle.

## The guarantee

> One schedule occurrence is **published once cluster-wide**, while handler
> execution remains **at-least-once** — exactly-once handler execution is not
> claimed. A crash between a handler's side effect and its completion re-runs the
> handler, so scheduled handlers must stay idempotent.

Publication recovery has two layers: the bounded in-memory retry (per tick), and
a durable outbox that keeps retrying an occurrence that was actually attempted
but unresolved, indefinitely and across restarts, until publication resolves. The
24h catch-up remains the bounded, latest-only recovery for occurrences the
worker never got to attempt (a miss while it was down); older misses are dropped.
The fleet-level single-publication guarantee always rests on the atomic
publish-if-new: any retry, durable retry, or catch-up that finds the key already
present is a clean duplicate.

The durable outbox is required for scheduler correctness: while it is
unavailable the scheduler does not fire, but that affects **only** schedule
publication. External event consumption, functions, services, metrics, logging,
tracing, and worker readiness are unaffected (see
[Storage availability](#storage-availability)). An outbox operation failure at
runtime pauses publication (degraded); a recovery drains pending rows and re-runs
the same bounded latest-only catch-up before live ticks resume.

Schedule occurrences bypass event matching, so they do **not** advance the
event-classification counters (`events_received_total`, `events_matched_total`,
`events_unmatched_total`, `app_events_matched_total`). They have their own
coordination counters (see [operations.md](operations.md)).
