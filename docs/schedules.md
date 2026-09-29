# Schedules

A `schedules` list in `template.yaml` makes a function's handler run on a cron
schedule. Scheduled invocations reuse the entire event execution path: the same
runtime image lifecycle, secrets, timeout cap, concurrency slots, retry/DLQ
machinery, and handler metrics.

```
gocron (every worker) → atomic publish-if-new → Redis Stream
→ existing consumer group → one worker → runner → container
```

A template must still declare at least one event or service; `schedules` alone
is not a valid template (see [functions.md](functions.md)).

## Cron expressions

Each entry requires `handler` and `cron`:

```yaml
schedules:
  - handler: jobs.cleanup.handler
    cron: "0 3 * * *" # minute hour day-of-month month day-of-week
    timezone: Europe/Rome # optional IANA timezone, default UTC
    timeout: 20s # optional, same rules as event rules
    retries: 2 # optional, same rules as event rules
```

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

An invalid cron expression or timezone fails template validation (the function
is logged and skipped). An embedded `TZ=`/`CRON_TZ=` prefix in the expression is
rejected — use the `timezone` field.

`relay function inspect` shows the raw expression plus a best-effort
human-readable description in 24-hour time (display only).

## Timezone and identity

`timezone` is an IANA location resolved with Go's `time.LoadLocation`
(`Europe/Rome`, `America/Sao_Paulo`, …); omitted means UTC. The scheduler runs in
UTC and encodes the zone in the expression, so DST and offset changes are
handled by Go and the cron parser. `Local` is rejected.

An occurrence's identity is deterministic —
`schedule:<function>:<handler>:<scheduled_at RFC3339 UTC>` — derived from the
absolute instant normalized to **UTC**, never from a timezone representation.
The configured timezone therefore affects **when** a schedule fires, never the
identity, so DST and offset changes cannot split or merge occurrences.

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
- Once the stream entry exists it is an ordinary Relay message: the consumer
  group delivers it to one worker, and PEL / `XAUTOCLAIM` recovery, retries,
  exhaustion, and DLQ apply exactly as for an event.
- A duplicate publication is a successful no-op.

The scheduled handler receives a deterministic payload on stdin (same contract
as events):

```json
{ "source": "relay.schedule", "scheduled_at": "2026-09-29T03:00:00Z" }
```

## Publication recovery (bounded)

A tick is not a single best-effort publish:

- A failed publish retries the **same logical occurrence** (the ID is computed
  once and never recomputed) with a bounded exponential backoff — 100ms, 500ms,
  2s, 5s (five attempts total) — that observes the worker lifecycle, so shutdown
  aborts promptly. A success or a clean duplicate ends the loop; a duplicate is
  never retried.
- On startup, before jobs begin, each worker performs a bounded **catch-up**:
  for each schedule it republishes the latest missed occurrence within a
  **24-hour horizon**, using the same bounded retry routine. Only the latest
  occurrence per schedule is recovered — older misses are intentionally dropped
  (bounded recovery, not unbounded backlog replay) — and future occurrences are
  never synthesized. A catch-up another worker already published is a harmless
  duplicate.

## Live changes

Adding, changing, or removing schedules (or a schedule's handler/cron/timezone/
timeout) converges live through the reconciler: the worker's cron jobs are
replaced in place, so **future** occurrences use the current definition. Already
published occurrences are not purged from Redis; they expire via the dedup TTL
and stream retention.

An occurrence still pending when its function or schedule handler is removed is
treated as **obsolete**: it is acknowledged (terminal) rather than retried
forever or dead-lettered, because its removal was an intentional configuration
change.

## The guarantee

> One schedule occurrence is **published once cluster-wide**, while handler
> execution remains **at-least-once** — exactly-once handler execution is not
> claimed. A crash between a handler's side effect and its completion re-runs the
> handler, so scheduled handlers must stay idempotent.

Publication recovery is bounded per worker (retry budget, then the 24h catch-up
for the latest miss per schedule). Beyond that, older misses are dropped, and the
fleet-level single-publication guarantee always rests on the atomic
publish-if-new: a retry or catch-up that finds the key already present is a clean
duplicate.

Schedule occurrences bypass event matching, so they do **not** advance the
event-classification counters (`events_received_total`, `events_matched_total`,
`events_unmatched_total`, `function_events_matched_total`). They have their own
coordination counters (see [operations.md](operations.md)).
