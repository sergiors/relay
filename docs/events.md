# Events

Relay consumes events from the configured Redis Stream with a consumer group.
Each entry carries an `event` field whose value is a JSON **object**; Relay
matches that object against every loaded function's event rules and runs the
matching handlers.

```
Producer → Redis Stream → Relay (match) → runner → container → XACK
```

A message that decodes successfully is classified `matched` or `unmatched`
**before any execution**; an unmatched message is acknowledged and never
retried. A malformed message (missing `event`, not a string, not a JSON object)
can never succeed and is routed straight to the DLQ on first encounter without
running a handler, then acknowledged.

## Publishing an event

Any producer can append to the stream. With `redis-cli` against the configured
stream:

```sh
redis-cli XADD events '*' event '{"event_name":"INSERT","table_name":"users"}'
```

Relay never assumes where events originate; it only reads the stream.

Events may also be created by `relay function invoke` (manual, synchronous, not
part of the stream lifecycle — see [cli.md](cli.md)) and by Relay's own schedule
publication (see [schedules.md](schedules.md)).

## Pattern syntax

Rules live under `events` in `template.yaml`:

```yaml
events:
  - handler: events.created.handler
    pattern:
      event_name: [INSERT]
      table_name: [users]
    timeout: 20s
    retries: 2
```

A pattern is a tree of field conditions:

- A plain list is implicit equality: `status: [COMPLETED, FAILED]`.
- A bare scalar is implicit equality: `event_name: INSERT`.
- A map whose keys are all operator keys holds operators.
- Any other map is a nested condition over child fields.

### Operators

| Operator              | Semantics                                                                           |
| --------------------- | ----------------------------------------------------------------------------------- |
| `equals`              | Value equals any listed value (type-preserving; numeric kinds compare numerically). |
| `prefix`              | String value starts with any listed prefix (non-strings never match).               |
| `suffix`              | String value ends with any listed suffix (non-strings never match).                 |
| `exists`              | Key presence only; takes a boolean, not a list.                                     |
| `gt` `gte` `lt` `lte` | Numeric threshold, or `now()`-relative cutoff for RFC3339 strings.                  |

`equals`, `prefix`, and `suffix` take non-empty lists; `exists` takes a strict
boolean. An unknown or misspelled operator-like key is rejected rather than
silently widening the rule.

### Comparison operators

`gt`/`gte`/`lt`/`lte` take a number (compared numerically) or a `now()`-relative
expression string:

```yaml
pattern:
  new_image:
    created_at:
      gt: "now()-5m"
```

- Only the exact `now()` / `now()±duration` syntax is temporal (`now()`,
  `now()-5m`, `now()+10m`, compound forms like `now()-1h30m`). The cutoff is
  evaluated as a UTC instant **at match time**, never at template load.
- The event value must be an RFC3339 string; offsets are respected and values
  are compared as instants, not lexicographically. A missing, `null`,
  non-string, or invalid-RFC3339 value never matches a temporal comparison.
- A literal timestamp such as `gt: "2026-09-12T10:00:00Z"` is **not** accepted;
  only `now()`-syntax is valid for string operands. The old bare `now`-style
  syntax is a validation error.
- A list of operands is OR; multiple operators on the same field are also OR,
  so `gte: "now()-1h"` together with `lt: "now()"` is not a range.

`exists` checks key presence only — `null`, `false`, `0`, `""`, `{}`, `[]` all
count as present. It works recursively: `new_image: { exists: true }` checks the
top-level key; `new_image: { name: { exists: true } }` checks `name` inside
`new_image`. A nested `exists: true` fails when a parent is missing; a nested
`exists: false` matches when the nested key is absent, including when the parent
map itself is missing.

### Combining conditions

- Values within one operator's list, and multiple operators on one field, are
  **OR**.
- Different fields (siblings) and nested children are **AND**.
- A missing event field fails that field's value conditions (but `exists: false`
  can match absence).
- Extra event fields are ignored.
- Each rule's pattern is evaluated independently: multiple rules may match the
  same event and there is no deduplication, so every matching rule runs. Event
  handler names are unique within a function, so each matching rule is a
  distinct handler invocation.

Example: `status: [COMPLETED, FAILED]` matches either value; `id: { prefix:
["user_"] }` matches `user_123` but not `123`.

## Dispatch and concurrency

For each delivered message, Relay evaluates every loaded function (in sorted
name order) and each function's rules in declaration order:

- **Per message, matching invocations run one at a time.** The runner iterates
  functions and rules in a single loop and executes each matching handler in
  turn.
- **Across messages, invocations run concurrently.** Distinct events (and
  distinct workers) execute simultaneously, bounded by the worker-global
  `MAX_CONCURRENCY` and the per-function effective concurrency
  (`min(concurrency, MAX_CONCURRENCY)`).
- A failure in one matching handler does **not** prevent the remaining matched
  handlers of that message from running. Each matching invocation gets its own
  independent attempt and outcome.

A handler runs in an isolated container with `RELAY_HANDLER` set and the event
JSON on stdin; exit code `0` is success, anything else is failure. Container
stdout/stderr is forwarded verbatim (prefixed per handler) at every log level.

## Delivery, retry, and ACK

Delivery is **at-least-once**, never exactly-once. A handler can run more than
once, so handlers must be idempotent.

- A message is acknowledged (XACK) only after **every** matching invocation is
  terminal: each succeeded, or was exhausted and routed to the DLQ.
- A failing invocation is retried per its rule's `retries` (default `4`, so `5`
  total attempts) with a fixed backoff — 1m, 2m, 5m, then 10m capped — recorded
  as a `next_attempt_at` deadline in per-invocation state. A redelivery before
  that deadline skips the invocation without executing it.
- The attempt count is the **real handler execution count**. The Redis PEL
  delivery count is diagnostic only and is not the retry driver.
- If a matched invocation cannot run because its function is unavailable, the
  message stays pending (no handler attempt counted) and is never DLQ'd for
  unavailability alone.
- If no concurrency slot frees within a bounded wait, the message stays pending
  and a later reclaim replays it — no retry is charged for a merely-blocked
  invocation.
- Recovery (`XAUTOCLAIM`) reclaims messages idle beyond `MinPendingIdle`
  (default `1m`) as a message-level backstop; whether an individual invocation
  executes is decided per-invocation from its state. Retry timing is therefore
  quantized by the reclaim cadence.
- A `TryStart` claim error fails **closed**: the handler is not executed and the
  message stays pending, because running a duplicate could race a replica that
  won the same claim.

## Dead-letter queue

When **all** non-complete matched invocations are exhausted, the message is
dead-lettered and then acknowledged. One entry is written **per exhausted
invocation**, so a message matching several functions or handlers that all
exhaust produces one correctly-attributed entry each.

- The DLQ stream is `relay:<REDIS_STREAM>:dlq` (a Relay-owned `relay:` key).
- Entries are written **before** the XACK. If a write fails, the original stays
  pending and the exhausted invocation is skipped and re-reported on redelivery,
  so the message is re-routed rather than acknowledged without an entry.
- Idempotency without scanning: once an invocation's entry is persisted its
  marker becomes `exhausted:<attempt>:<token>:dlq`, so a redelivery skips the
  entries already written and writes only the missing ones.
- Entry fields: `original_stream`, `original_id`, `group`, `consumer`, `event`,
  `reason`, `function`, `handler`, `deliveries` (diagnostic PEL count),
  `handler_attempts` (real execution count), `timestamp` (RFC 3339), and an
  optional `trace` lineage. A malformed-message entry uses `-` for
  `function`/`handler` and `handler_attempts` `0`.

Manage the DLQ with `relay dlq ls` / `inspect` / `replay` / `rm` — see
[cli.md](cli.md).

## Guarantees summary

- Events are matched including functions that are currently unavailable.
- Unmatched and malformed messages are acknowledged (malformed ones only after a
  DLQ entry is persisted).
- No message is acknowledged while any invocation is protected, running, or
  unresolved.
- A reclaimed PEL entry whose stream body no longer exists is data loss, not
  success: it is counted in `missing_payload_total` and its dangling reference
  cleared — never a fabricated payload or DLQ entry. Relay's own `ACKED`
  retention never trims an unacknowledged entry, so a nonzero value points at an
  external unsafe trim or a manual delete racing the worker.

Retry/exhaustion/backoff and the reclaim interval are fixed internals, not
environment variables.
