# Events

Relay consumes external events from the configured Redis Stream with a consumer
group. Each external-event entry carries an `event` field whose value is a JSON
**object**; Relay classifies it against every loaded app's event rules, matches
that object against their patterns, and runs the matching handlers. Schedule
occurrences use the same stream and consumer group but a separate schedule
dispatch path: they resolve/adopt the configured handler directly and bypass
ordinary event classification and pattern matching (see
[schedules.md](schedules.md)).

```
Producer → Redis Stream → Relay (classify + pattern-match external event)
→ matching handler → one-shot function → shared invocation lifecycle → XACK
```

An external-event message that decodes successfully is classified `matched` or
`unmatched` **before any execution**; an unmatched event is acknowledged and
never retried. A malformed external-event message (missing `event`, not a string,
not a JSON object) can never succeed and is routed straight to the DLQ on first
encounter without running a handler, then acknowledged.

A message whose raw `event` value exceeds `MAX_EVENT_BYTES` (default 256 KiB) is
also non-retryable and routed straight to the DLQ before decode, matching, or a
handler runs. Because the cap is checked on the raw value, this applies to any
message kind — ordinary external events and schedule-claim payloads alike. The
check is a Relay-boundary guard: Redis and go-redis have already materialized the
entry before Relay can inspect it, so it is not transport-level protection
against a huge entry, and metadata/RESP overhead is outside the setting.

## Publishing an event

Any producer can append to the stream. With `redis-cli` against the configured
stream:

```sh
redis-cli XADD events '*' event '{"event_name":"INSERT","table_name":"users"}'
```

Relay never assumes where events originate; it only reads the stream.

Events may also be created by `relay app invoke` (manual, synchronous, not
part of the stream lifecycle — see [cli.md](cli.md)). Relay also publishes its
own schedule occurrences to this stream; those are not external events and do
not pass through the classification and pattern-matching flow described here
(see [schedules.md](schedules.md)).

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

A pattern is a tree of field conditions. Each field's condition is an
**ordered list of alternatives (OR)**. A list element is one of:

- a bare scalar literal — equality: `status: [COMPLETED, FAILED]` matches either
  value;
- a single-operator map — `id: [{prefix: "user_"}]`, `score: [{gt: 70}]`,
  `deleted_at: [{exists: false}]`;
- a nested map — a group of child fields, ANDed with each other:
  `new_image: {status: [COMPLETED]}`.

A bare map value is shorthand for a single-element list, so `new_image:
{status: [COMPLETED]}` and `new_image: [{status: [COMPLETED]}]` are equivalent.
A list element may itself be a nested map, which lets one field carry several
alternative sub-objects:

```yaml
pattern:
  result: [{ status: [OK] }, { status: [DEGRADED] }]
```

The following are rejected as parse errors, never silently widened:

- a scalar as a whole field condition (`status: COMPLETED`): every condition is
  a list — write `status: [COMPLETED]`;
- an empty list, an empty map, or a null condition;
- an operator used as a nested field name, and an unknown or misspelled
  operator-like key (`{prefix: 12}`, `{prefx: "x"}`).

### Operators

| Operator              | Semantics                                                            |
| --------------------- | -------------------------------------------------------------------- |
| `prefix`              | String value starts with the given prefix (non-strings never match). |
| `suffix`              | String value ends with the given suffix (non-strings never match).   |
| `exists`              | Key presence only; takes a boolean, not a list.                      |
| `gt` `gte` `lt` `lte` | Numeric threshold, or `now()`-relative cutoff for RFC3339 strings.   |

Each operator takes a **single scalar operand**. Multiple prefixes, suffixes, or
thresholds for one field are separate alternatives:

```yaml
id:
  - prefix: "user_"
  - prefix: "org_"
amount:
  - lt: 10
  - gte: 1000
```

There is no `equals` operator: a bare literal is equality. An unknown or
misspelled operator-like key is rejected rather than silently widening the rule.

### Comparison operators

`gt`/`gte`/`lt`/`lte` take a number (compared numerically) or a `now()`-relative
expression string:

```yaml
pattern:
  new_image:
    created_at:
      - gt: "now()-5m"
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
- Multiple alternatives on the same field are OR, so `{gte: "now()-1h"}` together
  with `{lt: "now()"}` is not a range.

`exists` checks key presence only — `null`, `false`, `0`, `""`, `{}`, `[]` all
count as present. It works recursively: `new_image: [{exists: true}]` checks the
top-level key; `new_image: {name: [{exists: true}]}` checks `name` inside
`new_image`. A nested `exists: true` fails when a parent is missing; a nested
`exists: false` matches when the nested key is absent, including when the parent
map itself is missing.

### Combining conditions

- Alternatives within one field's list are **OR**.
- Different fields (siblings) and nested children are **AND**.
- A missing event field fails that field's value conditions (but `exists: false`
  can match absence).
- Extra event fields are ignored.
- Each rule's pattern is evaluated independently: multiple rules may match the
  same event and there is no deduplication, so every matching rule runs. Event
  handler names are unique within an app, so each matching rule is a
  distinct handler invocation.

Example: `status: [COMPLETED, FAILED]` matches either value; an `id` field
with `- prefix: "user_"` matches `user_123` but not `123`.

### Pattern limits

Each `pattern` is bounded by two **fixed limits**. They are not configurable:
there is no environment variable or template key that changes them.

- **Structural depth 8.** Depth counts field nesting plus nested
  composite-literal containers. Each top-level pattern field is level 1 and each
  nested child field is one level deeper, so a pattern whose deepest field is at
  level 8 is accepted and one at level 9 is rejected. The grammar wrappers add no
  depth: a field's OR-alternative list (`status: [A, B]`) and a single-key
  operator map (`id: [{prefix: "x"}]`) do not consume a level, and
  siblings/alternatives do not accumulate. A nested-map alternative introduces its
  child fields at the next level, so `new_image: [{status: [COMPLETED]}]` puts
  `status` at level 2. A **composite literal container** — a list or map used as
  an equality value, e.g. `tags: [[1, 2]]` — consumes one level per container
  nesting, so a deeply nested composite cannot bypass the cap.
- **Size 4096 Unicode characters.** The size is measured per `pattern` (never the
  whole template, never the sum across a rule's patterns) as the number of
  Unicode **code points** in that pattern subtree's canonical compact JSON
  serialization: `encoding/json`'s compact form with object keys sorted. JSON
  punctuation, quotes, and escaping all count, including `encoding/json`'s
  HTML-sensitive escapes (`<` becomes `\u003c`). Because it is a code-point count,
  each multibyte character contributes exactly one code point rather than its
  UTF-8 byte length; the total serialized pattern, JSON syntax included, must
  still be <= 4096. Non-finite YAML floats (`.nan`/`.inf`), which JSON cannot
  represent, are measured as the four-character literal `null`. A mapping with
  non-string keys has no JSON encoding and can never match a JSON object, so it is
  rejected as a parse error rather than measured.

Both limits are enforced once when the template is parsed: an over-limit pattern
fails `template.yaml` validation with an error naming the rule (and, for depth,
the offending field path) and the exceeded limit. An accepted pattern parses,
matches, and is indexed exactly as before. A valid nested pattern:

```yaml
pattern:
  new_image:
    status: [COMPLETED]
    metadata:
      source: [{prefix: "sync_"}]
```

## Dispatch and concurrency

For each delivered external-event message, Relay evaluates every loaded app (in
sorted name order) and each app's rules in declaration order:

- **Per message, matching invocations run one at a time.** The runner iterates
  apps and rules in a single loop and executes each matching handler in
  turn.
- **Across messages, invocations run concurrently.** Distinct events (and
  distinct workers) execute simultaneously, bounded by the worker-global
  `MAX_CONCURRENT_INVOCATIONS` and the per-app effective concurrency
  (`min(concurrency, MAX_CONCURRENT_INVOCATIONS)`).
- A failure in one matching handler does **not** prevent the remaining matched
  handlers of that message from running. Each matching invocation gets its own
  independent attempt and outcome.

A matched event handler runs as a one-shot function in an isolated container,
with `RELAY_HANDLER` set and the external event JSON on stdin; exit code `0` is
success, anything else is failure. Container stdout/stderr is forwarded verbatim
(prefixed per handler) at every log level. A schedule-triggered function
invocation uses the same one-shot runtime and outcome machinery but receives an
occurrence payload and is selected by schedule dispatch, not event pattern
matching.

## Delivery, retry, and ACK

Delivery is **at-least-once**, never exactly-once. A handler can run more than
once, so handlers must be idempotent.

- A message is acknowledged (XACK) only after **every** matching invocation is
  terminal: each succeeded, or was exhausted and routed to the DLQ.
- A failing invocation is retried per its rule's `retries` (default `4`, so `5`
  total attempts; maximum `100`, so at most `101` total attempts) with a fixed
  backoff — 1m, 2m, 5m, then 10m capped — recorded
  as a `next_attempt_at` deadline in per-invocation state. A redelivery before
  that deadline skips the invocation without executing it.
- The persisted attempt count is the number of **admitted claims** (`TryStart`),
  which normally equals the real handler execution count but can exceed it in the
  crash-before-execute window described
  [below](#where-the-attempt-count-advances). It drives exhaustion; the Redis PEL
  delivery count is diagnostic only and is not the retry driver.
- If a matched invocation cannot run because its app is unavailable, the
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

### Where the attempt count advances

The **claim is the attempt boundary**, not the handler start. The runner
persists the attempt (via `TryStart`, atomically with the running deadline and a
fresh claim token) only **after** both the concurrency slot and the warm-container
budget have been admitted, and every later transition — retry, completion,
exhaustion — is compare-and-set on that claim. This makes the capacity gates
strictly non-charging:

- A **congestion/backpressure** rejection before the claim (no concurrency slot
  within the bounded wait, or a saturated warm-container budget) records no
  attempt and charges no retry or DLQ: the message stays pending and is replayed.
- A **lifecycle cancellation** (worker shutdown) changes no **persisted**
  retry/exhaustion state. If it lands before the claim, no attempt is claimed at
  all; if it lands after a confirmed claim, that claimed attempt is spent exactly
  like a crash in the same window (see below), but **no** retry backoff and
  **no** exhausted/DLQ marker is written. The stream leaves the message pending
  regardless, so repeated shutdowns cannot consume the retry budget or
  dead-letter work. This is narrower than "the failure is not charged": failure
  **telemetry** is separate from persisted invocation state, and a post-claim
  execution that reports an error on a canceled delivery is still counted by
  `handler_failure_total`, the labeled invocation-outcome and per-app failure
  counters, and the `last_failure_at` app timestamp. The runner records the
  execution failure before its cancellation guard suppresses the persisted
  transition, so the counter moves even though no retry/exhaustion state does.
- A crash **after** a confirmed claim but **before** the handler actually starts
  leaves the persisted running marker behind. Its deadline simply elapses (the
  message is never lost and the invocation is not permanently locked), after which
  a reclaim claims the **next** attempt. The unexecuted attempt is therefore
  consumed, and repeated lost claims raise the persisted count further. The
  configured retry budget bounds normal failing executions, not guaranteed
  user-code runs: it does **not** cap admitted claims across repeated crashes, so
  the persisted count can exceed `1 + retries` before any real failure. This is
  the accepted at-least-once trade-off (a bounded exposure of at most one attempt
  per crash); closing it would require a two-phase claim/execute handshake that
  at-least-once delivery does not provide. The same accounting applies to the
  narrow create-time warm-budget race, which happens after the claim like any
  other crash in that window.

  `TryStart` **claims but does not exhaust**: it has no retry budget and never
  writes a terminal marker, so a crash **alone** never dead-letters a message — it
  only advances the persisted attempt count. The invocation becomes terminal (and
  the message routable to the DLQ) only when a **later** delivery actually runs the
  handler and it fails with the persisted count already at or above
  `1 + retries`. In other words, repeated lost claims can make the next **real**
  failure exhaust with a larger persisted attempt count; the failure is what
  triggers the DLQ.

### Invocation state persistence

Per-message invocation state is a Redis hash keyed by message and
`<app>/<handler>`. While the message is still pending (recoverable — it can
be redelivered from the PEL) that hash is **persistent with no TTL**, so however
long a message sits pending its `running`/`next_attempt_at`/terminal markers are
still there when a reclaim reads them. Only after the message has left the PEL —
a successful XACK on the success, obsolete-schedule, or DLQ path, or a purged
missing-payload PEL reference — is the hash switched to **terminal retention**:
a reserved marker plus the configured `REDIS_INVOCATION_RETENTION` TTL (default
`48h`; empty/`0`/negative disables the expiry while still writing the marker, so
the hash stays persistent). For every path that ACKs, the XACK and that retention
switch are performed by **one atomic Lua script**, so a crash or error can never
leave a hash whose message already left the PEL without a marker/TTL; a failed
ACK stages only a persistent marker (no TTL), so a still-pending message's state
never expires (the terminal marker is nonetheless present, and a redelivery just
re-acks). Once retained, every lifecycle transition
from a stale in-memory delivery is inert, so it can neither mutate the retained
state nor remove the retention TTL. A hash whose retention never runs is simply
leaked, never prematurely expired.

## Dead-letter queue

When **all** non-complete invocations dispatched for a message are exhausted,
the message is dead-lettered and then acknowledged. One entry is written **per
exhausted invocation**, so an external event matching several apps or handlers
that all exhaust produces one correctly-attributed entry each. Scheduled
occurrence functions use this shared invocation/DLQ infrastructure.

- The DLQ is a **PostgreSQL table** (`dlq`), Relay's sole durable dead-letter
  persistence, admin read model, and retention store. It has no broker stream,
  consumer group, PEL, or XACK flow: rows are inserted before the source ACK and
  read only by the admin `relay dlq` commands.
- Each row is inserted **idempotently by a stable ID** derived from the logical
  source delivery identity (source stream, group, message ID) plus the
  invocation identity, so a crash, an ambiguous transaction, or a failed source
  ACK can neither duplicate nor lose a row. A real invocation's retained
  exhausted marker is validated against the runner's attribution before the
  insert, so a stale claim can never create an entry. Rows are written **before**
  the XACK. If persistence fails, the original stays pending and the invocation
  is re-reported on redelivery, so the message is re-routed rather than
  acknowledged without an entry. See
  [DLQ persistence contract](#dlq-persistence-contract).
- Delivery identity is scoped to one Relay deployment's state database. The
  source, group, and message ID are opaque logical coordinates, not a promise
  that broker-assigned IDs are globally unique. Workers in one deployment must
  share the database; separate deployments must not. A future broker adapter
  must supply a deterministic delivery identity that includes enough broker and
  resource scope to avoid reusing coordinates across recreated streams/topics.
  The Redis adapter treats stream and group names as that scope; destructive
  recreation with reused names and message IDs is unsupported. Use a new stream
  or group name rather than reusing old coordinates.
- Rows start **unsettled** and become settled only after the source ACK is
  confirmed. A bounded worker reconciliation repairs a crash between a successful
  Redis ACK and the PostgreSQL settlement marker by asking the exact source
  stream/group whether the delivery is still in its PEL; it pages past a
  delivery-ID cursor and wraps, so permanently pending rows cannot starve later
  deliveries, marks settled only when the delivery is no longer pending, and
  preserves rows on any Redis error. Admin removal of an unsettled entry is
  refused, and retention deletes only settled rows, so a delivery that may still
  be recoverable can never lose its dead letter.
- **Replay admission is a fenced lease.** `relay dlq replay` atomically claims
  one settled row, increments a monotonic fencing token, and stamps a ten-minute
  lease in database time before contacting the worker. A concurrent replay gets
  an explicit replay-in-progress error and never dials. A definitive
  worker-reported failure, a failed dial, or an explicit no-invoker answer
  releases the claim; a request write failure, lost or malformed response, other
  I/O failure, or client cancellation retains it until expiry, which is also how
  a crashed claimant recovers. The success delete is one conditional statement
  requiring the settled state, the same token, and an unexpired lease, so a stale
  claimant cannot remove a re-claimed row. Admin removal and retention both skip
  an active claim. Replay is **at-least-once**: the worker does not cancel
  execution on disconnect, and work that outlives the lease can be replayed
  again.
- The DLQ is pruned **age-only** by `DLQ_RETENTION` (default 7 days, a bounded
  PostgreSQL `DELETE` of **settled** rows with no active replay claim). This is
  separate from the source stream's `REDIS_STREAM_RETENTION` ACKED trim and from
  `REDIS_INVOCATION_RETENTION` (the terminal invocation-state TTL); all three are
  independent windows. See
  [configuration.md](configuration.md#stream-and-dlq-retention).
- Idempotency without scanning: the stable row ID is the idempotency key, so a
  redelivery re-inserts only the missing rows and leaves already-committed rows
  untouched. A malformed-message entry has no handler invocation and uses the
  same stable identity with an empty invocation component.
- Storage follows the state-package convention: `id TEXT PRIMARY KEY`,
  `data JSONB NOT NULL` (private payload), plus only the relational
  `delivery_id`, nullable `settled_at_ms`, `created_at_ms`, and the replay
  admission columns `replay_claim_token` and `replay_lease_until_ms` (part of the
  fresh schema; there is no migration or existing-database compatibility).
  Event and reason bytes are base64-encoded inside the JSON payload, so arbitrary
  malformed bytes, including NUL, survive JSONB losslessly.
- Entry fields: `id`, `delivery_id`, `source`, `source_group`,
  `source_message_id`, `consumer`, `event`, `reason`, `app`, `handler`,
  `deliveries` (diagnostic source count), `handler_attempts` (the persisted
  admitted-claim count; normally the real execution count, but a crash or
  abandoned claim can advance it without an execution), `timestamp` (RFC 3339),
  `settled_at` (empty while unsettled), and an optional `trace` lineage. A
  malformed-message entry uses `-` for `app`/`handler` and
  `handler_attempts` `0`. A schedule occurrence's event envelope
  (`occurrence_id`, `scheduled_at`) is stored verbatim, preserving provenance.
- An oversized-event entry is intentionally **summary-only and non-replayable**:
  it carries the same `-` placeholder app/handler (no handler invocation ever
  ran) and its `event` field is a small diagnostic JSON summary
  (`relay_summary`, `event_bytes`, `max_event_bytes`) instead of the oversized
  payload. Relay deliberately does not parse the huge payload for an `event_id`.
  `relay dlq replay` rejects it as non-replayable; use `inspect` to read the
  summary and `rm` to remove it. Oversized rejections are counted by the
  unlabeled `relay_events_oversized_total`.

### DLQ persistence contract

- **Idempotency is per exhausted invocation.** The unit of idempotency is one
  exhausted invocation, keyed by a stable ID derived from the logical source
  delivery identity (source stream, group, message ID) and the invocation
  identity. Re-persisting the same exhaustion yields **at most one** PostgreSQL
  row (`INSERT ... ON CONFLICT DO NOTHING`).
- **Validation before insert.** A real invocation's retained exhausted marker is
  re-read from Redis and its attempt/token identity must match the runner's
  attribution; a missing or stale marker refuses the insert and leaves the
  source pending. The marker remains terminal and PERSISTENT while the source is
  recoverable, so a validated identity cannot be superseded.
- **Source XACK follows all required persistence.** The source message is
  acknowledged only **after** every required DLQ row has been committed. If a
  PostgreSQL insert fails (or its outcome is ambiguous), the source stays pending
  and the invocation is re-reported on redelivery, so the message is re-routed
  rather than acknowledged without its entries.
- **Failed or crashed XACK after successful persistence.** If the XACK fails or
  the process crashes after the rows were committed, the message stays in the
  PEL and is redelivered; the redelivery **re-inserts idempotently** and retries
  the ACK. The terminal Redis retention TTL is applied only after a confirmed
  XACK, so the exhausted markers cannot expire while the source is recoverable.
- **Settlement follows a confirmed ACK.** All rows for one source delivery share
  a `delivery_id`. After `ackAndRetainTerminal` returns successfully, the stream
  marks that delivery settled; the update never happens before the ACK. If the
  process crashes between the ACK and that update, a bounded worker
  reconciliation asks Redis whether the exact source stream/group/message is
  still pending. It marks settled only when the delivery is gone from the PEL;
  Redis errors, an unavailable group/stream, or a decode failure preserve the
  unsettled rows for a later attempt.
- **Only settled rows are removable.** `relay dlq rm` returns an explicit error
  for an unsettled entry, and `DLQ_RETENTION` deletes only settled rows. Removal
  never mutates broker invocation state and never causes execution.
- **Partial fan-out.** A message matching several apps or handlers writes one
  row per exhausted invocation. A redelivery after a partial write **skips the
  rows already committed and retries only the missing ones**, so completed rows
  are neither duplicated nor lost.
- **At-least-once, not exactly-once.** These guarantees make DLQ persistence
  idempotent and correctly ordered; they do **not** make source delivery or
  handler processing exactly-once. A handler can still run more than once, so
  handlers must be idempotent (see
  [Delivery, retry, and ACK](#delivery-retry-and-ack)).

Manage the DLQ with `relay dlq ls` / `inspect` / `replay` / `rm` — see
[cli.md](cli.md).

## Guarantees summary

- External events are classified and matched including apps that are currently
  unavailable; schedule occurrences bypass that matching and dispatch by
  schedule name to the configured/admitted handler.
- Unmatched external events and malformed external-event messages are
  acknowledged (malformed ones only after a DLQ entry is persisted).
- No message is acknowledged while any invocation is protected, running, or
  unresolved.
- A reclaimed PEL entry whose stream body no longer exists is data loss, not
  success: it is counted in `missing_payload_total` and its dangling reference
  cleared — never a fabricated payload or DLQ entry. Relay's own `ACKED`
  retention never trims an unacknowledged entry, so a nonzero value points at an
  external unsafe trim or a manual delete racing the worker.

Retry/exhaustion/backoff and the reclaim interval are fixed internals, not
environment variables.
