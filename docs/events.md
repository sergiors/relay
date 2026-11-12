# Events

Relay reads messages from the configured Redis Stream. Put the JSON object in
`value`; unrelated fields do not affect envelope decoding or event matching.
`format=raw` is optional and forces a message to be treated as a raw event.
Trace context fields (`traceparent`, `tracestate`, and `baggage`) are used for
distributed tracing when tracing is enabled.

Decoded external events are matched against app rules. Every matching handler
runs as a one-shot function. Schedule occurrences use the same stream, but are
dispatched directly to their configured handler; they do not use event matching.
See [Schedules](schedules.md).

## Event formats

### Raw JSON

An ordinary JSON object in `value` is delivered to matching rules without
normalization. For example:

```sh
redis-cli XADD events '*' value '{"type":"course.completed","studentId":"123"}'
```

Use `format=raw` when an object intentionally resembles a Debezium or schedule
envelope:

```sh
redis-cli XADD events '*' format raw value '{"source":"custom","op":"c"}'
```

The value must be a JSON object. Missing or non-string `value` fields,
non-string or unknown `format` markers, invalid JSON, and invalid recognized
envelopes are sent to the DLQ without running a handler.

### Debezium CDC

Use Debezium Server's Redis sink in `extended` format:
`debezium.sink.redis.message.format=extended`. Relay reads the change from
`value`; compact format is not supported. MongoDB and PostgreSQL records may use
schema-enabled or schemaless JSON.

Relay normalizes supported changes to a handler event with the same common fields
for both connectors. The connector-specific name field is `collectionName` for
MongoDB and `tableName` for PostgreSQL. For example, a MongoDB event is:

```json
{
  "collectionName": "users",
  "recordType": "UPDATE",
  "newImage": {},
  "oldImage": null
}
```

The corresponding PostgreSQL event uses `tableName` instead:

```json
{
  "tableName": "users",
  "recordType": "UPDATE",
  "newImage": {},
  "oldImage": null
}
```

`source.collection` (MongoDB) supplies `collectionName`; `source.table`
(PostgreSQL) supplies `tableName`. The other normalized fields are the same.
Operations `c`, `u`, and `d` become `INSERT`, `UPDATE`, and `DELETE`. Snapshot
operation `r` becomes `INSERT`; it represents an existing captured row, not a
new database write. Each document image is an object or `null`; a missing image
remains `null`. Invalid recognized changes are dead-lettered rather than treated
as raw events.

MongoDB Extended JSON values are normalized recursively before matching and
execution:

- A canonical `$oid` becomes a string; other forms remain wrapped.
- `$date` becomes an RFC3339 UTC timestamp.
- Supported numeric wrappers become JSON numbers without silently losing
  precision.
- Decimal and binary values retain their Extended JSON representation. A
  `$timestamp` wrapper is retained while its safe numeric `t`/`i` values may be
  normalized.

Only exact single-key Extended JSON wrappers are converted; objects with
additional fields remain application data. Malformed, out-of-range, or
sub-nanosecond dates are rejected. Decimal values are not converted to floating
point; invalid numeric wrappers remain wrapped. PostgreSQL images are not subject
to MongoDB-specific normalization. Values too large for a safe numeric
conversion remain exact, but numeric matching operators may not match those
values.

A Debezium tombstone is acknowledged without execution when `value` is exactly
the default null-value sentinel `default`. `format=raw` takes precedence, and
custom null-value sentinels are not recognized.

### Schedule envelopes

Relay publishes schedule occurrences to the stream. Their envelopes carry
`source: relay.schedule`, an app and schedule name, the scheduled instant, and a
deterministic `occurrence_id`. Relay validates the envelope and dispatches it to
the current or already-admitted handler for that schedule. Invalid claims are
dead-lettered and never matched as ordinary events. The handler receives only
`{"source":"relay.schedule","scheduled_at":"…"}`. See
[Schedules](schedules.md) for identity and recovery behavior.

## Event matching

Declare event rules in an app's `template.yaml`. This MongoDB CDC example matches
`collectionName`; for PostgreSQL CDC, use `tableName` in its place:

```yaml
events:
  - handler: events.created.handler
    pattern:
      recordType: [INSERT]
      collectionName: [users]
    timeout: 20s
    retries: 2
```

A pattern is a tree of field conditions. Alternatives in a field's list are
**OR**; sibling fields and nested conditions are **AND**. A bare value means
equality, and nested maps match object fields:

```yaml
pattern:
  newImage:
    status: [COMPLETED, VERIFIED]
    certificate: [{exists: false}]
    completed_at: [{gt: "now()-5m"}]
```

Supported operators:

| Operator | Meaning |
| --- | --- |
| `prefix`, `suffix` | String prefix or suffix |
| `exists` | Whether the field is present; `null` counts as present |
| `gt`, `gte`, `lt`, `lte` | Numeric comparison or comparison with a `now()`-relative time |

Temporal operands use `now()` or `now()±duration`, such as `now()-5m`. The
cutoff is evaluated at match time. Event values must be valid RFC3339 strings;
offsets are compared as instants. Missing, null, or invalid date values do not
match. Use `exists: false` to match an absent field; a present `null` field does
not count as absent. See [Apps](apps.md) for the complete template reference.

Unknown operators and invalid pattern structures fail template validation.
Patterns are limited to depth 8 and a canonical JSON size of 4096 Unicode code
points.

Every rule is evaluated independently. Multiple rules may match one event, and
each matching handler runs once for that delivery. Unmatched events are
acknowledged without retry.

## Execution, retries, and delivery

Handlers receive the decoded event as JSON on stdin. For one message, Relay runs
matching handlers sequentially; separate messages may run concurrently, subject
to worker and app concurrency limits.

Delivery is **at least once**, not exactly once. A handler may run again after a
crash, so handlers that cause side effects should be idempotent. Relay
acknowledges a message only after every matched invocation is terminal: it
succeeded or was dead-lettered after exhausting its retry budget.

`retries` is the number of additional attempts (default `4`, maximum `100`).
Backoff is fixed at 1m, 2m, 5m, then 10m for later retries. Events remain
pending while an app is unavailable, a handler is running or in backoff, or
execution capacity is unavailable. Capacity rejections do not consume a retry.
Pending messages are reclaimed for recovery after about one minute; reclaim is a
recovery backstop, not the retry timer. See [Configuration](configuration.md)
for concurrency and buffer limits.

## Size limit

`MAX_EVENT_BYTES` (default `256KiB`, maximum `1MiB`) limits the raw `value` size.
Oversized messages are dead-lettered before decoding or execution, with a bounded
diagnostic summary instead of the full payload. The setting limits the message
body after Redis has delivered it; it does not limit Redis entry overhead. See
[Configuration](configuration.md#event-size-limit).

## Dead-letter queue

Relay stores dead letters durably in PostgreSQL before acknowledging their
source message. A row is written per exhausted invocation; malformed messages
use a non-replayable placeholder. Rows remain unsettled until the source ACK is
confirmed, and only settled rows can be removed or expired.

Inspection preserves the original Redis `value`. Replay uses the separately
stored `replay_payload`, which is the exact logical handler input: normalized
Debezium event, unchanged raw event, or schedule handler payload. Invalid and
oversized messages have no replay payload. Replay is also at least once; keep
handlers idempotent. See [CLI](cli.md#relay-dlq) for inspection, replay, and
removal commands, and [Operations](operations.md#dlq-controls) for recovery
guarantees.
