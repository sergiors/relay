# Schedules

A schedule is a trigger for a one-shot function. Each firing creates an
occurrence and publishes it to the same Redis Stream used for external events.
Relay dispatches an occurrence directly to its configured handler; event
patterns are not evaluated. See [Events](events.md) for the shared retry and
DLQ behavior.

An app must also declare at least one event rule or service; schedules alone do
not make a valid app. See [Apps](apps.md).

## Configure a schedule

Each schedule needs a stable, app-unique `name`, a handler, and a cron
expression:

```yaml
schedules:
  - name: nightly-cleanup
    handler: jobs.cleanup.handler
    cron: "0 3 * * *"
    timezone: Europe/Rome # optional; defaults to UTC
    timeout: 20s
    retries: 2 # additional attempts; defaults to 4, maximum 100
```

Relay accepts standard five-field cron expressions and the descriptors
`@hourly`, `@daily`/`@midnight`, `@weekly`, `@monthly`, and
`@yearly`/`@annually`. Schedules have minute granularity. Seconds-based
expressions and `@every` are not supported. Use the `timezone` field for an IANA
timezone; embedded `TZ=` and `CRON_TZ=` prefixes are rejected.

Changing the cron, timezone, handler, timeout, or retries under the same name
updates that schedule without affecting other schedules. Multiple schedules may
use the same handler.

## Occurrence identity and delivery

An occurrence is identified by its app, schedule name, and absolute scheduled
instant in UTC. Changing the timezone does not change the identity of an
occurrence.

Relay checks that an occurrence matches the schedule and a real firing time.
Invalid schedule messages are dead-lettered without running a handler. If a
schedule is removed before an occurrence is admitted, that occurrence is
obsolete and acknowledged.

The scheduled handler receives only this payload:

```json
{ "source": "relay.schedule", "scheduled_at": "2026-10-31T03:00:00Z" }
```

Every worker may evaluate every schedule. Relay deduplicates publication by
occurrence, so only one stream entry is published cluster-wide. This guarantees
single publication, not exactly-once handler execution: schedule handlers use
the same at-least-once delivery and recovery behavior as event handlers and
should be idempotent. Publication deduplication lasts 14 days, longer than the
seven-day publication retry window.

## Changes to schedules

Before an occurrence starts, Relay resolves the current app and schedule by name.
Changes to the handler or retry settings therefore apply to work that has not
started; if the schedule has been removed, that occurrence is acknowledged as
obsolete.

Once Relay accepts an occurrence for execution, its handler, timeout, and retry
settings remain fixed. Later configuration changes or schedule removal do not
cancel it.

## Recovery and availability

Relay records each due occurrence in PostgreSQL before publishing it to Redis.
If publication is ambiguous or Redis is unavailable, Relay retries the same
occurrence across restarts. Unresolved publications are retried for up to seven
days; expired occurrences are not published.

On startup, Relay also performs a bounded catch-up for the latest missed
occurrence per schedule within the previous 24 hours. Older missed occurrences
are intentionally skipped. Catch-up and publication retries use the same
deduplication, so they do not create a second entry for an occurrence already
published.

Schedule firing pauses if PostgreSQL is unavailable and resumes when it
recovers. This does not stop event consumption or make the worker unready.
PostgreSQL is required when the worker starts. See
[Operations](operations.md#state-paths-and-persistence) and
[Configuration](configuration.md#retention-windows).

If Relay crashes before saving a due occurrence while PostgreSQL is unavailable,
that occurrence can be lost. Startup catch-up may recover the latest missed
occurrence within 24 hours; it does not replay every missed firing.
