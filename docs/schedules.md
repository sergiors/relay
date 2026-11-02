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
instant in UTC:

```text
schedule:<app>:<schedule-name>:<scheduled-at-RFC3339-UTC>
```

Timezone affects when a schedule fires, not its identity. Relay validates the
occurrence ID against the app, schedule, and scheduled time in the envelope, and
checks that the time is a real firing of the configured cron. Invalid claims are
dead-lettered without running a handler; a removed, never-admitted schedule is
treated as obsolete and acknowledged.

The scheduled handler receives only this payload on stdin:

```json
{ "source": "relay.schedule", "scheduled_at": "2026-10-31T03:00:00Z" }
```

Coordination fields in the internal occurrence envelope are not exposed to the
handler.

Every worker may evaluate every schedule. Relay deduplicates publication by
occurrence, so only one stream entry is published cluster-wide. This guarantees
single publication, not exactly-once handler execution: schedule handlers use
the same at-least-once delivery and recovery behavior as event handlers and
should be idempotent. Publication deduplication lasts 14 days, longer than the
seven-day outbox retry window.

## Changes to schedules

Before an occurrence is first admitted, Relay resolves the current app and
schedule by name. A handler or retry configuration change therefore applies to
an occurrence that has not yet started; if the schedule has been removed, that
occurrence is acknowledged as obsolete.

At first admission, Relay pins the handler, timeout, and retry settings for that
invocation. Later configuration changes or schedule removal do not cancel or
reset an admitted invocation; it finishes under the pinned settings.

## Recovery and availability

Relay persists each due occurrence to a PostgreSQL outbox before publishing it
to Redis. If publication is ambiguous or Redis is unavailable, Relay retries
the same occurrence identity across restarts. The outbox retains unresolved
occurrences for up to seven days; expired occurrences are not published.

On startup, Relay also performs a bounded catch-up for the latest missed
occurrence per schedule within the previous 24 hours. Older missed occurrences
are intentionally skipped. Catch-up and outbox retries use the same publication
deduplication, so they do not create a second entry for an occurrence already
published.

Schedule firing pauses if the outbox cannot be used and resumes after storage
recovers. This runtime scheduler degradation does not stop event consumption or
make the worker unready. PostgreSQL is required when the worker starts. See
[Operations](operations.md#state-paths-and-persistence) and
[Configuration](configuration.md#retention-windows).

If an occurrence is held only in memory while the outbox is unavailable, a crash
before it is persisted can lose it. Startup catch-up may recover the latest
missed occurrence within 24 hours; it does not replay every missed firing.

## Reliability summary

- Publication is deduplicated cluster-wide by deterministic occurrence ID.
- The 14-day deduplication window outlasts the seven-day outbox retry window.
- Unresolved publication intent is retried durably for up to seven days.
- Startup catch-up is limited to the latest missed occurrence within 24 hours.
- Handler execution is at least once, so handlers should be idempotent.
- Event and schedule functions share retry, recovery, and DLQ behavior; schedule
  occurrences bypass event matching.
