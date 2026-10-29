# CLI

Relay ships as one binary. `relay start` runs the long-running runtime; every
other command is administrative or inspection-only and never starts the runtime.

```
relay start
relay health
relay stats [reset]
relay app <ls|inspect|invoke>
relay apps                                 # short form of `relay app ls`
relay dlq <ls|inspect|replay|rm>
relay secret <ls|set|rm>
relay git <keygen|set|sync|status|remove>
```

Running `relay` with no command prints the top-level help. A grouping command run
bare (e.g. `relay app`) prints its subcommand help; an unknown subcommand is
a usage error naming the full command path.

`relay apps` is a thin top-level alias for `relay app ls` and the only short
form; `relay app ls` stays canonical. It is a leaf command with no namespace: it
lists apps exactly as `relay app ls` does, and any positional argument
(including `relay apps inspect` or `relay apps invoke`) is a usage error rather
than a subcommand dispatch. Use `relay app inspect` / `relay app invoke` for
those operations.

## Prerequisites per command

| Command                       | Needs Redis | Needs Docker | Needs a running worker | Reads/writes                                 |
| ----------------------------- | ----------- | ------------ | ---------------------- | -------------------------------------------- |
| `start`                       | yes         | yes          | —                      | runtime                                      |
| `health`                      | no          | no           | **yes**                | worker socket                                |
| `stats`                       | no          | no           | no                     | state DB (read)                              |
| `stats reset`                 | no          | no           | optional (socket)      | state DB (write); worker socket when running |
| `app ls` / `inspect` / `apps` | no          | no           | optional (inspect)     | state DB (read); socket for live pool gauges |
| `app invoke`                  | no          | no           | **yes**                | worker socket                                |
| `dlq ls` / `inspect` / `rm`   | no          | no           | no                     | PostgreSQL DLQ (read/write)                  |
| `dlq replay`                  | no          | no           | **yes**                | PostgreSQL DLQ + worker socket               |
| `secret ls` / `set` / `rm`    | no          | no           | no                     | local secret files                           |
| `git *`                       | no          | no           | no                     | local git config/key/checkout + `/apps`      |

Commands that need Redis resolve the Redis configuration with the same required
`REDIS_*` validation as `relay start`, so a missing `REDIS_URI`,
`REDIS_STREAM`, or `REDIS_GROUP` is an error for them too. `relay dlq` instead
requires `DATABASE_URL`: the durable DLQ lives in PostgreSQL, and those commands
never start or contact the worker unless `replay` needs its socket.

## relay start

Runs the runtime in the foreground, holds the process lock, and blocks until
SIGINT/SIGTERM. It never daemonizes or writes a PID file. Fails fast if another
`relay start` holds the lock, if Redis or Docker is unreachable, or if a required
variable or startup verification fails (e.g. a missing `NETWORKS` network or a
taken metrics/webhook port).

## relay health

```
relay health
```

Asks the **running worker** — over the same Unix control socket `app
inspect` and `stats reset` use — whether it is healthy: ready to consume with
its live dependencies healthy. Exits `0` when healthy, `1` otherwise. It requires
no `REDIS_*`/Docker configuration in the CLI process and never creates a Redis or
Docker client itself; the answer comes entirely from the worker over its socket
(see [operations.md](operations.md) for details). Used as the container
healthcheck.

- No running worker, a worker still starting, or a shutting-down worker is **not
  healthy**. Readiness is set only at the ready-to-consume boundary (after the
  dependency preflight, app load/preparation, and the socket/listener/loop
  wiring, immediately before consumption) and cleared first on shutdown; it is
  bound to the worker lifecycle, so a lifecycle cancellation that precedes the
  clear also reports not-healthy.
- In steady state the worker answers from **live** dependency health: Redis
  consumer health, a bounded Docker ping, and `NETWORKS` verification. A
  dependency failing reports not-healthy, and recovery reports healthy again — no
  restart required. PostgreSQL (and therefore the scheduler state), optional tracing, and asynchronous service
  convergence/housekeeping are not blockers, and a per-app
  `degraded`/`unavailable`/`invalid` status does not change worker health.

## relay stats

```
relay stats
```

```
Events received:     153000
Events matched:      152934
Events unmatched:    66
Handler successes:   152801
Handler failures:    133
Message reclaims:    82
DLQ entries:         4
Pending entries:     17
Oldest pending age:  2m14s
Updated:             10s ago
```

Reads the persisted snapshot only; works when the runtime is down. A fresh
database renders zeroes with `Updated: never`. Backlog gauges are global.

`Message reclaims` is `relay_retries_total`: stream messages reclaimed and
re-delivered, **not** handler retries (`relay_function_retries_total`, rendered
per app by `relay app inspect` as `Handler retries`). `DLQ entries`
is `relay_dlq_entries_total`: successful inserts into the PostgreSQL DLQ (one per
exhausted invocation, plus a placeholder per malformed message), **not** the
count of invocations that exhausted their retries (that is
`relay_function_dlq_total`, rendered per app as `Invocations exhausted`).

```
relay stats reset
```

Zeroes the cumulative global and per-app totals in place. With a running
worker it resets through the worker socket (so the in-memory source resets too
under the flush lock); otherwise directly in the database. Backlog gauges are
not reset.

## relay app

`ls` and `inspect` read the state database only — no Redis, no Docker, no
`/apps` (they need `DATABASE_URL` and a reachable database).

```
relay app ls
relay apps        # equivalent short form
```

```
NAME                  RUNTIME      STATUS    UPDATED
user-events-python    python3.14   ready     12s ago
welcome-email-node    node24       ready     12s ago
```

`UPDATED` is `prepared_at` (else `updated_at`) as a relative age; rows are sorted
by name. `relay apps` is a top-level alias for `relay app ls`; it accepts no
arguments and owns no subcommands (`relay apps inspect` and `relay apps invoke`
are usage errors — use the `relay app` forms below).

```
relay app inspect user-events-python
```

Shows name/runtime/status/image/fingerprint/prepared/last-reconcile/last-error,
per-container `Resources`, per-app stats, `Events`, `Schedules`, `Services`,
`Environment` (names only, values redacted), `Secrets` (references only), and a
`Runtime pool` section. The live pool gauges (capacity, containers, busy, idle,
starting) are resolved live from an in-process provider or the worker socket;
without a reachable worker they render `unknown`, while the cumulative
warm/cold/discarded counters always come from the persisted snapshot.

The per-app stats rows are `Handler retries` (handler retry attempts,
`relay_function_retries_total`) and `Invocations exhausted` (invocations that
exhausted their retry budget, `relay_function_dlq_total`) — **not** a count of
successfully written DLQ entries. `Last exhaustion` is the last retry exhaustion
(the exhaustion commit), not a successful DLQ write. The global `relay stats`
labels `Message reclaims` and `DLQ entries` mean different things; see
[relay stats](#relay-stats).

### relay app invoke

Runs an app's matching event handlers **synchronously on the running
worker's live runtime pool**, without publishing to the stream. Supply the event
as a positional JSON object, or use `-` to read it from stdin:

```sh
relay app invoke user-events-python '{"event_name":"INSERT"}'
relay app invoke user-events-python - < event.json
echo '{"event_name":"INSERT"}' | relay app invoke user-events-python -
```

A bare `relay app invoke NAME` is a usage error; stdin is never read implicitly,
so an open stream cannot block the command. The payload must be a JSON **object**
(arrays, scalars, and `null` are rejected), and malformed JSON is reported as
`app invoke: invalid event JSON: ...`. Output is one of:

```
No matching handlers
Invoked 1 handler
Invoked N handlers
```

Inside Docker, pass `-T` so stdin stays a pipe (the default allocates a
pseudo-TTY), and use `-` to select it:

```sh
docker compose exec -T relay app invoke user-events-python - < event.json
```

Unlike stream consumption, a manual invocation is **not** part of the
at-least-once lifecycle: it never touches Redis, never claims event
classification, never schedules a retry, and never writes to the DLQ. There is
no offline fallback — a worker must be running.

## relay dlq

```
ID                     ORIGINAL               APP                  HANDLER                ATTEMPTS  AGE
8f0c...                 events/1757...-0        welcome-email-node   handler.handler        5         2m ago
```

`ls` lists entries in write order (stable ID, source message, failed
app/handler, handler attempts, age). Entry IDs are deterministic logical
identities (source delivery + invocation), so they survive worker restarts and
redeliveries. `inspect ID` shows one entry's fields plus the original
event JSON pretty-printed; a non-JSON (malformed placeholder) event is shown
verbatim. `rm ID` deletes one **settled** entry without touching broker
invocation state or causing execution. An entry whose source delivery may still
be recoverable is refused with an explicit "not settled" error; it becomes
removable only after its source ACK is confirmed (or reconciliation proves it is
no longer pending).

```
relay dlq replay ID
```

Re-executes that entry's exact recorded app/handler once on the running
worker and deletes the entry **only on success**; the entry is kept on any
failure (removed handler, failed handler, or unavailable worker). A
malformed-message placeholder has no handler to re-execute and is not replayable;
an unsettled entry is rejected before dialing because its source delivery may
still be recoverable.

These commands need PostgreSQL (`DATABASE_URL`); `replay` additionally needs the
running worker socket.

## relay secret

```
relay secret ls                      # NAME column, sorted
relay secret set database-url        # hidden prompt on an interactive terminal
relay secret set database-url -      # read all of stdin
relay secret rm database-url         # a missing secret is an error
```

`secret set NAME` requires an interactive terminal and prompts with echo
disabled; if no terminal is available it fails with guidance instead of blocking.
`secret set NAME -` reads all of stdin. A secret value is never accepted as a
positional argument: the only allowed second argument is `-`, and the value is
never printed.

```sh
relay secret set API_KEY                          # hidden prompt
printf '%s' 'value' | relay secret set API_KEY -  # read all of stdin
docker compose exec -T relay secret set API_KEY - < value.txt
```

Piped values are stored byte-for-byte except for exactly one trailing line
ending: one `CRLF`, or else one `LF`, is removed. Everything else is preserved,
including leading whitespace, trailing spaces, internal newlines, and a sole
trailing `CR`. An empty piped value is rejected.

Values are stored as files under `/var/lib/relay/secrets` and are only ever
referenced by name from `template.yaml`.

## relay git

```
relay git keygen
relay git set <repository> [--ref REF] [--path PATH] [--webhook-secret NAME]
relay git sync
relay git status
relay git remove [-y|--yes]
```

- `keygen` generates an ed25519 pair under `/var/lib/relay/ssh` and prints the
  public key. Fails if a key already exists.
- `set` remembers an SSH repository (scp-like or `ssh://`) plus optional
  `--ref` (default `main`), `--path` (monorepo subdirectory), and
  `--webhook-secret` (a secret-store name; required to start the webhook).
  Calling it again overwrites the source.
- `sync` checks out the configured ref and deterministically rewrites
  `/apps` to match the repository/path (removing directories not in the
  source). This is the only command that writes `/apps`. All app trees are
  staged before any live change; each staged tree is then compared with its
  live tree. Changed existing apps are replaced by an atomic per-app directory
  exchange; new apps are atomically renamed into place. An unchanged app keeps
  its live directory and is not reported as materialized, so an identical
  revision replaces nothing. An updated app name is never absent or partially
  copied. Sync fails before changing a live app if a needed exchange is
  unsupported.
- `status` shows the configured source, key/checkout existence, resolved commit,
  and last sync time. Nothing configured prints a message and exits `0`.
- `remove` deletes the persisted config and checkout, leaving `/apps` and
  the SSH key untouched. It prompts with a default of **No**; pass `-y`/`--yes`
  for automation.

See [operations.md](operations.md) for the webhook, SSH/TOFU behavior, and the
on-disk layout.
