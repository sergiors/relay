# CLI

Relay ships as one binary. `relay start` runs the long-running runtime; every
other command is administrative or inspection-only and never starts the runtime.

```
relay start
relay health
relay stats [reset]
relay function <ls|inspect|invoke>
relay dlq <ls|inspect|replay|rm>
relay secret <ls|set|rm>
relay git <keygen|set|sync|status|remove>
```

Running `relay` with no command prints the top-level help. A grouping command run
bare (e.g. `relay function`) prints its subcommand help; an unknown subcommand is
a usage error naming the full command path.

## Prerequisites per command

| Command                              | Needs Redis | Needs Docker | Needs a running worker | Reads/writes                                  |
| ------------------------------------ | ----------- | ------------ | ---------------------- | --------------------------------------------- |
| `start`                              | yes         | yes          | —                      | runtime                                       |
| `health`                             | yes (ping)  | yes (ping)   | no                     | nothing                                       |
| `stats`                              | no          | no           | no                     | state DB (read)                               |
| `stats reset`                        | no          | no           | optional (socket)      | state DB (write); worker socket when running  |
| `function ls` / `inspect`            | no          | no           | optional (inspect)     | state DB (read); socket for live pool gauges  |
| `function invoke`                    | no          | no           | **yes**                | worker socket                                 |
| `dlq ls` / `inspect` / `rm`          | yes         | no           | no                     | Redis DLQ stream                              |
| `dlq replay`                         | yes         | no           | **yes**                | Redis DLQ + worker socket                     |
| `secret ls` / `set` / `rm`           | no          | no           | no                     | local secret files                            |
| `git *`                              | no          | no           | no                     | local git config/key/checkout + `/functions`  |

Commands that need Redis call the same `config.Load` as `relay start`, so a
missing `REDIS_*` variable is an error for them too.

## relay start

Runs the runtime in the foreground, holds the process lock, and blocks until
SIGINT/SIGTERM. It never daemonizes or writes a PID file. Fails fast if another
`relay start` holds the lock, if Redis or Docker is unreachable, or if a required
variable or startup verification fails (e.g. a missing `NETWORKS` network or a
taken metrics/webhook port).

## relay health

Pings Redis and the Docker daemon and exits `0` when both are reachable, `1`
otherwise. Used as the container healthcheck.

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
Retries:             82
DLQ entries:         4
Pending entries:     17
Oldest pending age:  2m14s
Updated:             10s ago
```

Reads the persisted snapshot only; works when the runtime is down. A fresh
database renders zeroes with `Updated: never`. Backlog gauges are global.

```
relay stats reset
```

Zeroes the cumulative global and per-function totals in place. With a running
worker it resets through the worker socket (so the in-memory source resets too
under the flush lock); otherwise directly in the database. Backlog gauges are
not reset.

## relay function

`ls` and `inspect` read the local state database only — no Redis, no Docker, no
`/functions`.

```
relay function ls
```

```
NAME                  RUNTIME      STATUS    UPDATED
user-events-python    python3.14   ready     12s ago
welcome-email-node    node24       ready     12s ago
```

`UPDATED` is `prepared_at` (else `updated_at`) as a relative age; rows are sorted
by name.

```
relay function inspect user-events-python
```

Shows name/runtime/status/image/fingerprint/prepared/last-reconcile/last-error,
per-container `Resources`, per-function stats, `Events`, `Schedules`, `Services`,
`Environment` (names only, values redacted), `Secrets` (references only), and a
`Runtime pool` section. The live pool gauges (capacity, containers, busy, idle,
starting) are resolved live from an in-process provider or the worker socket;
without a reachable worker they render `unknown`, while the cumulative
warm/cold/discarded counters always come from the persisted snapshot.

### relay function invoke

Runs a function's matching event handlers **synchronously on the running
worker's live runtime pool**, without publishing to the stream:

```sh
relay function invoke user-events-python --event '{"event_name":"INSERT"}'
relay function invoke user-events-python --file event.json
echo '{"event_name":"INSERT"}' | relay function invoke user-events-python
```

`--event` and `--file` are mutually exclusive; with neither, the event is read
from stdin when stdin is not an interactive terminal. The payload must be a JSON
**object** (arrays, scalars, and `null` are rejected). Output is one of:

```
No matching handlers
Invoked 1 handler
Invoked N handlers
```

Unlike stream consumption, a manual invocation is **not** part of the
at-least-once lifecycle: it never touches Redis, never claims event
classification, never schedules a retry, and never writes to the DLQ. There is
no offline fallback — a worker must be running.

## relay dlq

```
relay dlq ls
```

```
ID                     ORIGINAL               FUNCTION             HANDLER                ATTEMPTS  AGE
1757...-0              events/1757...-0        welcome-email-node   handler.handler        5         2m ago
```

`ls` lists entries in stream order (ID, source message, failed function/handler,
handler attempts, age). `inspect ID` shows one entry's fields plus the original
event JSON pretty-printed; a non-JSON (malformed placeholder) event is shown
verbatim. `rm ID` deletes one entry.

```
relay dlq replay ID
```

Re-executes that entry's exact recorded function/handler once on the running
worker and deletes the entry **only on success**; the entry is kept on any
failure (removed handler, failed handler, or unavailable worker). A
malformed-message placeholder has no handler to re-execute and is not replayable.

These commands need Redis. The DLQ stream is `relay:<REDIS_STREAM>:dlq`.

## relay secret

```
relay secret ls                      # NAME column, sorted
relay secret set database-url        # read hidden from the terminal, or all of stdin
relay secret rm database-url         # a missing secret is an error
```

`secret set NAME` never accepts a value as a positional argument and never
prints it. When stdin is not a terminal it reads all of stdin:

```sh
printf 'value' | relay secret set database-url
```

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
  `/functions` to match the repository/path (removing directories not in the
  source). This is the only command that writes `/functions`.
- `status` shows the configured source, key/checkout existence, resolved commit,
  and last sync time. Nothing configured prints a message and exits `0`.
- `remove` deletes the persisted config and checkout, leaving `/functions` and
  the SSH key untouched. It prompts with a default of **No**; pass `-y`/`--yes`
  for automation.

See [operations.md](operations.md) for the webhook, SSH/TOFU behavior, and the
on-disk layout.
