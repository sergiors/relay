# CLI

`relay start` runs the worker in the foreground. Other commands inspect or
administer a running Relay deployment; they do not start the worker.

```text
relay start
relay health
relay stats [reset]
relay app <ls|inspect|invoke>
relay apps
relay dlq <ls|inspect|replay|rm>
relay secret <ls|set|rm>
relay git <keygen|set|sync|status|remove>
```

Run `relay <command> --help` for flags. `relay apps` is an alias for
`relay app ls`.

## Command requirements

| Command                              | Requirements                                   |
| ------------------------------------ | ---------------------------------------------- |
| `start`                              | Redis, PostgreSQL, Docker, configured networks |
| `health`, `app invoke`, `dlq replay` | Running worker                                 |
| `stats`, `app ls`, `app inspect`     | PostgreSQL (`DATABASE_URL`)                    |
| `dlq ls`, `dlq inspect`, `dlq rm`    | PostgreSQL (`DATABASE_URL`)                    |
| `secret *`                           | Local Relay data directory                     |
| `git *`                              | Local Git configuration and `/apps`            |

`health` and `app invoke` communicate with the worker over its local control
socket. `relay dlq` commands access PostgreSQL; replay also requires the worker.

## relay start

Runs the worker until SIGINT or SIGTERM. It fails if another worker holds the
process lock or if required configuration, Redis, PostgreSQL, Docker, or a
configured network is unavailable.

## relay health

```sh
relay health
```

Reports whether the running worker is ready and its Redis, Docker, and configured
networks are healthy. It exits successfully when healthy and non-zero otherwise.
There is no HTTP health endpoint. See [Operations](operations.md#health-and-statistics).

## relay stats

```sh
relay stats
relay stats reset
```

`stats` displays the latest persisted counters and backlog gauges; it works
without a running worker. The snapshot can lag live metrics by about five
seconds. `stats reset` clears cumulative counters, not backlog gauges or
Prometheus counters.

“Message reclaims” counts stream redeliveries, not handler retry attempts. “DLQ
entries” counts rows written to PostgreSQL, not just invocations that exhausted
their retry budgets. See [Operations](operations.md#health-and-statistics).

## relay app

```sh
relay app ls
relay app inspect my-app
```

These commands read PostgreSQL state. `inspect` shows app status, handlers,
schedules, services, environment variable names, secret references, and runtime
pool information. Some live pool values are unavailable when the worker is not
running. `relay apps` is equivalent to `relay app ls`.

### relay app invoke

Runs matching handlers synchronously on the running worker without publishing a
stream message:

```sh
relay app invoke my-app '{"recordType":"INSERT","collectionName":"users"}'
relay app invoke my-app - < event.json
```

The input must be a JSON object. Use `-` to read from stdin; stdin is never read
implicitly. Manual invocation does not use stream retries or the DLQ. The event
must match the app's patterns to invoke a handler. In Docker, use `-T` to keep
stdin as a pipe:

```sh
docker compose exec -T relay relay app invoke my-app - < event.json
```

## relay dlq

```sh
relay dlq ls
relay dlq inspect ID_OR_PREFIX
relay dlq replay ID_OR_PREFIX
relay dlq rm ID_OR_PREFIX
```

`ls` lists dead letters. `inspect` shows metadata and the original message value;
replay uses the separately stored logical handler payload. A malformed or
oversized message has no replay payload and cannot be replayed.

`replay` requires a running worker and removes an entry only after successful
execution. Failed replays leave it available. Replay is at least once, so a
handler may run again if execution completes after a client disconnect or lease
expiry. Keep handlers idempotent.

`rm` removes only settled entries. An entry is settled after its source message
is acknowledged; unsettled entries are retained for recovery. IDs accept a
unique prefix of at least 10 hexadecimal characters. See [Events](events.md#dead-letter-queue)
and [Operations](operations.md#dlq-controls).

## relay secret

```sh
relay secret ls
relay secret set API_KEY
relay secret rm API_KEY
```

`secret set NAME` prompts with input hidden. To read a value from stdin, use `-`:

```sh
printf '%s' 'value' | relay secret set API_KEY -
docker compose exec -T relay relay secret set API_KEY - < value.txt
```

Secret values are never accepted as positional arguments or printed. They are
stored under `/var/lib/relay/secrets`; templates reference them by name.

## relay git

```sh
relay git keygen
relay git set git@github.com:acme/repo.git [--ref main] [--path apps] [--webhook-secret NAME]
relay git sync
relay git status
relay git remove [-y|--yes]
```

- `keygen` creates an SSH deploy key under `/var/lib/relay/ssh` and prints the
  public key.
- `set` configures an SSH repository, optional ref/path, and optional webhook
  secret name.
- `sync` updates `/apps` to match the configured repository and path, removing
  apps no longer present. Review the source and path before syncing.
- `status` reports the source and last sync.
- `remove` removes the saved Git source and checkout, but leaves `/apps` and the
  deploy key in place. It prompts for confirmation; use `-y` for automation.

Git synchronization is manual unless a webhook is configured. See
[Operations](operations.md#git-managed-apps) for synchronization behavior and
host-key verification.
