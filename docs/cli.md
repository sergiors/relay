# CLI

`relay start` runs the worker in the foreground. Other commands inspect or
administer a running Relay deployment; they do not start the worker.

```text
relay start
relay health
relay stats [reset]
relay app <ls|inspect|invoke>
relay apps
relay runtime <ls|inspect|check|update>
relay dlq <ls|inspect|replay|rm>
relay secret <ls|set|rm>
relay git <keygen|set|sync|status|remove>
relay deploy [status|rollback <sha>|pin|unpin]
```

Run `relay <command> --help` for flags. `relay apps` is an alias for
`relay app ls`.

## relay start

Runs the worker until SIGINT or SIGTERM. It fails if required configuration,
Redis, PostgreSQL, Docker, or a configured network is unavailable.

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

## relay runtime

```sh
relay runtime ls
relay runtime inspect python3.14
relay runtime check [python3.14]
relay runtime update python3.14
relay runtime update --all
```

`ls` and `inspect` show the local runtime image selection without contacting a
registry. `check` compares the saved selection with the current image and makes
no changes. `update` pins the runtime's configured base image to its current
multi-platform registry digest; it does not pull or build an image. `check`
without a runtime checks all supported runtimes; `update --all` updates them.
Both continue after individual failures and report unsuccessful results.

Changing a selection is not an app deployment. Running workers use it during a
later reconciliation and reprepare apps that use that runtime; apps continue
using their current image until then. See
[Operations](operations.md#managed-runtime-images).

`check` and `update` require Docker Engine access and registry credentials. For
private registries, make the Docker client configuration available to Relay;
when it is not in the container user's default location, set `DOCKER_CONFIG`.
A Docker socket proxy must permit registry image checks as well as Relay's
container operations. Public Docker Hub images can be checked anonymously.

## relay app

```sh
relay app ls
relay app inspect my-app
```

These commands read PostgreSQL state. `inspect` also performs one best-effort,
read-only Docker image inspect when the app has an active image, to show the
base image that image was built from. Unavailable provenance is reported
honestly rather than falling back to current configuration. `inspect` shows app
status, handlers, schedules, services, environment variable names, secret
references, and runtime pool information. Some live pool values are unavailable
when the worker is not running. `relay apps` is equivalent to `relay app ls`.

### relay app invoke

Runs matching handlers synchronously on the running worker without publishing a
stream message:

```sh
# MongoDB CDC event; use tableName instead for PostgreSQL CDC.
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
relay dlq inspect ID
relay dlq replay ID
relay dlq rm ID
```

`ls` lists dead letters. `inspect` shows metadata and the original message value;
replay uses the separately stored logical handler payload. A malformed or
oversized message has no replay payload and cannot be replayed.

`replay` requires a running worker and removes an entry only after successful
execution. Failed replays leave it available. Replay is at least once: an
uncertain result or client disconnect may occur after the handler ran. Keep
handlers idempotent.

`rm` removes only settled entries. An entry is settled after its source message
is acknowledged; unsettled entries are retained for recovery. Entry IDs are the
full 64-character lowercase hexadecimal value shown by `relay dlq ls`; `inspect`,
`replay`, and `rm` require that exact ID. See [Events](events.md#dead-letter-queue)
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

`secret ls` lists names from the single active provider: local files by default,
or Infisical when configured. It does not display secret values. `secret set` and
`secret rm` operate only on local files under `/var/lib/relay/secrets` and fail
when Infisical is active. Manage Infisical secrets in Infisical. Relay uses only
one provider at a time and does not fall back between providers. Secret values
are never accepted as positional arguments or printed. See
[Installation](installation.md#infisical-provider) for Infisical deployment
guidance and [Configuration](configuration.md) for provider settings.

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
  secret name. This only saves the source; run `relay git sync` to deploy it.
- `sync` deploys the latest revision from the configured ref to this node's
  `/apps`, removing apps no longer present in the source. Review the source and
  path before syncing. A pinned deployment blocks sync; run
  `relay deploy unpin` first. If another update wins during a sync, run it again.
- `status` reports the source and last sync.
- `remove` removes the saved Git source and checkout, but leaves `/apps` and the
  deploy key in place. It prompts for confirmation; use `-y` for automation.

Git synchronization is manual unless a webhook is configured. See
[Operations](operations.md#git-managed-apps) for webhook setup, host-key
verification, and multi-node behavior.

## relay deploy

```sh
relay deploy
relay deploy status
relay deploy rollback <sha>
relay deploy pin
relay deploy unpin
```

`relay deploy` is an alias for `relay deploy status`. `rollback` selects a commit
from the configured repository and pins it. `pin` blocks `git sync` and webhook
updates; `unpin` allows future updates but does not immediately change the
current revision. Status shows the desired generation and commit, pin state, and
each node's current revision and app status. A node that is still updating keeps
its previous apps active. See [Operations](operations.md#git-managed-apps).
