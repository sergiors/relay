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
```

Run `relay <command> --help` for flags. `relay apps` is an alias for
`relay app ls`.

## Command requirements

| Command                              | Requirements                                    |
| ------------------------------------ | ----------------------------------------------- |
| `start`                              | Redis, PostgreSQL, Docker, configured networks  |
| `health`, `app invoke`, `dlq replay` | Running worker                                  |
| `stats`, `app ls`                    | PostgreSQL (`DATABASE_URL`)                     |
| `app inspect`                        | PostgreSQL; Docker optional for provenance      |
| `runtime ls`, `runtime inspect`      | PostgreSQL (`DATABASE_URL`)                     |
| `runtime check`, `runtime update`    | PostgreSQL and Docker Engine (Distribution API) |
| `dlq ls`, `dlq inspect`, `dlq rm`    | PostgreSQL (`DATABASE_URL`)                     |
| `secret ls`                          | Active secret provider and its configuration    |
| `secret set`, `secret rm`            | Local provider only                             |
| `git *`                              | Local Git configuration and `/apps`             |

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

## relay runtime

```sh
relay runtime ls
relay runtime inspect python3.14
relay runtime check [python3.14]
relay runtime update python3.14
relay runtime update --all
```

Managed runtimes build from their configured base tag unless the operator pins
that tag to a registry digest. `ls` and `inspect` read only PostgreSQL; they
never contact Docker or a registry. `check` reads PostgreSQL and resolves the
current registry digest through the Docker Engine Distribution API without
writing anything, pulling, building, or restarting. `update` resolves the
runtime's exact configured base tag through the same API and persists the
multi-platform index digest.

- `ls` lists every runtime this Relay binary supports, its configured base
  image, whether the runtime is `pinned`, `unpinned`, or `invalid`, the
  selection generation and time, and the persisted digest.
- `inspect` shows one runtime's engine, base image, target build platform,
  source image, digest, generation, selection time, and effective image
  reference (`base@digest` when pinned, the base tag when unpinned). A runtime
  with no row is reported as unpinned.
- `check` validates the persisted binding and compares it with the current
  registry digest. It displays the selected digest (or `unpinned`), the remote
  digest, and a status: `matching`, `different`, `available` (no selection and
  the remote digest is available), `unpinned` (no selection and the registry
  could not be checked), `invalid` (a stored row a worker would reject), or
  `failed` (a pinned runtime whose registry lookup failed). It continues across
  every runtime in all-mode, shows per-runtime registry errors, and exits
  non-zero if any runtime failed. It never writes PostgreSQL.
- `update RUNTIME` resolves only that runtime's configured base tag. It requires
  an OCI image index or Docker manifest list and rejects a missing, unknown, or
  platform-specific manifest. When the Engine reports platforms, the index must
  cover more than one concrete platform. The command compare-and-swaps the
  persisted row; if the current digest is already selected, its generation and
  timestamp do not change. A concurrent change is reported as a conflict rather
  than overwritten.
- `update --all` processes every runtime independently, continues after
  failures, and exits non-zero if any runtime failed. Both update forms report
  the previous identity (`unpinned` or digest) and the newly resolved digest.

A persisted selection is not an application rollout. Running workers refresh
the selection once per periodic reconciliation pass; only the runtime-backed
apps using that runtime then reprepare. Neither `check` nor `update` pulls or
builds an image, restarts the worker, restarts apps, or deploys anything. See
[Operations](operations.md#runtime-image-selection-state).

Both `check` and `update` use the Docker Engine API, so the Engine (or an
externally managed socket proxy) must allow the Distribution endpoint. Registry
credentials come from the Docker client configuration (`DOCKER_CONFIG` or
`$HOME/.docker/config.json`), including `auths`, `identitytoken`, `credHelpers`,
and `credsStore`. Public Docker Hub works anonymously. In a container, mount the
Docker client configuration and set `DOCKER_CONFIG`; Relay does not read
credentials from the daemon. Credential values are never printed.

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
execution. Failed replays leave it available. Replay is at least once, so a
handler may run again if execution completes after a client disconnect or lease
expiry. Keep handlers idempotent.

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
  secret name.
- `sync` updates `/apps` to match the configured repository and path, removing
  apps no longer present. Review the source and path before syncing.
- `status` reports the source and last sync.
- `remove` removes the saved Git source and checkout, but leaves `/apps` and the
  deploy key in place. It prompts for confirmation; use `-y` for automation.

Git synchronization is manual unless a webhook is configured. See
[Operations](operations.md#git-managed-apps) for synchronization behavior and
host-key verification.
