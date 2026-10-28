# Self-hosted deployment

Relay runs as a worker connected to Redis, PostgreSQL, and a Docker Engine. It
loads applications from `/apps` and runs their handlers in isolated containers.
This guide covers the repository's production Compose deployment. See
[configuration](configuration.md) for the full settings reference.

## Prerequisites

- Docker Engine and Docker Compose.
- Production Redis and PostgreSQL services reachable by Relay. They are not
  provisioned by `compose.prod.yaml`.
- The production Compose file's required environment and supporting files (see
  [Configuration](#configuration)).
- Compose creates its declared `relay`, `traefik`, and internal `control`
  networks. Any additional network named in `NETWORKS` must exist before
  startup; Relay does not create networks, and a missing configured network is
  fatal at worker startup.

## Configuration

The production Compose file is not self-contained as checked in: its `.env`
file and mounted observability configuration files are absent from this
workspace. Supply and review them before deployment. `docker compose config
--quiet` validates Compose configuration and required environment interpolation;
it does not check that bind-mount source files exist. With short bind-mount
syntax, `up` may create missing host paths as empty files or directories, so
check the sources explicitly before starting.

Create `.env` beside `compose.prod.yaml`. Compose requires `RELAY_IMAGE`,
`WORKER_ID`, `ACME_EMAIL`, `TEMPO_VERSION`, `OTEL_VERSION`, `ALLOY_VERSION`,
`GRAFANA_VERSION`, `GRAFANA_DOMAIN`, `GRAFANA_ADMIN_USER`, and
`GRAFANA_ADMIN_PASSWORD`. The same file must configure Relay's external
dependencies with `REDIS_URI`, `REDIS_STREAM`, `REDIS_GROUP`, and
`DATABASE_URL`. Choose values appropriate to your deployment; do not use
development credentials or an untested Relay image.

`WORKER_ID` must be unique among concurrent workers, stable across restarts,
and at most 128 characters using only ASCII letters, digits, `.`, `_`, and `-`.
The Compose file sets `DOCKER_HOST` to its socket proxy, sets
`NETWORKS=relay,traefik`, and configures Relay's Traefik network, HTTPS
entrypoint, and certificate resolver. Compose creates its declared networks;
Relay verifies the configured workload networks exist and fails startup if one
is missing. Consult [configuration](configuration.md) before changing them.

From the repository root, verify all referenced observability sources are
present and review their contents:

```sh
test -f prometheus.yml && test -f loki.yaml && test -f tempo.yaml && \
  test -f otel-collector.yaml && test -f alloy/config.alloy && \
  test -d grafana/provisioning
```

These config files and directory must be supplied at the referenced paths.

## Start Relay

Only after the `.env`, observability sources, networks, and external services
are ready and the checks above pass, validate and start the stack:

```sh
docker compose -f compose.prod.yaml config --quiet
docker compose -f compose.prod.yaml up -d
```

## Verify

Check container status and Relay readiness:

```sh
docker compose -f compose.prod.yaml ps
docker compose -f compose.prod.yaml exec -T relay relay health
```

On success, `relay health` prints `healthy`. It exits with an error if no worker
is available or the worker is unhealthy. To inspect startup or runtime logs:

```sh
docker compose -f compose.prod.yaml logs -f relay
```

During startup, look for `Worker ready to consume` in the Relay logs.

## Connect Applications (Git workflow)

Relay loads each direct child directory of `/apps` that contains a
`template.yaml`. The production Compose file stores `/apps` in the persistent
`relay-apps` volume. Configure a Git repository as the source of these apps:

```sh
docker compose -f compose.prod.yaml exec -T relay relay git keygen
docker compose -f compose.prod.yaml exec -T relay relay git set git@github.com:OWNER/REPOSITORY.git
docker compose -f compose.prod.yaml exec -T relay relay git sync
docker compose -f compose.prod.yaml exec -T relay relay git status
```

Add the public key printed by `git keygen` to the repository as a read-only
deploy key. `git set` accepts an SSH repository URL; the default ref is
`main`, and `--ref REF` and `--path PATH` can select another ref or app path.
`git sync` updates `/apps` and removes apps no longer present in the source, so
review the repository and selected path before running it.

Relay does not poll Git automatically. Run `git sync` when you want to publish
changes. An optional GitHub push webhook can request synchronization with
`relay git set ... --webhook-secret NAME`, but the production Compose file does
not configure `GIT_WEBHOOK_ADDR` or expose/route the webhook endpoint. It does
not provide automatic external webhook access as configured.

For example, the `welcome-email-node` app uses the `node24` runtime and matches
`INSERT` events for the `users` table. Its handler reads `event.new_image` and
logs a welcome-email message. See [apps](apps.md), [events](events.md), and
[services](services.md) for app behavior and configuration.

## Manage Relay

Run CLI commands inside the Relay container. Choose an app from `relay app ls`;
replace `my-app` below with its actual name. The JSON passed to `app invoke`
must match that app's event pattern, or no handler may match. The sample JSON
below is specifically for `welcome-email-node`; use it only if that app is in
your Git-sourced applications, otherwise supply a matching event for your app.

```sh
docker compose -f compose.prod.yaml exec -T relay relay app ls
docker compose -f compose.prod.yaml exec -T relay relay app inspect my-app
docker compose -f compose.prod.yaml exec -T relay relay app invoke my-app - <<'EOF'
{"event_name":"INSERT","table_name":"users","new_image":{"email":"alice@example.com"}}
EOF
docker compose -f compose.prod.yaml exec -T relay relay stats
docker compose -f compose.prod.yaml exec -T relay relay dlq ls
```

`app ls` and `app inspect` read PostgreSQL state; `stats` reads persisted
statistics. `app invoke` runs a handler synchronously using the running worker
and does not publish to Redis. Event-driven handlers require events to be
published to the configured Redis stream. `health` and `app invoke` require a
live worker.

Set a secret with a hidden interactive prompt, then confirm its name is listed
(secret values are not shown):

```sh
docker compose -f compose.prod.yaml exec -it relay relay secret set API_KEY
docker compose -f compose.prod.yaml exec -T relay relay secret ls
```

For a dead-letter entry, copy its exact ID from `relay dlq ls`. Run
`docker compose -f compose.prod.yaml exec -T relay relay dlq inspect ID` to
inspect it, or `docker compose -f compose.prod.yaml exec -T relay relay dlq replay ID`
to retry it; replace `ID` with the listed entry ID. Replay requires a running
worker and removes the entry only on success. See [CLI](cli.md) for the complete
command reference.

## Observability

The production Compose stack includes Prometheus for metrics, Loki and Alloy
for container logs, Tempo for traces, an OpenTelemetry collector, and Grafana
for viewing telemetry. The required configuration mounts are currently absent
from the repository, so these services cannot be assumed to validate, start,
or have working dashboards until those files are supplied and reviewed. Relay's
`relay stats` is also available through the CLI without Grafana.
Relay's metrics endpoint on port `9090` is not published on a host port; it is
reachable from networks joined by Relay (`runtime`, `edge`, and `control`).
Grafana is routed through Traefik using the configured domain. Its HTTPS
certificate uses HTTP-01, so the domain must resolve publicly to this ingress
and be reachable on ports 80 and 443.

## Updates and Persistence

PostgreSQL initializes its schema automatically but does not migrate it across
Relay releases. Stop the old worker before switching versions. Set `RELAY_IMAGE`
to a tested release tag or digest, then update only the Relay service:

```sh
docker compose -f compose.prod.yaml pull relay
docker compose -f compose.prod.yaml up -d --no-deps relay
docker compose -f compose.prod.yaml exec -T relay relay health
```

The `relay-data` volume contains Relay secrets, Git key/configuration, and
checkouts; `relay-apps` contains synchronized app files. Preserve both across
restarts and upgrades. Redis and PostgreSQL persistence is managed by their
respective services. Back up these stores according to your recovery
requirements; do not remove volumes as part of routine updates. See
[operations](operations.md) for lifecycle and recovery details.

## Troubleshooting

- If `compose ... config --quiet` fails, check the required `.env` values and
  Compose syntax. Check bind-mount sources separately with the presence checks
  in [Configuration](#configuration); `config --quiet` does not verify them.
- If the Relay container does not become healthy, check its logs and verify
  `WORKER_ID`, Docker Engine access, and Redis/PostgreSQL connectivity.
- If the worker fails at startup, verify every network configured in
  `NETWORKS` exists; a missing network is fatal.
- If apps are missing or stale, check `relay git status` and run `relay git sync`
  after confirming the configured source and path.
- If application commands cannot read status, verify PostgreSQL availability;
  commands that execute work also need a live worker.

## Development Compose

`compose.yaml` is a standalone local quick-start that runs the published Relay
image, bind-mounts `./apps` at `/apps`, and provisions local Redis, PostgreSQL,
a Docker socket proxy, and Traefik. Place an app under `./apps` to run it. This
development stack is not a substitute for the production deployment described
above, which uses externally managed dependencies and production configuration.
