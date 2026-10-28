# Development

Relay requires Go 1.27 to build the binary:

```sh
go build -o relay ./cmd
```

To run the binary against your own Redis, PostgreSQL, and Docker Engine:

```sh
REDIS_URI=localhost:6379 \
REDIS_STREAM=events \
REDIS_GROUP=relay \
WORKER_ID=relay-1 \
DATABASE_URL='postgres://relay:relay@localhost:5432/relay?sslmode=disable' \
./relay start
```

Relay reads apps from the fixed `/apps` root. `WORKER_ID` is the required,
stable worker identity used as the Redis consumer name, `relay.hostname`
container label, and stats owner. `DATABASE_URL` is required and points to the
shared state database. See [operations](operations.md#state-database) and
[configuration](configuration.md) for details.

Run formatting, static analysis, and tests with the repository's Makefile:

```sh
make fmt
make vet
make test-unit
make test-unit-race
make test-integration
make test-integration-race
make test-all
make test-race
```

`make test-unit` runs `go test ./...`; `make test-unit-race` runs the same unit
suite with the race detector. Integration targets use the `integration` build
tag and a 20-minute timeout. `make test-all` runs unit and integration tests,
and `make test-race` runs both suites with the race detector. Formatting uses
`go fmt ./...`, and vet runs `go vet ./...`.

Unit tests require only Go. Integration tests require PostgreSQL
(`TEST_DATABASE_URL`, defaulting to the CI DSN), Redis (`REDIS_TEST_ADDR`,
defaulting to `localhost:6379`), Docker, Node 24, Python 3.14, and Git. The
integration suite fails if a required dependency is unavailable.

See the repository's [`AGENTS.md`](../AGENTS.md) for additional development
conventions and focused test guidance.
