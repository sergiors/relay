.PHONY: test test-unit test-unit-race test-integration test-integration-race \
	test-all test-race dev-up dev-down

test: test-unit

test-unit:
	go test ./...

test-unit-race:
	go test -race ./...

dev-up:
	docker compose -f compose.dev.yaml up -d redis

dev-down:
	docker compose -f compose.dev.yaml down

test-integration:
	go test -tags=integration ./...

test-integration-race:
	go test -race -tags=integration ./...

test-all: test-unit test-integration

test-race: test-unit-race test-integration-race
