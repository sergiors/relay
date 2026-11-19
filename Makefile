.PHONY: test test-unit test-unit-race test-integration test-integration-race \
	test-all test-race fmt vet up down ci-local ci-local-unit \
	ci-local-integration ci-local-down

GOCACHE := /tmp/go-build
GO := go
GOTEST := $(GO) test -v
CI_TARGET ?= all

test: test-unit

test-unit:
	GOCACHE=$(GOCACHE) $(GOTEST) ./...

test-integration:
	GOCACHE=$(GOCACHE) $(GOTEST) -timeout 20m -tags=integration ./...

test-unit-race:
	GOCACHE=$(GOCACHE) $(GOTEST) -race ./...

test-integration-race:
	GOCACHE=$(GOCACHE) $(GOTEST) -race -timeout 20m -tags=integration ./...

test-all: test-unit test-integration

test-race: test-unit-race test-integration-race


fmt:
	$(GO) fmt ./...

vet:
	GOCACHE=$(GOCACHE) $(GO) vet ./...

up:
	docker compose up -d --build

down:
	docker compose down

ci-local:
	docker compose -p relay-ci-local -f compose.ci.yaml run --build --rm ci $(CI_TARGET)

ci-local-unit:
	$(MAKE) ci-local CI_TARGET=unit

ci-local-integration:
	$(MAKE) ci-local CI_TARGET=integration

ci-local-down:
	docker compose -p relay-ci-local -f compose.ci.yaml down
