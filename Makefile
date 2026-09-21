.PHONY: test test-unit test-unit-race test-integration test-integration-race \
	test-all test-race fmt vet up down

GOCACHE := /tmp/go-build
GO := go
GOTEST := $(GO) test -v

test: test-unit

test-unit:
	GOCACHE=$(GOCACHE) $(GOTEST) -short ./...

test-integration:
	GOCACHE=$(GOCACHE) $(GOTEST) -tags=integration ./...

test-unit-race:
	GOCACHE=$(GOCACHE) $(GOTEST) -race ./...

test-integration-race:
	GOCACHE=$(GOCACHE) $(GOTEST) -race -tags=integration ./...

test-all: test-unit test-integration

test-race: test-unit-race test-integration-race


fmt:
	$(GO) fmt ./...

vet:
	GOCACHE=$(GOCACHE) $(GO) vet ./...

up:
	docker compose -f compose.dev.yaml up -d --build

down:
	docker compose -f compose.dev.yaml down
