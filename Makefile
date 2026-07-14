GO ?= go

.PHONY: build migrate-postgres run-ingest run-query run-processor fmt test

build:
	$(GO) build ./...

migrate-postgres:
	$(GO) run ./cmd/postgres-migrate

run-ingest:
	$(GO) run ./cmd/ingest-api

run-query:
	$(GO) run ./cmd/query-api

run-processor:
	$(GO) run ./cmd/processor

fmt:
	$(GO) fmt ./...

test:
	$(GO) test ./...
