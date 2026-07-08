GO ?= go

.PHONY: build run-ingest run-query run-processor fmt test

build:
	$(GO) build ./...

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
