GO ?= go
GOFMT ?= gofmt

.PHONY: build migrate-postgres run-ingest run-query run-processor fmt fmt-check test integration-test e2e-test functional-test validate-repository distributed-integration ci

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

fmt-check:
	@unformatted="$$(find . \
		-path './.git' -prune -o \
		-path './.gomodcache' -prune -o \
		-path './vendor' -prune -o \
		-name '*.go' -print0 | xargs -0 $(GOFMT) -l)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted Go files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

test:
	$(GO) test ./...

integration-test:
	$(GO) test ./internal/processor -run TestIngestQueueProcessorFlow -count=1

e2e-test:
	$(GO) test ./internal/processor -run TestEndToEndIngestProcessQueryPipeline -count=1

functional-test: integration-test e2e-test

validate-repository:
	bash .github/scripts/validate-repository.sh

distributed-integration:
	bash .github/scripts/distributed-clickhouse-integration.sh

ci: fmt-check test build functional-test validate-repository
