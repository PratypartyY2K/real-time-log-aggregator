GO ?= go
GOFMT ?= gofmt
DOCKER ?= docker
IMAGE_PREFIX ?= logagg

.PHONY: build cli docker-ingest docker-processor docker-query docker-images k8s-validate migrate-postgres run-ingest run-query run-processor fmt fmt-check test integration-test e2e-test functional-test performance-test query-benchmarks chaos-test validate-repository distributed-integration ci

build:
	$(GO) build ./...

cli:
	$(GO) build -o bin/logagg ./cmd/logagg

docker-ingest:
	$(DOCKER) build --build-arg SERVICE=ingest-api -t $(IMAGE_PREFIX)/ingest-api:local .

docker-processor:
	$(DOCKER) build --build-arg SERVICE=processor -t $(IMAGE_PREFIX)/processor:local .

docker-query:
	$(DOCKER) build --build-arg SERVICE=query-api -t $(IMAGE_PREFIX)/query-api:local .

docker-images: docker-ingest docker-processor docker-query

k8s-validate:
	kubectl kustomize deployments/kubernetes/base >/dev/null
	kubectl kustomize deployments/kubernetes/overlays/production >/dev/null
	kubectl kustomize deployments/kubernetes/external-secrets >/dev/null

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

performance-test:
	$(GO) test ./cmd/loadtest -count=1

query-benchmarks:
	$(GO) test ./internal/queryapi -run '^$$' -bench 'Benchmark(QueryLogsHandler|BuildLogsQuery)$$' -benchmem

chaos-test:
	$(GO) test ./internal/stream -run '^TestChaos' -count=1

validate-repository:
	bash .github/scripts/validate-repository.sh

distributed-integration:
	bash .github/scripts/distributed-clickhouse-integration.sh

ci: fmt-check test build functional-test validate-repository
