# Kubernetes deployment

The manifests deploy `ingest-api`, `processor`, and `query-api` into the
`logagg` namespace. NATS, Postgres, and ClickHouse are external dependencies;
use managed services or deploy them separately before applying Logagg.

## Configuration model

The Go services read environment variables through `internal/config`. The
manifests preserve a strict boundary:

- `logagg-config` contains non-sensitive operational settings such as stream
  names, retry policies, log level, replay mode, and backpressure thresholds.
- `logagg-secrets` contains authenticated connection strings:
  `POSTGRES_DSN`, `NATS_URL`, `CLICKHOUSE_DSN`, and
  `CLICKHOUSE_SHARD_DSNS`.
- Each Deployment supplies its own `SERVICE_NAME` and listen address.

Edit the production overlay for environment-specific non-secret values. Use
immutable image tags or digests at deployment time:

```bash
cd deployments/kubernetes/overlays/production
kustomize edit set image \
  logagg/ingest-api=registry.example/logagg/ingest-api:v1.0.0 \
  logagg/processor=registry.example/logagg/processor:v1.0.0 \
  logagg/query-api=registry.example/logagg/query-api:v1.0.0
kubectl apply -k .
```

Do not commit the resulting overlay edit unless it is the repository's desired
declarative release state.

## Secrets

For a local or development cluster, create the referenced Secret from an
ignored env file:

```bash
cp deployments/kubernetes/secrets.env.example deployments/kubernetes/secrets.env
# Replace every example value.
deployments/kubernetes/create-secret.sh
kubectl apply -k deployments/kubernetes/base
```

The script uses `kubectl create secret --dry-run=client` and applies the result
directly; it does not write rendered Secret YAML to disk. The env file is
ignored by Git. Kubernetes Secrets are only base64 encoded, so enable
encryption at rest and restrict namespace RBAC in every real cluster.

For production, install External Secrets Operator and provide a
`ClusterSecretStore` named `production-secrets`. Its provider must contain:

- `logagg/postgres-dsn`
- `logagg/nats-url`
- `logagg/clickhouse-dsn`
- `logagg/clickhouse-shard-dsns`

Then apply the External Secrets overlay:

```bash
kubectl apply -k deployments/kubernetes/external-secrets
```

The operator creates and rotates the same `logagg-secrets` Secret consumed by
the Deployments, so application manifests do not change between secret
backends.

## Verification

```bash
kubectl -n logagg rollout status deployment/ingest-api
kubectl -n logagg rollout status deployment/processor
kubectl -n logagg rollout status deployment/query-api
kubectl -n logagg port-forward service/query-api 8081:8081
curl -fsS http://localhost:8081/readyz
```

All containers run as an unprivileged user with a read-only root filesystem,
dropped Linux capabilities, resource requests and limits, and liveness and
readiness probes. The ServiceAccount does not mount a Kubernetes API token.
