#!/usr/bin/env sh
set -eu

secret_file=${1:-deployments/kubernetes/secrets.env}
namespace=${LOGAGG_NAMESPACE:-logagg}

if [ ! -f "$secret_file" ]; then
  echo "secret file not found: $secret_file" >&2
  echo "copy deployments/kubernetes/secrets.env.example and replace its values" >&2
  exit 1
fi

kubectl get namespace "$namespace" >/dev/null 2>&1 || kubectl create namespace "$namespace"
kubectl -n "$namespace" create secret generic logagg-secrets \
  --from-env-file="$secret_file" \
  --dry-run=client \
  -o yaml |
  kubectl apply -f -
