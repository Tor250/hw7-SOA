#!/usr/bin/env bash
set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

mkdir -p artifacts/ci artifacts/load

until curl -fsS "${PROMETHEUS_URL}/-/ready" >/dev/null 2>&1; do
  sleep 5
done

python3 scripts/ci/collect_prometheus_samples.py \
  --prometheus-url "${PROMETHEUS_URL}" \
  --output artifacts/ci/prometheus-samples.json \
  --interval 5 \
  --duration 45 &

collector_pid=$!

cleanup() {
  kill "${collector_pid}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose --profile test run --rm load-test

wait "${collector_pid}"

python3 scripts/ci/check_prometheus_sli.py \
  --prometheus-url "${PROMETHEUS_URL}" \
  --output artifacts/ci/prometheus-sli.json
