#!/usr/bin/env bash
set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
PRODUCER_URL="${PRODUCER_URL:-http://localhost:8000}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

mkdir -p artifacts/ci artifacts/load

until curl -fsS "${PROMETHEUS_URL}/-/ready" >/dev/null 2>&1; do
  sleep 5
done

wait_for_completed_service() {
  local service_name="$1"
  local container_id=""

  until container_id="$(docker compose ps -a -q "${service_name}")" && [ -n "${container_id}" ]; do
    sleep 5
  done

  until [ "$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "${container_id}")" = "exited 0" ]; do
    sleep 5
  done
}

wait_for_completed_service schema-registry-bootstrap
wait_for_completed_service kafka-init

until curl -fsS "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; do
  sleep 5
done

until curl -fsS "${PRODUCER_URL}/health" >/dev/null 2>&1; do
  sleep 5
done

warmup_payload=$(cat <<'JSON'
{"user_id":"warmup-user","movie_id":"warmup-movie","event_type":"VIEW_STARTED","timestamp":"2026-05-29T00:00:00Z","device_type":"DESKTOP","session_id":"warmup-session","progress_seconds":0}
JSON
)

curl -fsS \
  -X POST "${PRODUCER_URL}/events" \
  -H "Content-Type: application/json" \
  -d "${warmup_payload}" \
  >/dev/null

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

docker compose --profile test run --rm load-test 2>&1 | tee artifacts/load/k6.log

wait "${collector_pid}"

python3 scripts/ci/check_prometheus_sli.py \
  --prometheus-url "${PROMETHEUS_URL}" \
  --output artifacts/ci/prometheus-sli.json
