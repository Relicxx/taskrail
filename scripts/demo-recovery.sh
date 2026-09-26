#!/usr/bin/env bash
set -euo pipefail
: "${API_KEY:?set API_KEY}"
base="${BASE_URL:-http://localhost:8080}"
key="$(date +%s)-$RANDOM"
id=$(curl --fail --silent -X POST "$base/v1/tasks" -H "X-API-Key: $API_KEY" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d '{"type":"demo.wait","payload":{"duration_ms":10000}}' | jq -er .id)
deadline=$((SECONDS+90)); service=''
cleanup() { if [[ -n "$service" ]]; then docker compose start "$service" >/dev/null; fi; }
trap cleanup EXIT
while [[ -z "$service" ]]; do
  (( SECONDS < deadline )) || { echo 'Timed out waiting for running task' >&2; exit 1; }
  service=$(curl --fail --silent "$base/v1/tasks/$id/attempts" -H "X-API-Key: $API_KEY" | jq -r '.items[] | select(.outcome=="running") | .worker_id' | tail -n 1)
  sleep .2
done
[[ "$service" == worker-a || "$service" == worker-b ]] || { echo 'Unknown worker' >&2; exit 1; }
docker compose kill -s SIGKILL "$service"
while true; do
  (( SECONDS < deadline )) || { echo 'Recovery deadline exceeded' >&2; exit 1; }
  state=$(curl --fail --silent "$base/v1/tasks/$id" -H "X-API-Key: $API_KEY" | jq -r .state)
  [[ "$state" == succeeded ]] && break
  [[ "$state" != failed && "$state" != canceled ]] || exit 1
  sleep .3
done
curl --fail --silent "$base/v1/tasks/$id/attempts" -H "X-API-Key: $API_KEY" | jq -e '.items | length >= 2 and any(.[]; .outcome=="lease_expired")' >/dev/null
echo "Recovery verified: $id"
