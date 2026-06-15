#!/usr/bin/env bash
set -euo pipefail

skip_serving=false
if [[ "${1:-}" == "--skip-serving" ]]; then
  skip_serving=true
fi

failures=0

pass() {
  printf '[PASS] %s\n' "$1"
}

fail() {
  printf '[FAIL] %s\n' "$1" >&2
  failures=$((failures + 1))
}

if ! docker compose ps --services >/dev/null 2>&1; then
  fail "Docker Compose runtime is not reachable. Start Docker Desktop, then run make infra-light."
  exit 1
fi

require_running_service() {
  local service="$1"
  if docker compose ps --status running --services | grep -qx "${service}"; then
    pass "service ${service} is running"
  else
    fail "service ${service} is not running"
  fi
}

require_running_service postgres
require_running_service kafka
require_running_service flink-jobmanager
require_running_service flink-taskmanager

if [[ "${skip_serving}" == false ]]; then
  require_running_service doris-fe
  require_running_service doris-be
fi

if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list | grep -qx 'ods_llm_request_events'; then
  pass "Kafka topic ods_llm_request_events exists"
else
  fail "Kafka topic ods_llm_request_events is missing"
fi

source_count="$(
  docker compose exec -T postgres psql \
    -U ai_observability \
    -d ai_observability \
    -tAc 'SELECT COUNT(*) FROM public.llm_request_events;' 2>/dev/null \
    | tr -d '[:space:]' || true
)"
if [[ "${source_count}" =~ ^[0-9]+$ ]] && (( source_count > 0 )); then
  pass "Postgres source table has ${source_count} LLM request rows"
else
  fail "Postgres source table has no LLM request rows"
fi

flink_jobs_json="$(curl -fsS http://localhost:8081/jobs/overview 2>/dev/null || true)"
if [[ -z "${flink_jobs_json}" ]]; then
  fail "Flink REST API is not reachable at http://localhost:8081"
else
  if python3 - "${flink_jobs_json}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
jobs = payload.get("jobs", [])
running_names = [job.get("name", "") for job in jobs if job.get("state") == "RUNNING"]
expected = {
    "insert-into_paimon_lake.dwd.llm_request_events": False,
    "insert-into_paimon_lake.dws.llm_feature_daily_metrics": False,
}
for name in running_names:
    for expected_name in expected:
        if expected_name in name:
            expected[expected_name] = True

missing = [name for name, found in expected.items() if not found]
if missing:
    print("\n".join(missing))
    sys.exit(1)
PY
  then
    pass "Flink DWD and DWS streaming jobs are running"
  else
    fail "Flink DWD/DWS streaming jobs are not both running"
  fi
fi

if [[ "${skip_serving}" == false ]]; then
  if docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root \
    -e 'SELECT 1;' >/dev/null 2>&1; then
    pass "Doris FE query endpoint is reachable"
  else
    fail "Doris FE query endpoint is not reachable"
  fi
fi

if (( failures > 0 )); then
  printf '\nPipeline health check failed with %s issue(s).\n' "${failures}" >&2
  exit 1
fi

printf '\nPipeline health check passed.\n'
