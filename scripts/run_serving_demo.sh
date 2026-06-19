#!/usr/bin/env bash
set -euo pipefail

event_count="${1:-210}"

wait_for_doris() {
  local retries=60
  until docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e 'SELECT 1;' >/dev/null 2>&1; do
    retries=$((retries - 1))
    if (( retries == 0 )); then
      echo "Doris FE did not become ready in time." >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_grafana() {
  local retries=60
  until curl -fsS http://localhost:3001/api/health >/dev/null 2>&1; do
    retries=$((retries - 1))
    if (( retries == 0 )); then
      echo "Grafana health endpoint did not become ready in time." >&2
      return 1
    fi
    sleep 2
  done
}

docker compose up -d doris-fe doris-be doris-init
uv run python -m scripts.build_dashboard_demo_datasets --llm-count "${event_count}"

wait_for_doris

docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/create_dashboard_doris_tables.sql
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dim/dim_model_df.parquet --table dim_model_df
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet --table dws_ai_llm_feature_request_1d
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dwd/dwd_ai_compliance_access_audit_di/events.parquet --table dwd_ai_compliance_access_audit_di
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dwd/dwd_ai_compliance_data_retention_di/events.parquet --table dwd_ai_compliance_data_retention_di
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dws/dws_ai_agent_orchestration_handoff_1d.parquet --table dws_ai_agent_orchestration_handoff_1d
uv run python -m scripts.load_dws_metrics_to_doris --input data/warehouse/dws/dws_ai_platform_component_health_1d.parquet --table dws_ai_platform_component_health_1d

docker compose up -d superset-metadata superset-redis superset grafana
scripts/init_superset.sh
wait_for_grafana

scripts/check_pipeline_health.sh --dashboard-only

printf '\nDashboard query preview:\n'
sed -n '1,120p' sql/doris_dashboard_queries.sql

printf '\nDashboards available at:\n'
printf '  Superset: http://localhost:8088 (admin/admin)\n'
printf '  Grafana:  http://localhost:3001 (admin/admin)\n'
