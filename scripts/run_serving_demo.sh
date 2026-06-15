#!/usr/bin/env bash
set -euo pipefail

docker compose up -d doris-fe doris-be doris-init

uv run python -m scripts.spark_build_ads_cost_anomaly
uv run python -m scripts.spark_build_ads_prompt_version_metrics
uv run python -m scripts.spark_build_dim_model

docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/create_doris_tables.sql
docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_create_paimon_catalog.sql
docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_sync_paimon_dws.sql
uv run python -m scripts.load_dws_metrics_to_doris

scripts/check_pipeline_health.sh

printf '\nDashboard query preview:\n'
sed -n '1,120p' sql/doris_dashboard_queries.sql
