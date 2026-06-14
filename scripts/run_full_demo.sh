#!/usr/bin/env bash
set -euo pipefail

docker compose up -d postgres kafka flink-jobmanager flink-taskmanager doris-fe doris-be doris-init
scripts/prepare_flink_warehouse.sh
scripts/create_kafka_topics.sh

uv run python -m scripts.generate_mock_llm_logs --count "${1:-100}" --seed 42
uv run python -m scripts.generate_mock_agent_logs --count "${1:-100}" --seed 42
scripts/load_llm_jsonl_to_postgres_source.sh data/raw/mock_llm_requests/events.jsonl

scripts/run_flink_sql_sequence.sh \
  flink/sql/00_catalogs.sql \
  flink/sql/01_source_postgres_cdc.sql \
  flink/sql/02_ods_kafka_tables.sql \
  flink/sql/03_dwd_paimon_tables.sql \
  flink/sql/04_ads_paimon_tables.sql \
  flink/sql/10_ingest_ods_to_kafka.sql \
  flink/sql/20_build_dwd_from_kafka_ods.sql \
  flink/sql/30_build_ads_from_dwd.sql

uv run python -m scripts.run_local_batch_pipeline --count "${1:-100}" --seed 42
uv run python -m scripts.spark_build_ads_cost_anomaly
uv run python -m scripts.spark_build_ads_prompt_version_metrics
uv run python -m scripts.spark_build_dim_model
docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root < sql/create_doris_tables.sql
uv run python -m scripts.load_ads_metrics_to_doris
printf '\nDashboard query preview:\n'
sed -n '1,120p' sql/doris_dashboard_queries.sql
