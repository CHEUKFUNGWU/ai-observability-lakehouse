#!/usr/bin/env bash
set -euo pipefail

event_count="${1:-100}"

docker compose up -d postgres kafka flink-jobmanager flink-taskmanager
scripts/prepare_flink_warehouse.sh
scripts/create_kafka_topics.sh

uv run python -m scripts.generate_mock_llm_logs --count "${event_count}" --seed 42
uv run python -m scripts.generate_mock_agent_logs --count "${event_count}" --seed 42
scripts/load_llm_jsonl_to_postgres_source.sh data/raw/mock_llm_requests/events.jsonl

scripts/run_flink_sql_sequence.sh \
  flink/sql/00_catalogs.sql \
  flink/sql/01_source_postgres_cdc.sql \
  flink/sql/02_ods_kafka_tables.sql \
  flink/sql/03_dwd_paimon_tables.sql \
  flink/sql/04_dws_paimon_tables.sql \
  flink/sql/10_ingest_ods_to_kafka.sql \
  flink/sql/20_build_dwd_from_kafka_ods.sql \
  flink/sql/30_build_dws_from_dwd.sql

scripts/check_pipeline_health.sh --skip-serving
