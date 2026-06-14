# Flink SQL + Paimon Jobs

This directory contains the first stream-batch SQL assets for the AI observability platform.

## Flow

```text
Postgres operational source tables
  -> Flink CDC source tables
  -> Kafka ODS table
  -> Paimon DWD tables
  -> Paimon DWS tables
```

## SQL Execution Order

```text
sql/00_catalogs.sql
sql/01_source_postgres_cdc.sql
sql/02_ods_kafka_tables.sql
sql/03_dwd_paimon_tables.sql
sql/04_dws_paimon_tables.sql
sql/10_ingest_ods_to_kafka.sql
sql/20_build_dwd_from_kafka_ods.sql
sql/30_build_dws_from_dwd.sql
```

## Local Runtime

Build the Flink image with Paimon, Postgres CDC, and Kafka connectors:

```bash
docker compose build flink-jobmanager
```

Start the source database and Flink cluster:

```bash
docker compose up -d postgres kafka flink-jobmanager flink-taskmanager
scripts/prepare_flink_warehouse.sh
scripts/create_kafka_topics.sh
```

Load mock LLM events into the Postgres operational source table:

```bash
uv run python -m scripts.generate_mock_llm_logs --count 100 --seed 42
scripts/load_llm_jsonl_to_postgres_source.sh data/raw/mock_llm_requests/events.jsonl
```

Open the Flink Web UI:

```text
http://localhost:8081
```

Run one SQL file through the Flink SQL Client:

```bash
scripts/run_flink_sql_file.sh flink/sql/00_catalogs.sql
```

The runner starts the dedicated `flink-sql-client` service with `docker compose run --rm`, matching the Apache Flink session-cluster deployment pattern. The JobManager and TaskManager containers remain the long-running cluster; SQL Client is only used to submit SQL statements.

The local TaskManager is configured with four slots so the Kafka ingestion, DWD, and DWS streaming jobs can keep running while batch verification queries use the remaining capacity.

The Flink DWS SQL stores `max_latency_ms` as an explicit upper-bound metric and writes `p95_latency_ms = 0` because Flink 1.20 SQL does not support `PERCENTILE_CONT` as a streaming aggregate.

Run the SQL files in the order listed above. Long-running `INSERT INTO ... SELECT ...` statements are streaming jobs and should remain visible in the Flink Web UI.

## Runtime Notes

The SQL uses:

- `postgres-cdc` connector for source capture
- `kafka` connector for the real-time ODS buffer
- `paimon` catalog for lakehouse tables
- primary keys on CDC-backed DWD tables
- partitioned DWS tables for reusable summary metrics

Connector jars are not vendored in this repository. The local Flink image downloads compatible jars during Docker build:

- `paimon-flink-1.20`
- `flink-sql-connector-postgres-cdc`
- `flink-sql-connector-kafka`
