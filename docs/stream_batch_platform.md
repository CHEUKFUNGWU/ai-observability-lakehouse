# Stream-Batch Platform Design

## 1. Goal

This project is being upgraded from a Spark-only batch lakehouse into a stream-batch platform for AI observability.

The target architecture is:

```text
Application / Agent Runtime / Hermes / DeepSeek
        |
        v
Operational Source Tables
        |
        v
Flink CDC
        |
        v
Flink SQL
        |
        v
Paimon ODS / DWD / ADS Tables
        |
        +------------------+
        |                  |
        v                  v
Spark Batch Backfill   ClickHouse / Dashboard
```

## 2. Engine Responsibilities

| Component | Responsibility |
|---|---|
| Flink CDC | Capture source-table changes from operational systems |
| Flink SQL | Define streaming transformations from source to ODS, DWD and ADS |
| Paimon | Store lakehouse tables that support streaming writes and batch reads |
| Spark | Batch backfill, offline correction, large-scale historical recomputation |
| ClickHouse | Low-latency serving layer for dashboard queries |

## 3. Layering

The generator, live collector and Hermes parser are sources or source adapters. They are not ODS.

```text
Source
  -> CDC / raw landing
  -> ODS Paimon tables
  -> DWD Paimon tables
  -> ADS Paimon tables
  -> ClickHouse serving tables
```

## 4. First Flink/Paimon Scope

The first streaming scope focuses on LLM request observability because it already has a mature batch model:

```text
postgres.public.llm_request_events
  -> Flink CDC source table
  -> paimon_lake.ods.llm_request_events
  -> paimon_lake.dwd.llm_request_events
  -> paimon_lake.ads.llm_feature_daily_metrics
```

Agent and Hermes tool-call streams can follow the same pattern:

```text
postgres.public.agent_run_events
postgres.public.agent_span_events
postgres.public.agent_tool_call_events
  -> Flink CDC source tables
  -> Paimon ODS
  -> Paimon DWD
  -> Paimon ADS
```

## 5. Why Paimon Here

Paimon is the lakehouse storage layer for the stream-batch path because it supports:

- Streaming writes from Flink
- Batch reads from Spark
- Primary-key tables for CDC upserts
- Partitioned analytical tables for ADS metrics
- A unified table abstraction for both streaming and batch jobs

## 6. Spark Compatibility

Spark remains useful after Flink is introduced. Its role changes from the only transformation engine to the batch/offline engine:

- Recompute historical DWD/ADS tables
- Backfill late source ranges
- Validate Flink/Paimon outputs against existing batch Parquet outputs
- Export ADS metrics into ClickHouse when needed

## 7. Initial SQL Assets

The Flink SQL assets are stored under:

```text
flink/sql/
```

Execution order:

```text
00_catalogs.sql
01_source_postgres_cdc.sql
02_ods_paimon_tables.sql
03_dwd_paimon_tables.sql
04_ads_paimon_tables.sql
10_ingest_ods_from_cdc.sql
20_build_dwd_from_ods.sql
30_build_ads_from_dwd.sql
```

The local operational source schema is stored under:

```text
sql/source_postgres_schema.sql
```

## 8. Local Flink Runtime Notes

The Docker runtime follows the Apache Flink standalone session-cluster pattern:

- `flink-jobmanager` and `flink-taskmanager` are the long-running runtime services.
- `flink-sql-client` is a short-lived tool service started with `docker compose run --rm` to submit SQL.
- `scripts/prepare_flink_warehouse.sh` prepares the shared Paimon warehouse directories and grants write permission to the `flink` runtime user.

The local TaskManager uses four slots so the ODS, DWD, and ADS streaming jobs can remain running while batch verification queries read Paimon snapshots.

For the local Flink ADS MVP, `p95_latency_ms` is populated with `MAX(latency_ms)` as a conservative streaming proxy because Flink 1.20 SQL does not support `PERCENTILE_CONT` as a streaming aggregate. Spark and ClickHouse remain the better places for exact or approximate percentile reporting.
