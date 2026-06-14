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
Kafka ODS
        |
        v
Flink SQL
        |
        v
Paimon DWD / DWS Tables
        |
        +------------------+
        |                  |
        v                  v
Spark Batch Backfill   Doris / Dashboard
```

## 2. Engine Responsibilities

| Component | Responsibility |
|---|---|
| Flink CDC | Capture source-table changes from operational systems |
| Kafka | Buffer CDC events, provide replay, and decouple capture from downstream transforms |
| Flink SQL | Define streaming transformations from Kafka ODS to DWD and DWS |
| Paimon | Store lakehouse tables that support streaming writes and batch reads |
| Spark | Batch backfill, offline correction, large-scale historical recomputation |
| Doris | Low-latency serving layer for dashboard queries |

## 3. Layering

The generator, live collector and Hermes parser are sources or source adapters. They are not ODS.

```text
Source
  -> CDC / raw landing
  -> Kafka ODS
  -> DWD Paimon tables
  -> DWS Paimon tables
  -> Doris serving tables
```

## 4. First Flink/Paimon Scope

The first streaming scope focuses on LLM request observability because it already has a mature batch model:

```text
postgres.public.llm_request_events
  -> Flink CDC source table
  -> kafka_ods_llm_request_events
  -> paimon_lake.dwd.llm_request_events
  -> paimon_lake.dws.llm_feature_daily_metrics
```

Agent and Hermes tool-call streams can follow the same pattern:

```text
postgres.public.agent_run_events
postgres.public.agent_span_events
postgres.public.agent_tool_call_events
  -> Flink CDC source tables
  -> Kafka ODS
  -> Paimon DWD
  -> Paimon DWS
```

## 5. Why Paimon Here

Paimon is the lakehouse storage layer for the stream-batch path because it supports:

- Streaming writes from Flink
- Batch reads from Spark
- Primary-key tables for CDC upserts
- Partitioned analytical tables for DWS metrics
- A unified table abstraction for both streaming and batch jobs

## 6. Spark Compatibility

Spark remains useful after Flink is introduced. Its role changes from the only transformation engine to the batch/offline engine:

- Recompute historical DWD/DWS tables
- Backfill late source ranges
- Validate Flink/Paimon outputs against shared-warehouse expectations
- Export or sync DWS metrics into Doris when needed

## 7. Initial SQL Assets

The Flink SQL assets are stored under:

```text
flink/sql/
```

Execution order:

```text
00_catalogs.sql
01_source_postgres_cdc.sql
02_ods_kafka_tables.sql
03_dwd_paimon_tables.sql
04_dws_paimon_tables.sql
10_ingest_ods_to_kafka.sql
20_build_dwd_from_kafka_ods.sql
30_build_dws_from_dwd.sql
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

The local TaskManager uses four slots so the Kafka ingestion, DWD, and DWS streaming jobs can remain running while batch verification queries read Paimon snapshots.

For the local Flink DWS MVP, `max_latency_ms` is populated with `MAX(latency_ms)` as an explicit upper-bound metric because Flink 1.20 SQL does not support `PERCENTILE_CONT` as a streaming aggregate. Spark and Doris remain the better places for exact or approximate percentile reporting.
