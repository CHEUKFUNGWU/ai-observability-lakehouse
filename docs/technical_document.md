# AI Observability Lakehouse Technical Document

## 1. Current Architecture

The current project implements an AI application observability lakehouse for LLM request events and generic Agent runtime events.

```text
Mock / DeepSeek / Hermes sources
  -> Raw JSONL or Postgres source table
  -> ODS
  -> DWD
  -> ADS
  -> Doris
```

There are two execution paths:

| Path | Role |
|---|---|
| Spark batch | Local batch transform, backfill, testable development path |
| Flink + Kafka + Paimon | Stream-batch lakehouse path from Postgres CDC to Kafka ODS then Paimon DWD/ADS |

Doris is the serving layer for dashboard queries. It has ADS tables for fast aggregated views and DWD detail tables for proper percentile computation.

## 2. Implemented Event Model

The project models an AI observability subject domain rather than a Dify workflow domain.

Implemented source/domain models:

- `LLMRequestEvent`: one provider model request.
- `AgentRunEvent`: one end-to-end Agent task.
- `AgentSpanEvent`: one trace-like step inside an Agent run.
- `AgentToolCallEvent`: one concrete tool invocation with arguments and result payload.

Relationship:

```text
agent_run_events.run_id
  -> agent_span_events.run_id
  -> llm_request_events.run_id

agent_span_events.span_id
  -> llm_request_events.span_id
  -> agent_tool_call_events.span_id
```

## 3. Source Adapters

| Adapter | Script | Notes |
|---|---|---|
| Mock LLM generator | `scripts/generate_mock_llm_logs.py` | Deterministic local events for tests and demos |
| DeepSeek live collector | `scripts/run_deepseek_live_calls.py` | Calls DeepSeek-compatible API and appends live events |
| Mock Agent generator | `scripts/generate_mock_agent_logs.py` | Creates Agent run/span events |
| Hermes trajectory parser | `scripts/parse_hermes_trajectories.py` | Parses `trajectories.jsonl` into Agent run/span/tool-call raw events |
| Postgres copy exporter | `scripts/export_llm_jsonl_to_postgres_copy.py` | Converts LLM JSONL to Postgres `COPY` rows |

Source adapters are intentionally outside ODS. ODS starts after data lands in the warehouse path.

## 4. Spark Batch Path

LLM path:

```text
raw llm_request JSONL
  -> scripts.spark_build_ods_llm_events
  -> scripts.spark_transform_llm_events
  -> scripts.spark_build_ads_llm_feature_daily_metrics
```

Agent path:

```text
raw agent_run / agent_span JSONL
  -> scripts.spark_build_ods_agent_events
  -> scripts.spark_transform_agent_events
  -> scripts.spark_build_ads_agent_daily_metrics

raw agent_tool_call JSONL
  -> scripts.spark_build_ods_agent_tool_calls
  -> scripts.spark_transform_agent_tool_calls
  -> scripts.spark_build_ads_agent_tool_daily_metrics
```

Local orchestration:

```text
scripts/run_local_batch_pipeline.py
```

The local batch pipeline uses one shared SparkSession for ODS, DWD and ADS work.

## 5. Flink + Paimon Path

Flink SQL assets live under `flink/sql/`.

```text
Postgres source table
  -> Flink CDC source
  -> Kafka ODS table
  -> Paimon DWD table
  -> Paimon ADS table
```

Implemented Flink SQL files:

| File | Purpose |
|---|---|
| `00_catalogs.sql` | Create Paimon catalog and databases |
| `01_source_postgres_cdc.sql` | Define Postgres CDC source |
| `02_ods_kafka_tables.sql` | Define Kafka ODS table |
| `03_dwd_paimon_tables.sql` | Define DWD Paimon table |
| `04_ads_paimon_tables.sql` | Define ADS Paimon table |
| `10_ingest_ods_to_kafka.sql` | Insert source changes into Kafka ODS |
| `20_build_dwd_from_kafka_ods.sql` | Validate and transform Kafka ODS to DWD |
| `30_build_ads_from_dwd.sql` | Aggregate DWD to ADS |

All systems use `date` as the partition/date field. In Flink SQL it is escaped as `` `date` `` because `DATE` is also a type keyword.

## 6. Warehouse Modeling Rules

Kafka ODS:

- Preserves source-aligned fields.
- Buffers CDC changes for replay and downstream decoupling.

DWD:

- Casts types.
- Applies row-level validation through the Spark data-quality module.
- Keeps hash and size fields for prompt/response analytics.
- Drops raw `prompt_text` and `response_text` from LLM DWD to avoid large/sensitive text in analytical facts.
- Sends invalid rows to quarantine instead of failing the whole batch.

ADS:

- Stores additive or directly aggregated metrics.
- Does not store redundant `success_rate` or `error_rate`.
- Dashboard/query layer derives rates from counts.

## 7. Doris Serving Layer

Schema file:

```text
sql/create_doris_tables.sql
```

Implemented Doris DWD tables:

- `dwd_llm_request_events`
- `dwd_agent_run_events`
- `dwd_agent_span_events`
- `dwd_agent_tool_call_events`

Implemented Doris ADS tables:

- `ads_llm_feature_daily_metrics`
- `ads_agent_daily_metrics`
- `ads_agent_tool_daily_metrics`
- `ads_cost_anomaly_daily`
- `ads_sla_daily_report`
- `ads_prompt_version_daily_metrics`
- `dim_model`

Loader:

```text
scripts/load_ads_metrics_to_doris.py
```

The loader validates database and table identifiers before building `TRUNCATE TABLE` and `INSERT` targets.

## 8. Metric Notes

LLM ADS grain:

```text
date, app_name, feature_name, model_name
```

Agent ADS grain:

```text
date, app_name, agent_id, agent_name, task_type
```

Agent tool ADS grain:

```text
date, agent_id, tool_name, tool_type
```

For Spark ADS, p95 metrics use `percentile_approx`. For the local Flink ADS MVP, `max_latency_ms` stores an explicit upper-bound metric because Flink 1.20 SQL does not support `PERCENTILE_CONT` as a streaming aggregate in this setup. Doris DWD tables are available for proper serving-layer percentile queries.

## 9. Local Verification

Run all tests:

```bash
uv run pytest
```

Run the local batch demo:

```bash
uv run python -m scripts.run_local_batch_pipeline --count 100 --seed 42
```

Start Doris and create serving tables:

```bash
docker compose up -d doris-fe doris-be doris-init
docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root --multiquery < sql/create_doris_tables.sql
```

Run Flink SQL files in a shared SQL session:

```bash
scripts/run_flink_sql_sequence.sh \
  flink/sql/00_catalogs.sql \
  flink/sql/01_source_postgres_cdc.sql \
  flink/sql/02_ods_kafka_tables.sql \
  flink/sql/03_dwd_paimon_tables.sql \
  flink/sql/04_ads_paimon_tables.sql \
  flink/sql/10_ingest_ods_to_kafka.sql \
  flink/sql/20_build_dwd_from_kafka_ods.sql
```

## 10. Planned Extensions

Remaining optional extensions:

- Agent dimension table
- RAG retrieval event fact table
- Dashboard application layer
