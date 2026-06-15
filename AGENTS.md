# Repository Data Standards

This repository follows the data-layer, table-name, field-name, and task-name standards below for all warehouse SQL, Spark/Flink scripts, tests, and documentation.

## Warehouse Layers

Use these layer prefixes consistently:

| Prefix | Layer | Purpose |
|---|---|---|
| `ods` | Operational data store / raw landing | Preserve source-aligned events and technical metadata. Do not calculate business metrics. |
| `dwd` | Detail warehouse data | Typed, validated, row-level business facts. |
| `dwm` | Detail wide model | Lightly integrated wide detail tables when needed. |
| `dws` | Summary warehouse data | Reusable topic and metric aggregates. |
| `ads` | Application data service | Report, dashboard, alert, and application-specific marts. |
| `dim` | Dimension | Reference dimensions and mostly full snapshots. |
| `tmp` | Temporary | Short-lived development/debug tables only. |
| `view` | View | Logical wrappers over existing tables. |
| `_bak` | Backup suffix | Temporary or historical backup tables. |

## Project Naming Conventions

For this AI observability lakehouse:

- First-level data domain: `ai`.
- Second-level domains: `llm`, `agent`, and other runtime domains as they are introduced.
- Source database name for local Postgres CDC: `ai_observability`.
- Daily event/detail tables are day-incremental and use `di`.
- Dimension snapshots use day-full and use `df`.
- Daily aggregates use `1d`.

Physical table names must include the layer prefix even when the database/schema also represents the layer. This keeps table names portable across Paimon, Doris, local Parquet paths, tests, and documentation.

## Table Naming

ODS tables:

```text
ods_{source_database}_{source_table}_{storage_strategy}
```

Example for this project:

```text
ods_ai_observability_llm_request_events_di
```

DWD tables:

```text
dwd_{domain}_{subdomain}_{business_process}_{storage_strategy}
```

Current project examples:

```text
dwd_ai_llm_request_di
dwd_ai_agent_run_di
dwd_ai_agent_span_di
dwd_ai_agent_tool_call_di
```

DWS tables:

```text
dws_{domain}_{subdomain}_{grain}_{business_process}_{period}
```

Current project examples:

```text
dws_ai_llm_feature_request_1d
dws_ai_agent_agent_run_1d
dws_ai_agent_tool_tool_call_1d
```

ADS tables:

```text
ads_{application_theme}_{subtheme}_{grain}_{business_process}
```

Current project examples:

```text
ads_observability_cost_feature_anomaly
ads_observability_sla_feature_report
ads_observability_prompt_prompt_version_metrics
```

Dimension tables:

```text
dim_{dimension_definition}_{storage_strategy}
```

Current project example:

```text
dim_model_df
```

Temporary tables and views:

```text
tmp_{table_name}_{sequence_or_date}
view_{table_name}
{table_name}_bak
```

## Storage Strategy Suffixes

| Suffix | Meaning |
|---|---|
| `df` | Day full snapshot |
| `di` | Day incremental |
| `hf` | Hour full snapshot |
| `hi` | Hour incremental |

## Field Naming

- IDs are strings: `request_id`, `run_id`, `span_id`, `tool_call_id`, `user_id`.
- Boolean flag fields use `is_{meaning}`. In SQL engines that support booleans, use `BOOLEAN`; otherwise use `BIGINT` with `1` for yes and `0` for no.
- Enum fields use `{meaning}_type`, such as `request_type`, `tool_type`, `error_type`.
- Date fields use `{meaning}_date`; timestamp fields use `{meaning}_time` or an established event name such as `created_at`, `start_time`, `end_time`.
- Count metrics use `{metric}_cnt_{period}` for new metrics. Existing fields ending in `_count` may remain only when changing them would create broad compatibility churn; new DWS/ADS fields should prefer `_cnt_1d`, `_cnt_30d`, or `_cnt_td`.
- Amount metrics use `{metric}_amt_{period}`. Currency precision-sensitive amounts should use `DECIMAL`; estimated analytics costs may remain `DOUBLE` when they are approximations.
- Rate metrics use `{metric}_rate_{period}` and should generally be derived in query/view/report layers unless an ADR explicitly allows storage.
- Duration metrics use `{process}_dur` or engine-native units such as `duration_ms` and `latency_ms` when the metric is an event measurement.
- Recent/latest metrics use `{metric}_last1`.

## Grain and Period Rules

Every table definition, model doc, and transformation should make the grain obvious from the table name and primary key:

- `dwd_ai_llm_request_di`: one row per LLM provider request attempt result.
- `dwd_ai_agent_run_di`: one row per end-to-end agent task/run.
- `dwd_ai_agent_span_di`: one row per agent runtime step/span.
- `dwd_ai_agent_tool_call_di`: one row per concrete tool invocation.
- `dws_ai_llm_feature_request_1d`: one daily row per app, feature, and model.
- `dws_ai_agent_agent_run_1d`: one daily row per app, agent, and task type.
- `dws_ai_agent_tool_tool_call_1d`: one daily row per agent, tool, and tool type.

Use standard period suffixes such as `1d`, `30d`, `3m`, `6m`, `1y`, `td`, and `nd`.

## Task and Script Naming

- Transformation scripts should be named after the target table where practical.
- SQL transformation modules should use `trans_{table_name}` when split into reusable submodules.
- Backup modules should use `bak_{table_name}`.
- Python helpers may use `python_{operation}_{table_name}` for new standalone modules, but existing `spark_*` names may remain when they are clearer in the current codebase.
- Data movement jobs should make source and sink obvious, for example `postgres2kafka_ods_ai_observability_llm_request_events`.

## Lifecycle Guidance

- ODS/DWD incremental event tables are retained longer because they support traceability, replay, and debugging.
- DWS/ADS result tables may have shorter retention when they can be rebuilt from upstream data.
- Dimension tables are usually full snapshots and should use `df`.
- Configure physical retention with engine-specific partition settings. Doris dynamic partitions in this repo should keep at least 12 historical months unless an ADR says otherwise.

## Change Discipline

When adding or modifying SQL, Spark, Flink, Doris, tests, or docs:

- Use the canonical names in this file.
- Update all table references together: DDL, DML, scripts, tests, docs, runbooks, and health checks.
- Do not add a new layer/table naming style without first updating this file and documenting the exception in an ADR.
