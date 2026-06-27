# Runtime Runbook

This runbook defines how the local stream-batch lakehouse should be started,
checked, stopped, and recovered. It intentionally separates always-on streaming
work from finite batch and serving-layer work so local development does not need
to keep the full stack running.

## Prerequisites

- Docker Engine/Desktop with Docker Compose v2.
- `uv` and Python 3.11+ for local generators, Spark jobs, and tests.
- `curl`; database clients run inside containers, so host MySQL/psql clients are optional.
- Recommended: at least 12 GB free memory for the full stack.

Copy `.env.example` to `.env` only for live DeepSeek calls. Never commit `.env`
or a real API key.

## Runtime Model

### Long-running services

These services are infrastructure processes. They keep running until stopped:

| Service | Role | Start command |
| --- | --- | --- |
| `postgres` | Operational source database with logical WAL enabled | `make infra-light` |
| `kafka` | Real-time ODS buffer and replay log | `make infra-light` |
| `gravitino` | Metadata service and Web V2 UI for the shared Paimon catalog | `make infra-light` or `make gravitino-up` |
| `flink-jobmanager` | Flink control plane and REST API | `make infra-light` |
| `flink-taskmanager` | Flink worker slots for streaming SQL jobs | `make infra-light` |
| `doris-fe` | Doris SQL frontend, only needed for serving checks | `make infra-serving` |
| `doris-be` | Doris storage/query backend, only needed for serving checks | `make infra-serving` |
| `superset-metadata`, `superset-redis`, `superset` | BI metadata, cache, and analytics UI | `make infra-dashboard` |
| `grafana` | Operational dashboard UI | `make infra-dashboard` |

Use the light runtime for most stream-processing development:

```bash
make infra-light
```

Start Doris only when validating the serving/query layer:

```bash
make infra-serving
```

Stop all local services when they are not needed:

```bash
make infra-stop
```

Stop only dashboard services while keeping Doris and the streaming path running:

```bash
make dashboard-stop
```

### Long-running Flink streaming jobs

The SQL client is only a submission tool. It exits after submitting statements,
while `flink-jobmanager` and `flink-taskmanager` keep the streaming jobs alive.

Expected long-running SQL jobs:

| SQL file | Job role | Input | Output |
| --- | --- | --- | --- |
| `flink/sql/10_ingest_ods_to_kafka.sql` | CDC ingest | Postgres CDC source table | Kafka ODS topic |
| `flink/sql/20_build_dwd_from_kafka_ods.sql` | DWD transform and data quality filter | Kafka ODS topic | Paimon DWD |
| `flink/sql/30_build_dws_from_dwd.sql` | Daily, hourly and session DWS streaming aggregation | Paimon DWD | Paimon DWS |

Submit the streaming jobs:

```bash
make flink-submit
```

Check submitted jobs in the Flink Web UI:

```text
http://localhost:8081
```

Or from the command line:

```bash
make flink-jobs
```

Avoid repeatedly running `make flink-submit` against an already-running cluster
unless you intentionally want duplicate streaming jobs. Check `make flink-jobs`
first.

### Gravitino metadata service

`make infra-light` starts Gravitino and runs the idempotent initializer. Start
or repair only the metadata service with:

```bash
make gravitino-up
```

The initializer creates the `ai_observability` metalake and registers the
`paimon_lake` relational catalog backed by
`file:///workspace/data/paimon`. Re-running it after a restart is safe.

Verify the API and registered catalogs:

```bash
make gravitino-status
make gravitino-catalogs
```

Open Web V2 at `http://localhost:8090`. Gravitino metadata persists in the
`gravitino_data` volume; the Paimon data and snapshots remain in the separate
shared Paimon warehouse volume.

### Finite batch and serving tasks

These commands are finite tasks. They should start, complete, and exit:

| Command | Purpose |
| --- | --- |
| `make seed-data` | Generate mock LLM and Agent events and load LLM source rows into Postgres |
| `make batch-backfill` | Build Spark-derived ADS/dimension assets |
| `make sync-doris` | Create Doris tables/catalogs and load DWS metrics into Doris |
| `make init-superset` | Initialize Superset, register Doris, and provision dashboards |
| `make health` | Check that the running data path is healthy |

Airflow or another scheduler would only be useful around finite tasks like
backfills, validation, or periodic Doris syncs. It should not be used as the
primary mechanism for keeping Flink CDC and streaming SQL jobs alive.

## Demo Commands

The old full demo entry point still exists:

```bash
make demo
```

It now delegates to two smaller phases.

Run only the light streaming pipeline:

```bash
make demo-streaming
```

This starts Postgres, Kafka, Flink, loads mock source data, submits Flink SQL,
and runs a health check without Doris.

Run the serving/query phase:

```bash
make demo-serving
```

This starts Doris, builds bounded dashboard demo datasets, starts Superset and
Grafana, provisions the repository-managed dashboards, checks the serving stack,
and prints the dashboard query preview. It intentionally loads a representative
subset rather than all 44 warehouse tables.

## Dashboard Operations

Start the dashboard dependencies after Doris is ready and data has been synced:

```bash
make infra-dashboard
make init-superset
```

| UI | URL | Local demo credentials |
| --- | --- | --- |
| Superset | `http://localhost:8088` | `admin` / `admin` |
| Grafana | `http://localhost:3001` | `admin` / `admin` |

These credentials and the Compose secret are for localhost demos only. Shared or
production deployments must inject unique secrets, configure TLS/RBAC, restrict
network exposure, and use a non-root Doris account.

Superset dashboards are provisioned deterministically from
`scripts/provision_superset_dashboards.py`. Generate auditable Superset 4.1
ZIP/YAML bundles from the same specification with:

```bash
uv run python -m scripts.provision_superset_dashboards \
  --write-bundles config/superset/dashboards
```

Grafana loads its datasource and dashboard JSON automatically from
`config/grafana/provisioning/`. Run only the Doris/dashboard health checks with:

```bash
scripts/check_pipeline_health.sh --dashboard-only
```

## Health Checks

Run the full health check:

```bash
make health
```

For a light runtime without Doris:

```bash
scripts/check_pipeline_health.sh --skip-serving
```

The health check verifies:

- Required Docker Compose services are running.
- Kafka topic `ods_ai_observability_llm_request_events_di` exists.
- Postgres source table `public.llm_request_events` has rows.
- Flink REST API is reachable.
- DWD and DWS streaming jobs are running.
- Hourly feature and daily session DWS jobs are running.
- Tier 3 compliance, orchestration and platform-health topics and jobs are running.
- Doris FE is queryable when serving checks are enabled.
- Superset and Grafana health endpoints are reachable when serving checks are enabled.

This is a fast operational check. It does not replace deeper table-level
verification queries such as `flink/sql/91_verify_dwd_count.sql` and
`flink/sql/92_verify_dws_metrics.sql`.

The hourly feature job uses a five-second event-time watermark and a one-hour tumble window. The session job resolves sessions from positive feedback (`thumbs_up` or rating >= 4).

Generate deterministic Tier 3 source fixtures:

```bash
uv run python -m scripts.generate_mock_compliance_logs --count 100 --seed 42
uv run python -m scripts.generate_mock_orchestration_logs --count 100 --seed 42
uv run python -m scripts.generate_mock_platform_health_logs --sample-count 12 --seed 42
```

Platform thresholds are managed in `config/platform_health_thresholds.yaml`. All current metrics are upper-bound thresholds. The daily DWS retains the maximum observed value and marks a breach when that maximum exceeds the configured threshold.

## Langfuse External Observability POC

The Langfuse POC treats Langfuse as an External Observability Source per ADR 011. It normalizes representative Langfuse records into existing DWD-compatible event contracts and does not create Langfuse-specific DWD tables.

Trace rules:

- A Langfuse trace is a Trace Envelope by default. The envelope preserves `trace_id`, source identity, app/feature/user/session metadata, environment, observation count, and whether explicit run metadata exists.
- A Trace Envelope does not automatically create an Agent Run.
- An Agent Run is emitted only when trace metadata contains explicit run/task semantics: `run_id` or `agent_run_id`, and `task_type` or `agent_task_type`.
- Span and chain observations map to Agent Span-compatible events and preserve `trace_id`, `span_id`, and `parent_span_id`.
- Tool observations map to Agent Tool Call-compatible events only when a tool name, type, timing, status, and trace boundary can be derived.
- Retriever observations map to Retrieval Request-compatible events only when query, knowledge base, embedding model, strategy, top-k, returned/hit counts, similarity, latency, and environment metadata are sufficient.
- Unsupported or underspecified observations enter sanitized quarantine instead of being forced into the wrong DWD fact.

Score Event split rules:

- Langfuse Score Events are generic source signals and must be classified before DWD. They are not loaded into a standalone score fact.
- Score `source`, `name`, and `config` drive classification. `user`, `manual`, feedback names, and configured feedback types emit Feedback Action-compatible events.
- `evaluator`, `judge`, `test`, `dataset-run`, `automated`, configured evaluator types, evaluation dimensions, pass thresholds, or dataset-run metadata emit Evaluation Judgment-compatible events.
- Feedback scores preserve allowed contract fields such as trace/request/run correlation, user/session metadata, feedback action type, rating value, comment hash/length, model, prompt version, and environment.
- Evaluation scores preserve allowed contract fields such as trace/request/run correlation, evaluator type/model, evaluation dimension, normalized score, raw score, pass threshold, pass/fail, evaluated model, prompt version, latency, and environment.
- Unknown, conflicting, targetless, or invalid-range Score Events enter sanitized quarantine with `_dq_errors`, source ids, trace ids where available, and a payload hash.

Prompt version comparison:

- Langfuse-derived generation observations enter the existing `dwd_ai_llm_request_di`; evaluator/judge/test Score Events enter `dwd_ai_evaluation_judgment_di`.
- `dws_ai_prompt_version_request_1d` remains the single prompt-version DWS. It joins evaluation judgments through `request_id` when a unique LLM request prompt key exists, so a score is not copied across every prompt sharing the same version/model.
- Missing prompt metadata is grouped as `unknown`. Same-day `request_id` conflicts across multiple prompt/version/model keys are surfaced through `metadata_conflict_cnt_1d`.
- `ads_observability_prompt_prompt_version_metrics` consumes the DWS and keeps counts plus score numerator/denominator. Success rate, error rate, pass rate and average score are derived from summed numerators and denominators in queries.

Trace health detail:

- `ads_observability_trace_health_detail` is the first trace-level diagnostic product for Langfuse-backed traces.
- It consumes existing DWD facts directly: LLM requests, Agent runs, Agent spans, Agent tool calls, and Retrieval requests.
- Trace latency uses the earliest available child/run start time through the latest derived end time. Facts without usable timestamps fall back to the maximum node duration.
- It identifies high-cost, slow, failed, failed-child, slow-child, and missing-child-fact traces without adding a session or trace DWS table.
- Bottleneck nodes are labeled as `llm_generation`, `agent_span`, `tool_call`, `retrieval`, or `orchestration`.
- Output keeps correlation IDs, hashes, sizes, metadata, status, latency, token, and cost metrics only. It does not include prompt/response bodies, tool arguments, or tool result text.

Build the ADS detail after the relevant DWD facts are available:

```bash
uv run python -m scripts.spark_build_ads_trace_health_detail
```

Evaluation dataset/experiment regression:

- The assignment Parquet is controlled metadata with columns `request_id`, `dataset_name`, `experiment_name`, and `variant_name`.
- The comparison-config Parquet has `dataset_name`, `experiment_name`, `baseline_variant`, and `candidate_variant`.
- These inputs are ADS configuration. They are not loaded as dataset/experiment DWD, DWS, or DIM entities.
- Missing required metadata, equal baseline/candidate variants, and conflicting request assignments are excluded. Preserve the source config outside the ADS output for audit and reproducibility.

Build and load the component ADS after Evaluation Judgment and LLM Request facts are available:

```bash
uv run python -m scripts.spark_build_ads_evaluation_dataset_experiment_regression \
  --assignment-input data/config/evaluation_experiment_assignments.parquet \
  --comparison-config-input data/config/evaluation_experiment_comparisons.parquet

uv run python -m scripts.load_dws_metrics_to_doris \
  --input data/warehouse/ads/ads_observability_evaluation_dataset_experiment_regression.parquet \
  --table ads_observability_evaluation_dataset_experiment_regression
```

The physical ADS stores only additive baseline/candidate components. Use query 13 in `sql/doris_dashboard_queries.sql` or `build_evaluation_dataset_experiment_regression_comparison` to derive guarded pass rates, averages, deltas, and quality/cost/latency indicators. A zero denominator returns NULL. This stage intentionally does not create a complete dataset/experiment domain model or another Prompt version comparison ADS.

Run the checked-in representative fixture:

```bash
uv run python -m scripts.normalize_langfuse_generations \
  --input tests/fixtures/langfuse/generations.jsonl \
  --output data/raw/langfuse_llm_requests/events.jsonl \
  --quarantine-output data/warehouse/quarantine/dwd_ai_llm_request_di/langfuse_generations.jsonl
```

The output file is contract-compatible raw LLM Request JSONL for the existing Spark/Flink DWD path. Exact replayed source payloads are processed once per normalization batch. The adapter-level `estimated_cost_source` distinguishes Langfuse cost details, metadata estimates, and default zero values; the existing DWD projection continues to store only `estimated_cost_usd`. The quarantine file contains underspecified or invalid generation records with `_dq_errors`, source ids and a payload hash; it does not store Langfuse prompt or response bodies. Normalized LLM Request events retain only `prompt_hash`, `response_hash`, `input_chars`, `output_chars` and token counts for prompt/response content.

Trace and observation normalization is implemented at the Python boundary in `app/langfuse_trace.py`. It returns separate lists for trace envelopes, Agent Runs, Agent Spans, Agent Tool Calls, Retrieval Requests, Feedback Actions, Evaluation Judgments and quarantine records so callers can write each target to the existing ODS/DWD path. Standalone Score Event classification is implemented in `app/langfuse_score.py` for API/export payloads that are not embedded in trace records.

To validate the POC boundary:

```bash
uv run pytest tests/test_langfuse_generation_normalization.py tests/test_langfuse_trace_normalization.py tests/test_langfuse_score_normalization.py tests/test_ads_trace_health_detail.py tests/test_ads_evaluation_dataset_experiment_regression.py -v
```

For a local Spark/Paimon smoke test, feed the normalized JSONL into the existing LLM Request backfill path:

```bash
uv run python -m scripts.spark_paimon_backfill \
  --input data/raw/langfuse_llm_requests/events.jsonl \
  --quarantine-output data/warehouse/quarantine/dwd_ai_llm_request_di/events.parquet
```

Build and load the executive weekly summary after the daily DWS Parquet outputs are available:

```bash
uv run python -m scripts.spark_build_ads_executive_weekly_summary
uv run python -m scripts.load_dws_metrics_to_doris \
  --input data/warehouse/ads/ads_observability_executive_weekly_summary.parquet \
  --table ads_observability_executive_weekly_summary
```

## Flink Recovery

The local Flink runtime is configured with:

- `execution.checkpointing.interval: 10s`
- `execution.checkpointing.mode: EXACTLY_ONCE`
- `execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION`
- `state.checkpoints.dir: file:///workspace/data/paimon/_checkpoints`
- `state.savepoints.dir: file:///workspace/data/paimon/_savepoints`
- `restart-strategy.type: fixed-delay`
- `restart-strategy.fixed-delay.attempts: 3`
- `restart-strategy.fixed-delay.delay: 10s`

This means transient failures should be retried by Flink, and checkpoints are
kept when jobs are cancelled.

### Create a savepoint

Find the job id:

```bash
make flink-jobs
```

Create a savepoint:

```bash
make flink-savepoint JOB_ID=<flink-job-id>
```

The command writes the savepoint under:

```text
file:///workspace/data/paimon/_savepoints
```

### Cancel a job

After a savepoint has been created, cancel the old job:

```bash
make flink-cancel JOB_ID=<flink-job-id>
```

### Restore from a savepoint

Restore one job from its savepoint by submitting the catalog/table bootstrap SQL
plus the single streaming DML SQL that belongs to that job:

```bash
make flink-restore \
  SAVEPOINT=file:///workspace/data/paimon/_savepoints/savepoint-... \
  SQL_FILES="flink/sql/00_catalogs.sql flink/sql/02_ods_kafka_tables.sql flink/sql/03_dwd_paimon_tables.sql flink/sql/20_build_dwd_from_kafka_ods.sql"
```

Use the matching DML file for the job being restored:

| Job | Restore DML file |
| --- | --- |
| CDC to Kafka ODS | `flink/sql/10_ingest_ods_to_kafka.sql` |
| Kafka ODS to DWD | `flink/sql/20_build_dwd_from_kafka_ods.sql` |
| DWD to DWS | `flink/sql/30_build_dws_from_dwd.sql` |

Do not restore multiple independent streaming jobs from one savepoint. Each
Flink savepoint belongs to one job graph.

## Resource Notes

The full local stack is intentionally heavy. Kafka, Flink, and Doris are all
long-running services, and Doris FE/BE are especially expensive on a laptop.

Use this default workflow for day-to-day development:

```bash
make infra-light
make seed-data
make flink-submit
scripts/check_pipeline_health.sh --skip-serving
```

Start Doris only for serving-layer validation:

```bash
make infra-serving
make sync-doris
make health
```
