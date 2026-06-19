# Runtime Runbook

This runbook defines how the local stream-batch lakehouse should be started,
checked, stopped, and recovered. It intentionally separates always-on streaming
work from finite batch and serving-layer work so local development does not need
to keep the full stack running.

## Runtime Model

### Long-running services

These services are infrastructure processes. They keep running until stopped:

| Service | Role | Start command |
| --- | --- | --- |
| `postgres` | Operational source database with logical WAL enabled | `make infra-light` |
| `kafka` | Real-time ODS buffer and replay log | `make infra-light` |
| `flink-jobmanager` | Flink control plane and REST API | `make infra-light` |
| `flink-taskmanager` | Flink worker slots for streaming SQL jobs | `make infra-light` |
| `doris-fe` | Doris SQL frontend, only needed for serving checks | `make infra-serving` |
| `doris-be` | Doris storage/query backend, only needed for serving checks | `make infra-serving` |

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

### Finite batch and serving tasks

These commands are finite tasks. They should start, complete, and exit:

| Command | Purpose |
| --- | --- |
| `make seed-data` | Generate mock LLM and Agent events and load LLM source rows into Postgres |
| `make batch-backfill` | Build Spark-derived ADS/dimension assets |
| `make sync-doris` | Create Doris tables/catalogs and load DWS metrics into Doris |
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

This starts Doris, runs the Spark/Doris serving steps, checks the full stack, and
prints the dashboard query preview.

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
