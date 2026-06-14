# Architecture Unification Plan: Eliminate Dual Warehouse, Fix Layering

## Context

The AI observability lakehouse has a **dual warehouse anti-pattern**: Flink writes to Paimon tables while Spark writes to completely independent Parquet files. The two paths share no storage, have divergent schemas (`max_latency_ms` vs `p95_latency_ms`), duplicate business logic (DWD transforms in both Flink SQL and Spark Python), and have inconsistent DQ coverage (9 rules in Spark, 1 in Flink). Additionally, three core daily metrics tables are misclassified as ADS when they are actually DWS (public summary layer). Doris Multi-Catalog (reading Paimon directly) is documented but not implemented.

This plan eliminates all redundant pipelines and makes Paimon the single source of truth, with Spark as a supplementary backfill/validation tool — not a parallel pipeline.

---

## Phase 1: Introduce DWS Layer (Rename Misclassified Tables)

**Goal:** Correct the warehouse layering — the three general-purpose daily metrics tables are DWS, not ADS.

**Evidence:** `doris_dashboard_queries.sql` re-aggregates `ads_llm_feature_daily_metrics` with `GROUP BY feature`, `GROUP BY model`, `GROUP BY app+feature` across 7 of 8 queries. A true ADS table is consumed directly; a table that gets re-aggregated is DWS.

### Renames

| Current Name | New Name |
|---|---|
| `ads_llm_feature_daily_metrics` | `dws_llm_feature_daily_metrics` |
| `ads_agent_daily_metrics` | `dws_agent_daily_metrics` |
| `ads_agent_tool_daily_metrics` | `dws_agent_tool_daily_metrics` |

These stay as ADS (application-specific): `ads_cost_anomaly_daily`, `ads_sla_daily_report`, `ads_prompt_version_daily_metrics`, `mv_daily_summary`.

### Files to Modify

**Flink SQL (rename + update references):**
- `flink/sql/00_catalogs.sql` — add `CREATE DATABASE IF NOT EXISTS paimon_lake.dws;`
- `flink/sql/04_ads_paimon_tables.sql` → rename to `04_dws_paimon_tables.sql`, change table to `paimon_lake.dws.llm_feature_daily_metrics`
- `flink/sql/30_build_ads_from_dwd.sql` → rename to `30_build_dws_from_dwd.sql`, update INSERT INTO target
- `flink/sql/92_verify_ads_metrics.sql` → rename to `92_verify_dws_metrics.sql`, update FROM target

**Spark scripts (rename + update paths):**
- `scripts/spark_build_ads_llm_feature_daily_metrics.py` → `spark_build_dws_llm_feature_daily_metrics.py`, output path `dws/`
- `scripts/spark_build_ads_agent_daily_metrics.py` → `spark_build_dws_agent_daily_metrics.py`, output path `dws/`
- `scripts/spark_build_ads_agent_tool_daily_metrics.py` → `spark_build_dws_agent_tool_daily_metrics.py`, output path `dws/`
- `scripts/spark_build_ads_cost_anomaly.py` — update input path from `ads/` to `dws/`
- `scripts/spark_build_ads_sla_daily.py` — update input path from `ads/` to `dws/`
- `scripts/run_local_batch_pipeline.py` — update import and path references
- `scripts/run_benchmark.py` — update import references

**Doris:**
- `sql/create_doris_tables.sql` — rename three tables, update MV reference
- `sql/doris_dashboard_queries.sql` — replace `ads_llm_feature_daily_metrics` → `dws_llm_feature_daily_metrics` (8 queries)
- `scripts/load_ads_metrics_to_doris.py` → rename to `load_dws_metrics_to_doris.py`, update DEFAULT_TABLE_NAME

**Demo/Makefile:**
- `scripts/run_full_demo.sh` — update loader module name
- No Makefile change needed (it calls `run_local_batch_pipeline`)

**Tests (update imports, optionally rename files):**
- `tests/test_ads_feature_daily_metrics.py` — update import to `spark_build_dws_*`
- `tests/test_ads_agent_daily_metrics.py` — same
- `tests/test_ads_agent_tool_daily_metrics.py` — same
- `tests/test_flink_sql_assets.py` — update expected file names and table name assertions
- `tests/test_doris_schema.py` — update table name assertions
- `tests/test_doris_loader.py` — update import path
- `tests/test_local_batch_pipeline.py` — update imports
- `tests/test_run_benchmark.py` — update monkeypatch targets

**ADR:** Create `docs/adr/006-dws-layer-reclassification.md`

**Verification:** `uv run pytest -v` — all tests pass with renamed modules.

---

## Phase 2: Unify DQ Rules in Flink SQL

**Goal:** Add the 8 missing validation rules to the Flink DWD WHERE clause, matching `app/data_quality.py`.

### Current State

| Rule | Flink SQL | Spark |
|---|---|---|
| `request_id IS NOT NULL` | ❌ | ✅ |
| `created_at IS NOT NULL` | ❌ | ✅ |
| `prompt_tokens >= 0` | ❌ | ✅ |
| `completion_tokens >= 0` | ❌ | ✅ |
| `total_tokens = prompt_tokens + completion_tokens` | ✅ | ✅ |
| `latency_ms > 0` | ❌ | ✅ |
| `status IN ('success', 'error')` | ❌ | ✅ |
| `estimated_cost_usd >= 0` | ❌ | ✅ |
| `mode IN ('mock', 'live', 'replay', 'hermes')` | ❌ | ✅ |

### Files to Modify

- `flink/sql/20_build_dwd_from_kafka_ods.sql` — expand WHERE clause with all 9 rules
- `tests/test_flink_sql_assets.py` — add assertions for new WHERE conditions
- Create `docs/adr/007-unified-dq-rules.md`

**Verification:** `uv run pytest -v`

---

## Phase 3: Unify DWS Schema (max_latency_ms + p95_latency_ms)

**Goal:** The unified DWS table carries BOTH columns. Flink populates `max_latency_ms` (sets `p95_latency_ms = 0`). Spark populates both. Doris has both.

### Files to Modify

- `flink/sql/04_dws_paimon_tables.sql` (renamed in Phase 1) — add `p95_latency_ms BIGINT` column
- `flink/sql/30_build_dws_from_dwd.sql` (renamed in Phase 1) — add `CAST(0 AS BIGINT) AS p95_latency_ms`
- `scripts/spark_build_dws_llm_feature_daily_metrics.py` (renamed in Phase 1) — add `MAX(latency_ms)` as `max_latency_ms`
- `sql/create_doris_tables.sql` — add `max_latency_ms` column to `dws_llm_feature_daily_metrics`
- `scripts/load_dws_metrics_to_doris.py` (renamed in Phase 1) — add `max_latency_ms` to LOAD_COLUMNS
- Tests: update latency assertions in DWS and Doris schema tests
- Update `docs/adr/004-flink-ads-p95-max-proxy.md` — reference DWS naming, document unified schema

**Verification:** `uv run pytest -v`

---

## Phase 4: Spark Paimon Integration

**Goal:** Configure Spark to read/write Paimon tables — the same tables Flink writes to. This is the critical change that eliminates the dual warehouse.

### 4.1 Spark Configuration

**`scripts/spark_utils.py`** — add `build_paimon_spark_session()`:

```python
def build_paimon_spark_session(app_name: str, warehouse: str | None = None) -> SparkSession:
    wh = warehouse or os.environ.get("PAIMON_WAREHOUSE", "data/paimon")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.apache.paimon:paimon-spark-4.0:1.2.0")
        .config("spark.sql.catalog.paimon_lake", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon_lake.warehouse", wh)
        .config("spark.sql.extensions", "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
```

Keep the existing `build_spark_session()` unchanged — tests and mock generators don't need Paimon.

**Risk:** `paimon-spark-4.0:1.2.0` may not exist yet for PySpark 4.1.x. If unavailable:

- Fallback 1: use `paimon-spark-3.5:1.2.0` with `pyspark>=3.5,<4` (requires pyproject.toml change)
- Fallback 2: have Spark read Paimon's underlying Parquet/ORC files directly via the warehouse path (less elegant, no catalog, but works)
- **Verify first** by running `build_paimon_spark_session()` before proceeding with subsequent phases

### 4.2 New Scripts

**Create `scripts/spark_paimon_backfill.py`** — unified backfill script replacing `run_local_batch_pipeline.py`:

1. Reads JSONL source (or Parquet ODS)
2. Applies `transform_llm_events()` (reuses existing function)
3. Applies `validate_llm_events()` + `split_valid_quarantine()` (reuses existing DQ)
4. Writes valid events to `paimon_lake.dwd.llm_request_events`
5. Computes DWS metrics via `build_feature_daily_metrics()` (reuses existing function)
6. Writes to `paimon_lake.dws.llm_feature_daily_metrics`
- Accepts `--start-date`, `--end-date` for targeted backfill

**Create `scripts/spark_paimon_validate.py`** — reads Paimon DWD, runs DQ checks, reports statistics.

### 4.3 Test Fixture

**`tests/conftest.py`** — add `paimon_spark` session-scoped fixture using `build_paimon_spark_session` with `tmp_path_factory` warehouse. Mark with `@pytest.mark.paimon` for optional skip in CI.

**Create `tests/test_spark_paimon_integration.py`** — tests Paimon catalog creation, DWD write/read, backfill flow.

### 4.4 ADR

Create `docs/adr/008-spark-paimon-unified-warehouse.md`

**Verification:** `uv run pytest -v` — existing tests unaffected, new Paimon tests pass.

---

## Phase 5: Implement Doris Multi-Catalog

**Goal:** Doris reads Paimon tables directly via catalog federation. Implements Phase 6 from `migration_clickhouse_to_doris.md`.

### Files to Create/Modify

- `docker-compose.yml` — add `paimon_warehouse:/workspace/data/paimon:ro` to `doris-be` volumes
- Create `sql/doris_create_paimon_catalog.sql`:

```sql
CREATE CATALOG IF NOT EXISTS paimon_lake PROPERTIES (
    'type' = 'paimon',
    'warehouse' = 'file:///workspace/data/paimon'
);
```

- Create `sql/doris_sync_paimon_dws.sql`:

```sql
TRUNCATE TABLE ai_observability.dws_llm_feature_daily_metrics;
INSERT INTO ai_observability.dws_llm_feature_daily_metrics
SELECT * FROM paimon_lake.dws.llm_feature_daily_metrics;
```

- `scripts/run_full_demo.sh` — add catalog creation and sync steps
- Tests: verify SQL files exist and reference correct table names

**Verification:** `uv run pytest -v`, then Docker-based manual verification.

---

## Phase 6: Clean Up Redundant Batch Pipeline

**Goal:** Remove the independent JSONL→Spark ODS→DWD→ADS Parquet chain. Spark's role is now backfill/validation via Paimon only.

### Keep (source adapters + reusable functions)

| File | Reason |
|---|---|
| `scripts/generate_mock_llm_logs.py` | Seeds Postgres for CDC |
| `scripts/generate_mock_agent_logs.py` | Seeds Postgres |
| `scripts/load_llm_jsonl_to_postgres_source.sh` | Loads JSONL → Postgres |
| `scripts/export_llm_jsonl_to_postgres_copy.py` | JSONL → COPY format |
| `scripts/parse_hermes_trajectories.py` | Source adapter |
| `scripts/run_deepseek_live_calls.py` | Live API collector |
| `scripts/spark_utils.py` | Utility (both session builders) |
| `scripts/spark_paimon_backfill.py` | New Paimon backfill (Phase 4) |
| `scripts/spark_paimon_validate.py` | New validation (Phase 4) |
| `app/data_quality.py` | DQ rules (shared by backfill) |

### Keep as importable modules (remove `main()` / standalone entry point)

These files keep their pure transform functions but lose their standalone `main()` + Parquet I/O:

- `scripts/spark_transform_llm_events.py` — keep `transform_llm_events()`
- `scripts/spark_transform_agent_events.py` — keep `transform_agent_run_events()`, `transform_agent_span_events()`
- `scripts/spark_transform_agent_tool_calls.py` — keep `transform_agent_tool_call_events()`
- `scripts/spark_build_dws_llm_feature_daily_metrics.py` — keep `build_feature_daily_metrics()`
- `scripts/spark_build_dws_agent_daily_metrics.py` — keep `build_agent_daily_metrics()`
- `scripts/spark_build_dws_agent_tool_daily_metrics.py` — keep `build_agent_tool_daily_metrics()`

### Delete

| File | Reason |
|---|---|
| `scripts/spark_build_ods_llm_events.py` | ODS is now Flink CDC + Kafka |
| `scripts/spark_build_ods_agent_events.py` | Same |
| `scripts/spark_build_ods_agent_tool_calls.py` | Same |
| `scripts/run_local_batch_pipeline.py` | Replaced by `spark_paimon_backfill.py` |

### Update (true ADS scripts read from Paimon)

- `scripts/spark_build_ads_cost_anomaly.py` — use `build_paimon_spark_session`, read from `paimon_lake.dws.llm_feature_daily_metrics`
- `scripts/spark_build_ads_sla_daily.py` — same
- `scripts/spark_build_ads_prompt_version_metrics.py` — read from `paimon_lake.dwd.llm_request_events`
- `scripts/spark_build_dim_model.py` — optionally write to Paimon dim table

### Update demo/Makefile

- `scripts/run_full_demo.sh` — remove `run_local_batch_pipeline` call; flow becomes: generate → load Postgres → Flink CDC → Spark backfill (optional) → Doris catalog
- `Makefile` — change `pipeline` target to `spark_paimon_backfill`

### Tests to delete

- `tests/test_ods_events.py` — ODS scripts deleted
- `tests/test_local_batch_pipeline.py` — pipeline script deleted

### Tests to keep (they test pure functions, not pipeline orchestration)

- `tests/test_spark_transform_*.py` — test transform functions
- `tests/test_dws_*.py` — test aggregation functions
- `tests/test_data_quality.py` — test DQ rules
- `tests/test_dim_and_analytics_assets.py` — test ADS functions
- All other existing tests

### Update `scripts/run_benchmark.py`

Rewrite to use Paimon backfill, or mark as deprecated with a note.

**Verification:** `uv run pytest -v` — test count decreases by ~5 (deleted ODS/pipeline tests) but all retained tests pass.

---

## Phase 7: Documentation Updates

**Goal:** All docs reflect the unified architecture.

### New ADRs (created in prior phases)

- `docs/adr/006-dws-layer-reclassification.md`
- `docs/adr/007-unified-dq-rules.md`
- `docs/adr/008-spark-paimon-unified-warehouse.md`

### Docs to update

| File | Changes |
|---|---|
| `README.md` | Architecture diagrams: single Paimon warehouse, DWS layer, Spark as backfill tool. Remove independent batch pipeline section. Update Quick Start commands. |
| `docs/data_model.md` | Add DWS layer description. Rename tables. Remove Parquet ODS/DWD paths. |
| `docs/data_lineage.md` | Unified Mermaid diagram: Sources → Postgres → CDC → Kafka → Flink → Paimon DWD/DWS → Doris. Spark as supplementary. |
| `docs/stream_batch_platform.md` | Update engine responsibilities. Spark = backfill/validation, not parallel pipeline. |
| `docs/technical_document.md` | Update all table refs, layer descriptions, Spark role. |
| `docs/product_document.md` | Update layer descriptions. |
| `docs/metric_definitions.md` | Rename `ads_` → `dws_` for three summary tables. |
| `docs/migration_clickhouse_to_doris.md` | Add note that Phase 6 is now implemented. |
| `docs/upgrade_plan.md` | Update table references. |
| `docs/architecture.svg` | Update to show DWS layer, single Paimon warehouse. |

**Verification:** Review all docs for internal consistency. `uv run pytest -v` passes.

---

## Execution Order

```
Phase 1 (DWS Rename) ← foundation, must be first
  ↓
Phase 2 (Unify DQ) ← independent, quick
  ↓
Phase 3 (Unify Schema) ← independent, quick
  ↓
Phase 4 (Spark Paimon) ← critical integration, verify JAR compatibility first
  ↓
Phase 5 (Doris Multi-Catalog) ← requires Phase 4's Paimon warehouse
  ↓
Phase 6 (Cleanup) ← requires Phase 4's backfill script as replacement
  ↓
Phase 7 (Docs) ← last, reflects all changes
```

## Risk: Paimon Spark 4.x Compatibility

PySpark 4.1.2 is very new. `paimon-spark-4.0:1.2.0` may not exist on Maven Central yet.

**Verify early** (before Phase 4 implementation):

```python
from scripts.spark_utils import build_paimon_spark_session
spark = build_paimon_spark_session("test")
spark.sql("SHOW CATALOGS").show()
spark.stop()
```

**Fallback options if JAR unavailable:**

1. Downgrade to `pyspark>=3.5,<4` + `paimon-spark-3.5:1.2.0` (proven combination)
2. Spark reads Paimon's physical files directly via warehouse path (no catalog, less elegant)
3. Keep Spark writing to Parquet but have Doris Multi-Catalog handle the Paimon side only (partial unification)
