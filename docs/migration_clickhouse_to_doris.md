# Migration Plan: ClickHouse → Apache Doris

## Status

This migration is now reflected in the repository state.

Implemented changes:

- `docker-compose.yml` now provisions `doris-fe`, `doris-be`, and `doris-init` instead of ClickHouse.
- `sql/create_doris_tables.sql` replaces the old ClickHouse DDL.
- `sql/doris_dashboard_queries.sql` replaces the old dashboard query file.
- `scripts/load_dws_metrics_to_doris.py` replaces the ClickHouse ADS loader.
- `tests/test_doris_loader.py` and `tests/test_doris_schema.py` replace the ClickHouse-specific tests.
- `config/doris/init_fe.sh` replaces the old ClickHouse loader user config.
- `README.md` and the main design docs now describe Doris as the serving layer.

Quick verification:

```bash
uv run pytest
docker compose up -d doris-fe doris-be doris-init
docker compose exec -T doris-fe mysql -h 127.0.0.1 -P 9030 -u root --multiquery < sql/create_doris_tables.sql
uv run python -m scripts.load_dws_metrics_to_doris
```

The rest of this document remains as the detailed migration rationale and mapping record.

The current repository architecture no longer uses a parallel Spark Parquet ADS warehouse as the primary path. Flink and Spark now converge on shared Paimon DWD/DWS tables, and Doris reads those tables through local serving tables and optional Paimon catalog federation.

## 1. Why Doris

| Consideration | ClickHouse | Doris |
|---|---|---|
| Protocol | HTTP + native binary | **MySQL protocol** — any MySQL client/BI tool connects out of the box |
| Real-time ingestion | Batch INSERT or Kafka engine | **Stream Load** HTTP API + native **Flink Doris Connector** for direct Flink → Doris writes |
| Table model | MergeTree family only | DUPLICATE / AGGREGATE / UNIQUE KEY models — better fit for DWD facts vs ADS aggregations |
| Percentile in streaming | N/A (not a stream engine) | N/A, but `PERCENTILE_APPROX` available for serving-layer queries |
| Light-weight schema change | ALTER TABLE is async | **Light Schema Change** — add/drop columns in seconds |
| Dynamic partitioning | Manual `PARTITION BY toYYYYMM()` | **Built-in dynamic partitioning** — auto-creates and drops partitions by time |
| Materialized view | Requires separate engine | Native async/sync materialized views with automatic query rewrite |
| Ecosystem fit | Strong in Eastern Europe, growing globally | **Dominant in Chinese data ecosystem** — aligns with reference HSAP architecture |

The reference architecture diagram uses Doris as the real-time DWD + serving layer. Migrating to Doris aligns the implementation with that target design.

---

## 2. Architecture After Migration

```text
Real-time Path:
PG ──► Flink CDC ──► Kafka (ODS) ──► Flink SQL ──► Paimon (DWD/DWS)
                                         │                  │
                                         ▼                  ▼
                                   Doris DWD (direct)   Doris DWS (sync)

Batch Path:
JSONL ──► Spark Backfill / Validation ───────────► Paimon (DWD/DWS)

Both ──► Doris ──► Dashboard / BI
```

Key change: Flink can write directly to Doris DWD tables via the Flink Doris Connector, in addition to the batch sync path from Spark Parquet.

---

## 3. Impact Analysis

### Files to Delete

| File | Reason |
|---|---|
| `config/clickhouse/users.d/loader.xml` | ClickHouse-specific user config |
| `scripts/load_ads_metrics_to_clickhouse.py` | Replaced by Doris loader |
| `tests/test_clickhouse_loader.py` | Replaced by Doris loader tests |
| `tests/test_clickhouse_schema.py` | Replaced by Doris schema tests |

### Files to Create

| File | Purpose |
|---|---|
| `scripts/load_dws_metrics_to_doris.py` | Doris data loader via MySQL protocol |
| `sql/create_doris_tables.sql` | Doris DDL for all DWD + ADS tables |
| `sql/doris_dashboard_queries.sql` | Dashboard queries in Doris SQL dialect |
| `tests/test_doris_loader.py` | Loader unit tests |
| `tests/test_doris_schema.py` | Schema validation tests |
| `config/doris/init_fe.sh` | FE init script (register BE) |

### Files to Modify

| File | Change |
|---|---|
| `docker-compose.yml` | Replace `clickhouse` service with `doris-fe` + `doris-be` |
| `pyproject.toml` | Replace `clickhouse-connect` with `pymysql` |
| `docs/metric_definitions.md` | Replace ClickHouse-specific SQL syntax |
| `docs/technical_document.md` | Replace all ClickHouse references with Doris |
| `docs/product_document.md` | Update serving layer references |
| `docs/stream_batch_platform.md` | Update serving layer references |
| `docs/data_model.md` | Update serving layer mention |
| `docs/upgrade_plan.md` | Update ClickHouse references |
| `README.md` | Replace ClickHouse with Doris throughout |

---

## Phase 1: Docker Infrastructure

### 1.1 Remove ClickHouse Service

Delete from `docker-compose.yml`:

```yaml
# DELETE THIS
clickhouse:
  image: clickhouse/clickhouse-server:25.6
  ...
```

Delete volume `clickhouse_data`.

Delete directory `config/clickhouse/`.

### 1.2 Add Doris FE + BE Services

Add to `docker-compose.yml`:

```yaml
doris-fe:
  image: apache/doris.fe-ubuntu:2.1.7
  container_name: ai-observability-doris-fe
  ports:
    - "8030:8030"   # FE HTTP
    - "9030:9030"   # FE MySQL protocol
  environment:
    FE_SERVERS: "fe1:doris-fe:9010"
    FE_ID: 1
  volumes:
    - doris_fe_data:/opt/apache-doris/fe/doris-meta
    - ./config/doris/init_fe.sh:/docker-entrypoint-initdb.d/init_fe.sh:ro

doris-be:
  image: apache/doris.be-ubuntu:2.1.7
  container_name: ai-observability-doris-be
  ports:
    - "8040:8040"   # BE HTTP
  environment:
    FE_SERVERS: "fe1:doris-fe:9010"
    BE_ADDR: "doris-be:9050"
  volumes:
    - doris_be_data:/opt/apache-doris/be/storage
  depends_on:
    - doris-fe
```

Add volumes:

```yaml
volumes:
  doris_fe_data:
  doris_be_data:
```

### 1.3 Create BE Registration Script

**Create:** `config/doris/init_fe.sh`

After FE starts, register the BE node:

```bash
#!/bin/bash
# Wait for FE to be ready, then register BE.
# This runs inside the FE container.

sleep 15

mysql -h 127.0.0.1 -P 9030 -u root --batch -e \
  "ALTER SYSTEM ADD BACKEND 'doris-be:9050';" 2>/dev/null || true

echo "Doris BE registration attempted."
```

> **Note:** Doris Docker setup varies across versions. Test the exact image tags and adjust environment variables as needed. The official Doris Docker quick-start guide should be the source of truth for current image names and config.

---

## Phase 2: Doris DDL

### 2.1 Data Type Mapping

| ClickHouse | Doris | Notes |
|---|---|---|
| `String` | `VARCHAR(65533)` | Or `STRING` in Doris 2.x |
| `UInt32` | `INT` | |
| `UInt64` | `BIGINT` | |
| `UInt16` | `SMALLINT` | |
| `Float64` | `DOUBLE` | |
| `Bool` | `BOOLEAN` | |
| `Nullable(String)` | `VARCHAR(65533) NULL` | Doris columns are `NOT NULL` by default |
| `DateTime64(3, 'UTC')` | `DATETIME` | Doris DATETIME has microsecond precision |
| `Date` | `DATE` | |

### 2.2 Table Model Mapping

| Table Type | ClickHouse Engine | Doris Table Model | Reason |
|---|---|---|---|
| DWD fact tables | `MergeTree` | `DUPLICATE KEY` | Append-only event facts, no dedup needed |
| ADS metric tables | `MergeTree` | `DUPLICATE KEY` | Full-refresh from batch, no partial aggregation |
| Dimension tables (future) | `ReplacingMergeTree` | `UNIQUE KEY` | Upsert semantics for slowly changing dimensions |

### 2.3 Create Doris DDL

**Create:** `sql/create_doris_tables.sql`

```sql
CREATE DATABASE IF NOT EXISTS ai_observability;

-- DWD: LLM Request Events
DROP TABLE IF EXISTS ai_observability.dwd_llm_request_events;
CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events
(
    `date`               DATE           NOT NULL,
    request_id           VARCHAR(128)   NOT NULL,
    trace_id             VARCHAR(128)   NOT NULL DEFAULT '',
    run_id               VARCHAR(128)   NOT NULL DEFAULT '',
    span_id              VARCHAR(128)   NOT NULL DEFAULT '',
    agent_id             VARCHAR(128)   NOT NULL DEFAULT '',
    agent_name           VARCHAR(256)   NOT NULL DEFAULT '',
    channel              VARCHAR(64)    NOT NULL DEFAULT '',
    user_id              VARCHAR(128)   NOT NULL,
    session_id           VARCHAR(128)   NOT NULL,
    conversation_id      VARCHAR(128)   NOT NULL DEFAULT '',
    app_name             VARCHAR(256)   NOT NULL,
    feature_name         VARCHAR(256)   NOT NULL,
    prompt_category      VARCHAR(256)   NOT NULL,
    prompt_id            VARCHAR(128)   NOT NULL,
    prompt_version       VARCHAR(64)    NOT NULL,
    model_name           VARCHAR(256)   NOT NULL,
    provider             VARCHAR(128)   NOT NULL,
    prompt_hash          VARCHAR(128)   NOT NULL DEFAULT '',
    response_hash        VARCHAR(128)   NOT NULL DEFAULT '',
    input_chars          INT            NOT NULL DEFAULT 0,
    output_chars         INT            NOT NULL DEFAULT 0,
    prompt_tokens        INT            NOT NULL,
    completion_tokens    INT            NOT NULL,
    total_tokens         INT            NOT NULL,
    request_type         VARCHAR(64)    NOT NULL DEFAULT 'chat',
    is_streaming         BOOLEAN        NOT NULL DEFAULT FALSE,
    temperature          DOUBLE         NOT NULL DEFAULT 0.0,
    max_tokens           INT            NOT NULL DEFAULT 0,
    finish_reason        VARCHAR(64)    NOT NULL DEFAULT '',
    retry_count          INT            NOT NULL DEFAULT 0,
    latency_ms           INT            NOT NULL,
    status               VARCHAR(32)    NOT NULL,
    error_type           VARCHAR(128)   NULL,
    http_status          SMALLINT       NOT NULL,
    estimated_cost_usd   DOUBLE         NOT NULL,
    mode                 VARCHAR(32)    NOT NULL,
    region               VARCHAR(64)    NOT NULL,
    environment          VARCHAR(32)    NOT NULL,
    created_at           DATETIME       NOT NULL
)
DUPLICATE KEY(`date`, request_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(request_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- DWD: Agent Run Events
DROP TABLE IF EXISTS ai_observability.dwd_agent_run_events;
CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events
(
    `date`               DATE           NOT NULL,
    run_id               VARCHAR(128)   NOT NULL,
    trace_id             VARCHAR(128)   NOT NULL,
    agent_id             VARCHAR(128)   NOT NULL,
    agent_name           VARCHAR(256)   NOT NULL,
    agent_version        VARCHAR(64)    NOT NULL,
    app_name             VARCHAR(256)   NOT NULL,
    user_id              VARCHAR(128)   NOT NULL,
    session_id           VARCHAR(128)   NOT NULL,
    conversation_id      VARCHAR(128)   NOT NULL,
    task_type            VARCHAR(256)   NOT NULL,
    channel              VARCHAR(64)    NOT NULL,
    toolsets_used        VARCHAR(65533) NOT NULL,
    input_text_hash      VARCHAR(128)   NOT NULL,
    output_text_hash     VARCHAR(128)   NOT NULL,
    start_time           DATETIME       NOT NULL,
    end_time             DATETIME       NOT NULL,
    duration_ms          INT            NOT NULL,
    status               VARCHAR(32)    NOT NULL,
    error_type           VARCHAR(128)   NULL,
    turn_count           INT            NOT NULL,
    llm_call_count       INT            NOT NULL,
    tool_call_count      INT            NOT NULL,
    retrieval_count      INT            NOT NULL,
    total_tokens         INT            NOT NULL,
    estimated_cost_usd   DOUBLE         NOT NULL,
    mode                 VARCHAR(32)    NOT NULL,
    region               VARCHAR(64)    NOT NULL,
    environment          VARCHAR(32)    NOT NULL,
    created_at           DATETIME       NOT NULL
)
DUPLICATE KEY(`date`, run_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(run_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- DWD: Agent Span Events
DROP TABLE IF EXISTS ai_observability.dwd_agent_span_events;
CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events
(
    `date`               DATE           NOT NULL,
    span_id              VARCHAR(128)   NOT NULL,
    parent_span_id       VARCHAR(128)   NULL,
    run_id               VARCHAR(128)   NOT NULL,
    trace_id             VARCHAR(128)   NOT NULL,
    agent_id             VARCHAR(128)   NOT NULL,
    span_name            VARCHAR(256)   NOT NULL,
    span_type            VARCHAR(64)    NOT NULL,
    span_order           INT            NOT NULL,
    start_time           DATETIME       NOT NULL,
    end_time             DATETIME       NOT NULL,
    duration_ms          INT            NOT NULL,
    status               VARCHAR(32)    NOT NULL,
    error_type           VARCHAR(128)   NULL,
    retry_count          INT            NOT NULL,
    input_size           INT            NOT NULL,
    output_size          INT            NOT NULL,
    model_name           VARCHAR(256)   NULL,
    tool_name            VARCHAR(256)   NULL,
    mode                 VARCHAR(32)    NOT NULL,
    region               VARCHAR(64)    NOT NULL,
    environment          VARCHAR(32)    NOT NULL,
    created_at           DATETIME       NOT NULL
)
DUPLICATE KEY(`date`, span_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(span_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- DWD: Agent Tool Call Events
DROP TABLE IF EXISTS ai_observability.dwd_agent_tool_call_events;
CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events
(
    `date`               DATE           NOT NULL,
    tool_call_id         VARCHAR(128)   NOT NULL,
    span_id              VARCHAR(128)   NOT NULL,
    run_id               VARCHAR(128)   NOT NULL,
    trace_id             VARCHAR(128)   NOT NULL,
    agent_id             VARCHAR(128)   NOT NULL,
    tool_name            VARCHAR(256)   NOT NULL,
    tool_type            VARCHAR(64)    NOT NULL,
    arguments_json       VARCHAR(65533) NOT NULL,
    result_text          VARCHAR(65533) NOT NULL,
    result_size          INT            NOT NULL,
    duration_ms          INT            NOT NULL,
    status               VARCHAR(32)    NOT NULL,
    error_type           VARCHAR(128)   NULL,
    retry_count          INT            NOT NULL,
    mode                 VARCHAR(32)    NOT NULL,
    region               VARCHAR(64)    NOT NULL,
    environment          VARCHAR(32)    NOT NULL,
    created_at           DATETIME       NOT NULL
)
DUPLICATE KEY(`date`, tool_call_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(tool_call_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- ADS: LLM Feature Daily Metrics
DROP TABLE IF EXISTS ai_observability.dws_llm_feature_daily_metrics;
CREATE TABLE IF NOT EXISTS ai_observability.dws_llm_feature_daily_metrics
(
    `date`               DATE           NOT NULL,
    app_name             VARCHAR(256)   NOT NULL,
    feature_name         VARCHAR(256)   NOT NULL,
    model_name           VARCHAR(256)   NOT NULL,
    request_count        BIGINT         NOT NULL,
    success_count        BIGINT         NOT NULL,
    error_count          BIGINT         NOT NULL,
    prompt_tokens        BIGINT         NOT NULL,
    completion_tokens    BIGINT         NOT NULL,
    total_tokens         BIGINT         NOT NULL,
    estimated_cost_usd   DOUBLE         NOT NULL,
    avg_latency_ms       DOUBLE         NOT NULL,
    p95_latency_ms       BIGINT         NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- ADS: Agent Daily Metrics
DROP TABLE IF EXISTS ai_observability.dws_agent_daily_metrics;
CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_daily_metrics
(
    `date`               DATE           NOT NULL,
    app_name             VARCHAR(256)   NOT NULL,
    agent_id             VARCHAR(128)   NOT NULL,
    agent_name           VARCHAR(256)   NOT NULL,
    task_type            VARCHAR(256)   NOT NULL,
    run_count            BIGINT         NOT NULL,
    success_count        BIGINT         NOT NULL,
    error_count          BIGINT         NOT NULL,
    turn_count           BIGINT         NOT NULL,
    llm_call_count       BIGINT         NOT NULL,
    tool_call_count      BIGINT         NOT NULL,
    retrieval_count      BIGINT         NOT NULL,
    total_tokens         BIGINT         NOT NULL,
    estimated_cost_usd   DOUBLE         NOT NULL,
    avg_duration_ms      DOUBLE         NOT NULL,
    p95_duration_ms      BIGINT         NOT NULL,
    span_count           BIGINT         NOT NULL,
    failed_span_count    BIGINT         NOT NULL,
    tool_span_count      BIGINT         NOT NULL,
    llm_span_count       BIGINT         NOT NULL
)
DUPLICATE KEY(`date`, app_name, agent_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(agent_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);

-- ADS: Agent Tool Daily Metrics
DROP TABLE IF EXISTS ai_observability.dws_agent_tool_daily_metrics;
CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_tool_daily_metrics
(
    `date`               DATE           NOT NULL,
    agent_id             VARCHAR(128)   NOT NULL,
    tool_name            VARCHAR(256)   NOT NULL,
    tool_type            VARCHAR(64)    NOT NULL,
    tool_call_count      BIGINT         NOT NULL,
    success_count        BIGINT         NOT NULL,
    error_count          BIGINT         NOT NULL,
    retry_count          BIGINT         NOT NULL,
    avg_duration_ms      DOUBLE         NOT NULL,
    p95_duration_ms      BIGINT         NOT NULL,
    avg_result_size      DOUBLE         NOT NULL,
    max_result_size      BIGINT         NOT NULL
)
DUPLICATE KEY(`date`, agent_id, tool_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(agent_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4"
);
```

### 2.4 Doris Design Decisions

**DUPLICATE KEY chosen over AGGREGATE KEY for ADS tables.** The batch pipeline does full-refresh (`TRUNCATE` + `INSERT`), so AGGREGATE KEY's incremental merge adds no value. DUPLICATE KEY is simpler and has better scan performance for dashboard queries.

**Dynamic partitioning** auto-creates monthly partitions from 12 months ago to 3 months ahead. No manual `PARTITION BY toYYYYMM()` needed.

**`replication_num = 1`** because this is a single-node local setup. Production would use 3.

**`date` as first column in DUPLICATE KEY** because all dashboard queries filter by date range first.

---

## Phase 3: Dashboard Queries for Doris

### 3.1 SQL Dialect Differences

| ClickHouse | Doris | Notes |
|---|---|---|
| `round(x, n)` | `ROUND(x, n)` | Same |
| `sum(...)` | `SUM(...)` | Same |
| `count(*)` | `COUNT(*)` | Same |
| `quantile(0.95)(latency_ms)` | `PERCENTILE_APPROX(latency_ms, 0.95)` | Key syntax difference |
| `countIf(status='success')` | `SUM(CASE WHEN status='success' THEN 1 ELSE 0 END)` | Or `COUNT_IF` in Doris 2.x |
| Subquery syntax | Same | Standard SQL |

### 3.2 Create Doris Dashboard Queries

**Create:** `sql/doris_dashboard_queries.sql`

The current `dashboard_queries.sql` uses standard SQL that works in Doris with no changes for queries 1-7, because they query the ADS table with basic `SUM`, `ROUND`, and subqueries.

**Delete:** `sql/dashboard_queries.sql` (or rename to `sql/dashboard_queries_clickhouse.sql` for reference)

### 3.3 Update Metric Definitions

**File:** `docs/metric_definitions.md`

Replace ClickHouse-specific syntax:

```sql
-- ClickHouse (before)
SELECT quantile(0.95)(latency_ms) AS p95_latency_ms
FROM ai_observability.dwd_llm_request_events;

-- Doris (after)
SELECT PERCENTILE_APPROX(latency_ms, 0.95) AS p95_latency_ms
FROM ai_observability.dwd_llm_request_events;
```

```sql
-- ClickHouse (before)
SELECT countIf(status = 'success') / count(*) AS success_rate
FROM ai_observability.dwd_llm_request_events;

-- Doris (after)
SELECT SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*) AS success_rate
FROM ai_observability.dwd_llm_request_events;
```

Replace all `llm_request_events_ch` references with `ai_observability.dwd_llm_request_events`.

---

## Phase 4: Doris Data Loader

### 4.1 Replace Python Dependency

**File:** `pyproject.toml`

```toml
# Before
"clickhouse-connect>=1.1.1",

# After
"pymysql>=1.1.1",
```

### 4.2 Create Doris Loader Script

**Create:** `scripts/load_dws_metrics_to_doris.py`

```python
import argparse
import re
from datetime import date
from pathlib import Path

import pymysql
import pyarrow.parquet as pq

from app.logging_utils import get_logger, log_info


DEFAULT_INPUT_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")
DEFAULT_TABLE_NAME = "dws_llm_feature_daily_metrics"
DEFAULT_DATABASE = "ai_observability"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 9030
DEFAULT_USER = "root"
DEFAULT_PASSWORD = ""
LOGGER = get_logger(__name__)
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid identifier: {identifier!r}")
    return identifier


def qualified_table_name(database: str, table: str) -> str:
    return f"{validate_identifier(database)}.{validate_identifier(table)}"


def read_parquet_rows(input_path: Path) -> list[dict]:
    table = pq.read_table(input_path)
    rows = []
    for row in table.to_pylist():
        if isinstance(row.get("date"), str):
            row["date"] = date.fromisoformat(row["date"])
        rows.append(row)
    return rows


def load_rows_to_doris(
    rows: list[dict],
    database: str,
    table: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> None:
    table_name = qualified_table_name(database, table)

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")

        if not rows:
            return

        columns = [
            "date", "app_name", "feature_name", "model_name",
            "request_count", "success_count", "error_count",
            "prompt_tokens", "completion_tokens", "total_tokens",
            "estimated_cost_usd", "avg_latency_ms", "p95_latency_ms",
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        column_list = ", ".join(f"`{c}`" for c in columns)
        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

        data = [tuple(row[col] for col in columns) for row in rows]
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, data)

        conn.commit()
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE)
    parser.add_argument("--table", type=str, default=DEFAULT_TABLE_NAME)
    parser.add_argument("--host", type=str, default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--user", type=str, default=DEFAULT_USER)
    parser.add_argument("--password", type=str, default=DEFAULT_PASSWORD)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = read_parquet_rows(args.input)
    load_rows_to_doris(
        rows=rows,
        database=args.database,
        table=args.table,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )
    log_info(LOGGER, "doris_ads_metrics_loaded", rows=len(rows), database=args.database, table=args.table)


if __name__ == "__main__":
    main()
```

### 4.3 Default Port and Credentials

| Setting | Doris Default | Notes |
|---|---|---|
| MySQL port | `9030` | FE MySQL protocol port |
| HTTP port | `8030` | FE HTTP port (for Stream Load) |
| User | `root` | Doris default admin user |
| Password | `""` (empty) | Set via `SET PASSWORD` after first login |

For production, create a dedicated `loader` user:

```sql
CREATE USER 'loader'@'%' IDENTIFIED BY 'loader_pass';
GRANT ALL ON ai_observability.* TO 'loader'@'%';
```

---

## Phase 5: Tests

### 5.1 Doris Loader Tests

**Create:** `tests/test_doris_loader.py`

```python
import pytest
from scripts.load_dws_metrics_to_doris import qualified_table_name


def test_qualified_table_name_accepts_safe_identifiers():
    assert qualified_table_name("ai_observability", "dws_llm_feature_daily_metrics") == (
        "ai_observability.dws_llm_feature_daily_metrics"
    )


@pytest.mark.parametrize(
    ("database", "table"),
    [
        ("ai_observability;DROP TABLE x", "dws_llm_feature_daily_metrics"),
        ("ai_observability", "dws_llm_feature_daily_metrics;DROP TABLE x"),
        ("ai-observability", "dws_llm_feature_daily_metrics"),
        ("ai_observability", "ads llm feature daily metrics"),
        ("ai_observability", "`dws_llm_feature_daily_metrics`"),
    ],
)
def test_qualified_table_name_rejects_unsafe_identifiers(database, table):
    with pytest.raises(ValueError):
        qualified_table_name(database, table)
```

### 5.2 Doris Schema Tests

**Create:** `tests/test_doris_schema.py`

```python
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_doris_schema_defines_all_dwd_and_ads_tables():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_llm_feature_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_tool_daily_metrics" in sql


def test_doris_tables_use_duplicate_key_model():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "DUPLICATE KEY" in sql
    assert "AGGREGATE KEY" not in sql


def test_doris_tables_use_dynamic_partitioning():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert '"dynamic_partition.enable" = "true"' in sql
    assert '"dynamic_partition.time_unit" = "MONTH"' in sql


def test_doris_ads_tables_do_not_store_success_or_error_rates():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "success_rate" not in sql
    assert "error_rate" not in sql
```

---

## Phase 6: Doris Multi-Catalog — Read Paimon Directly

Doris 2.0+ supports Multi-Catalog, including native Paimon catalog integration. Instead of building a separate Flink-to-Doris write path, Doris reads Paimon tables directly. Paimon remains the single source of truth; Doris is purely a serving layer.

### 6.1 Architecture

```text
Kafka ODS ──► Flink SQL ──► Paimon DWD ──► Paimon ADS ──┐
                                                         │ (Doris Catalog)
Parquet ODS ──► Spark ──► Parquet DWD ──► Parquet ADS ──►├──► Doris ──► Dashboard
                                                         │
                                              pymysql sync┘
```

The streaming path writes to Paimon only. Doris reads Paimon ADS via catalog federation — no data movement needed for queries. For the batch Parquet path, the existing `load_dws_metrics_to_doris.py` script (Phase 4) syncs Parquet ADS into Doris local tables.

### 6.2 Mount Paimon Warehouse into Doris BE

The Doris BE needs filesystem access to the Paimon warehouse. Add a shared volume in `docker-compose.yml`:

```yaml
doris-be:
  # ... existing config ...
  volumes:
    - doris_be_data:/opt/apache-doris/be/storage
    - paimon_warehouse:/workspace/data/paimon:ro   # shared with Flink
```

The `paimon_warehouse` volume is already defined for the Flink services. Adding it read-only to the Doris BE container gives Doris access to the Paimon table files.

### 6.3 Register Paimon Catalog in Doris

**Create:** `sql/doris_create_paimon_catalog.sql`

```sql
-- Register Paimon catalog so Doris can read Paimon tables directly.
-- The warehouse path matches the Flink/Paimon volume mount.

CREATE CATALOG IF NOT EXISTS paimon_lake PROPERTIES (
    'type' = 'paimon',
    'warehouse' = 'file:///workspace/data/paimon'
);
```

After running this, all Paimon databases and tables are queryable from Doris:

```sql
-- List Paimon databases
SHOW DATABASES FROM paimon_lake;

-- Query Paimon ADS directly (zero data movement)
SELECT * FROM paimon_lake.dws.llm_feature_daily_metrics;

-- Query Paimon DWD for exact percentile computation
SELECT
    feature_name,
    PERCENTILE_APPROX(latency_ms, 0.95) AS p95_latency_ms
FROM paimon_lake.dwd.llm_request_events
WHERE `date` >= '2026-01-01'
GROUP BY feature_name;
```

This is mathematically correct — computing percentiles from raw Paimon DWD events, not re-aggregating pre-computed p95 values.

### 6.4 Optional: Sync Paimon ADS to Doris Local Tables

Catalog federation is convenient but scans Paimon files on every query. For faster dashboard performance, periodically sync Paimon ADS into Doris local tables:

**Create:** `sql/doris_sync_paimon_ads.sql`

```sql
-- Sync Paimon ADS to Doris local tables for faster dashboard queries.
-- Run this after Flink streaming jobs have written new ADS data.

TRUNCATE TABLE ai_observability.dws_llm_feature_daily_metrics;

INSERT INTO ai_observability.dws_llm_feature_daily_metrics
SELECT * FROM paimon_lake.dws.llm_feature_daily_metrics;
```

This can be triggered manually, by cron, or from a script:

```bash
mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_sync_paimon_ads.sql
```

### 6.5 Two Query Patterns

| Pattern | Source | Use Case | Performance |
|---|---|---|---|
| **Federation query** | `paimon_lake.dws.*` or `paimon_lake.dwd.*` | Ad-hoc analysis, exact percentiles from DWD | Slower — scans Paimon files on each query |
| **Local table query** | `ai_observability.ads_*` | Dashboard panels, high-frequency queries | Faster — data is in Doris columnar storage |

Dashboard queries should target Doris local tables for speed. Deep-dive analysis (e.g., correct p95 across feature groups) should use the Paimon DWD catalog query.

### 6.6 Why This Is Better Than Flink Direct Write

| | Flink → Doris Direct Write | Doris Paimon Catalog |
|---|---|---|
| Extra Flink jobs | One additional streaming job per table | Zero |
| Extra JARs | Flink Doris Connector JAR | None (Doris has built-in Paimon support) |
| Source of truth | Dual write — Paimon and Doris both have copies | **Single** — Paimon only, Doris reads it |
| Consistency risk | Two independent write paths can diverge | Zero — Doris reads the same data Flink wrote |
| Operational complexity | Two streaming jobs to monitor, checkpoint, recover | One streaming pipeline, one periodic sync |
| DWD percentile queries | Requires loading DWD into Doris separately | Query `paimon_lake.dwd.*` directly |

### 6.7 Update Tests

Add to `tests/test_doris_schema.py`:

```python
def test_doris_paimon_catalog_sql_exists():
    sql = (REPO_ROOT / "sql" / "doris_create_paimon_catalog.sql").read_text(encoding="utf-8")

    assert "CREATE CATALOG" in sql
    assert "'type' = 'paimon'" in sql
    assert "paimon_lake" in sql


def test_doris_sync_paimon_ads_sql_exists():
    sql = (REPO_ROOT / "sql" / "doris_sync_paimon_ads.sql").read_text(encoding="utf-8")

    assert "INSERT INTO ai_observability.dws_llm_feature_daily_metrics" in sql
    assert "FROM paimon_lake.dws.llm_feature_daily_metrics" in sql
```

---

## Phase 7: Documentation Updates

### 7.1 Files to Update

| File | Change |
|---|---|
| `README.md` | Replace all "ClickHouse" with "Doris", update ports (8123→9030), update CLI commands (`clickhouse-client` → `mysql -h localhost -P 9030 -u root`) |
| `docs/technical_document.md` | Section 7: rename "ClickHouse Serving Layer" → "Doris Serving Layer", update table names, update loader script name |
| `docs/product_document.md` | Replace "ClickHouse" references |
| `docs/stream_batch_platform.md` | Replace "ClickHouse" with "Doris" |
| `docs/data_model.md` | Update serving layer mention |
| `docs/metric_definitions.md` | Replace ClickHouse SQL syntax with Doris SQL |
| `docs/upgrade_plan.md` | Replace ClickHouse references with Doris |

### 7.2 README Command Updates

```bash
# Before (ClickHouse)
docker compose up -d clickhouse
docker compose exec -T clickhouse clickhouse-client --multiquery < sql/create_clickhouse_tables.sql
uv run python -m scripts.load_ads_metrics_to_clickhouse

# After (Doris)
docker compose up -d doris-fe doris-be
mysql -h 127.0.0.1 -P 9030 -u root < sql/create_doris_tables.sql
uv run python -m scripts.load_dws_metrics_to_doris
```

---

## Files Summary

| Action | File |
|---|---|
| **Delete** | `config/clickhouse/users.d/loader.xml` |
| **Delete** | `scripts/load_ads_metrics_to_clickhouse.py` |
| **Delete** | `tests/test_clickhouse_loader.py` |
| **Delete** | `tests/test_clickhouse_schema.py` |
| **Delete** | `sql/create_clickhouse_tables.sql` |
| **Delete** | `sql/dashboard_queries.sql` |
| **Create** | `sql/create_doris_tables.sql` |
| **Create** | `sql/doris_dashboard_queries.sql` |
| **Create** | `scripts/load_dws_metrics_to_doris.py` |
| **Create** | `tests/test_doris_loader.py` |
| **Create** | `tests/test_doris_schema.py` |
| **Create** | `config/doris/init_fe.sh` |
| **Create** (Phase 6) | `flink/sql/05_dwd_doris_sink.sql` |
| **Create** (Phase 6) | `flink/sql/21_build_dwd_to_doris.sql` |
| **Modify** | `docker-compose.yml` |
| **Modify** | `pyproject.toml` |
| **Modify** | `docker/flink/Dockerfile` (Phase 6 only) |
| **Modify** | `README.md` |
| **Modify** | `docs/technical_document.md` |
| **Modify** | `docs/product_document.md` |
| **Modify** | `docs/stream_batch_platform.md` |
| **Modify** | `docs/data_model.md` |
| **Modify** | `docs/metric_definitions.md` |
| **Modify** | `docs/upgrade_plan.md` |

---

## Execution Order

```text
Phase 1: Docker (Doris FE + BE)
    │
    ▼
Phase 2: DDL (create_doris_tables.sql)
    │
    ▼
Phase 3: Dashboard Queries + Metric Definitions
    │
    ▼
Phase 4: Doris Loader Script + Dependency
    │
    ▼
Phase 5: Tests
    │
    ▼
Phase 6: Flink Doris Connector (optional, independent)
    │
    ▼
Phase 7: Documentation
```

Phase 1-5 are the core migration. Phase 6 is an enhancement. Phase 7 can run in parallel with any phase.

---

## Verification

```bash
# Start Doris
docker compose up -d doris-fe doris-be

# Wait ~30s for FE + BE to register

# Create tables
mysql -h 127.0.0.1 -P 9030 -u root < sql/create_doris_tables.sql

# Run batch pipeline
uv run python -m scripts.spark_paimon_backfill --count 100 --seed 42

# Load ADS metrics to Doris
uv run python -m scripts.load_dws_metrics_to_doris

# Verify row count
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*) FROM ai_observability.dws_llm_feature_daily_metrics;"

# Run dashboard queries
mysql -h 127.0.0.1 -P 9030 -u root < sql/doris_dashboard_queries.sql

# Run tests
uv run pytest
```
