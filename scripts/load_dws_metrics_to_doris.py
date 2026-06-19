import argparse
import re
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pymysql

from app.logging_utils import get_logger, log_info

DEFAULT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet")
DEFAULT_TABLE_NAME = "dws_ai_llm_feature_request_1d"
DEFAULT_DATABASE = "ai_observability"
DEFAULT_USER = "root"
DEFAULT_PASSWORD = ""
LOGGER = get_logger(__name__)
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
LOAD_COLUMNS = [
    "date",
    "app_name",
    "feature_name",
    "model_name",
    "request_count",
    "success_count",
    "error_count",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "estimated_cost_usd",
    "avg_latency_ms",
    "max_latency_ms",
    "p95_latency_ms",
]
DIM_MODEL_COLUMNS = [
    "model_name",
    "provider",
    "input_price_per_1m_tokens",
    "output_price_per_1m_tokens",
    "max_context_tokens",
    "release_date",
    "status",
]
COMPLIANCE_ACCESS_COLUMNS = [
    "date",
    "audit_event_id",
    "user_id",
    "action_type",
    "resource_type",
    "resource_id",
    "ip_address",
    "access_granted",
    "denial_reason",
    "data_classification",
    "created_at",
]
COMPLIANCE_RETENTION_COLUMNS = [
    "date",
    "retention_event_id",
    "table_name",
    "partition_date",
    "action_type",
    "rows_affected",
    "policy_name",
    "created_at",
]
AGENT_ORCHESTRATION_HANDOFF_COLUMNS = [
    "date",
    "parent_agent_id",
    "child_agent_id",
    "handoff_type",
    "handoff_cnt_1d",
    "success_cnt_1d",
    "error_cnt_1d",
    "timeout_cnt_1d",
    "avg_handoff_latency_ms",
    "p95_handoff_latency_ms",
]
PLATFORM_HEALTH_COLUMNS = [
    "date",
    "component",
    "metric_name",
    "metric_value",
    "threshold",
    "is_breach",
]
EXECUTIVE_WEEKLY_COLUMNS = [
    "week_start_date",
    "app_name",
    "request_cnt_1w",
    "success_cnt_1w",
    "error_cnt_1w",
    "total_token_cnt_1w",
    "llm_cost_amt_1w",
    "p95_latency_ms_max",
    "agent_run_cnt_1w",
    "agent_success_cnt_1w",
    "agent_error_cnt_1w",
    "agent_cost_amt_1w",
    "retrieval_cnt_1w",
    "retrieval_returned_cnt_1w",
    "retrieval_hit_cnt_1w",
    "feedback_cnt_1w",
    "thumbs_up_cnt_1w",
    "thumbs_down_cnt_1w",
    "guardrail_check_cnt_1w",
    "guardrail_triggered_cnt_1w",
    "guardrail_block_cnt_1w",
    "evaluation_cnt_1w",
    "evaluation_pass_cnt_1w",
    "evaluation_fail_cnt_1w",
    "avg_latency_ms",
    "retrieval_hit_rate_1w",
    "satisfaction_rate_1w",
    "evaluation_pass_rate_1w",
    "avg_evaluation_score",
    "total_ai_cost_amt_1w",
]
TABLE_LOAD_COLUMNS = {
    DEFAULT_TABLE_NAME: LOAD_COLUMNS,
    "ads_observability_executive_weekly_summary": EXECUTIVE_WEEKLY_COLUMNS,
    "dim_model_df": DIM_MODEL_COLUMNS,
    "dwd_ai_compliance_access_audit_di": COMPLIANCE_ACCESS_COLUMNS,
    "dwd_ai_compliance_data_retention_di": COMPLIANCE_RETENTION_COLUMNS,
    "dws_ai_agent_orchestration_handoff_1d": AGENT_ORCHESTRATION_HANDOFF_COLUMNS,
    "dws_ai_platform_component_health_1d": PLATFORM_HEALTH_COLUMNS,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE)
    parser.add_argument("--table", type=str, default=DEFAULT_TABLE_NAME)
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=9030)
    parser.add_argument("--user", type=str, default=DEFAULT_USER)
    parser.add_argument("--password", type=str, default=DEFAULT_PASSWORD)
    return parser.parse_args()


def read_parquet_rows(input_path: Path) -> list[dict]:
    table = pq.read_table(input_path)
    return [normalize_row(row) for row in table.to_pylist()]


def normalize_row(row: dict) -> dict:
    normalized = dict(row)

    for field_name, value in normalized.items():
        if (field_name == "date" or field_name.endswith("_date")) and isinstance(value, str):
            normalized[field_name] = date.fromisoformat(value)

    return normalized


def columns_for_table(table: str) -> list[str]:
    try:
        return TABLE_LOAD_COLUMNS[table]
    except KeyError as error:
        raise ValueError(f"Unsupported Doris load table: {table!r}") from error


def validate_doris_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid Doris identifier: {identifier!r}")
    return identifier


def qualified_table_name(database: str, table: str) -> str:
    safe_database = validate_doris_identifier(database)
    safe_table = validate_doris_identifier(table)
    return f"`{safe_database}`.`{safe_table}`"


def load_rows_to_doris(
    rows: list[dict],
    database: str,
    table: str,
    host: str,
    port: int,
    user: str,
    password: str,
    columns: list[str] | None = None,
) -> None:
    load_columns = columns or columns_for_table(table)
    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )

    table_name = qualified_table_name(database, table)

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")

            if not rows:
                connection.commit()
                return

            placeholders = ", ".join(["%s"] * len(load_columns))
            column_clause = ", ".join(f"`{column}`" for column in load_columns)
            insert_sql = f"INSERT INTO {table_name} ({column_clause}) VALUES ({placeholders})"
            values = [tuple(row[column] for column in load_columns) for row in rows]
            cursor.executemany(insert_sql, values)

        connection.commit()
    finally:
        connection.close()


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

    log_info(LOGGER, "doris_dws_metrics_loaded", rows=len(rows), database=args.database, table=args.table)


if __name__ == "__main__":
    main()
