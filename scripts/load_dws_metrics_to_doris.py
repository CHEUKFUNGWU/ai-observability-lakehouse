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

    if isinstance(normalized["date"], str):
        normalized["date"] = date.fromisoformat(normalized["date"])

    return normalized


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
) -> None:
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

            placeholders = ", ".join(["%s"] * len(LOAD_COLUMNS))
            column_clause = ", ".join(f"`{column}`" for column in LOAD_COLUMNS)
            insert_sql = f"INSERT INTO {table_name} ({column_clause}) VALUES ({placeholders})"
            values = [tuple(row[column] for column in LOAD_COLUMNS) for row in rows]
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
