import argparse
from datetime import date
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq

DEFAULT_INPUT_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")
DEFAULT_TABLE_NAME = "ads_llm_feature_daily_metrics"
DEFAULT_DATABASE = "ai_observability"
DEFAULT_USER = "loader"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--database", type=str, default=DEFAULT_DATABASE)
    parser.add_argument("--table", type=str, default=DEFAULT_TABLE_NAME)
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=8123)
    parser.add_argument("--user", type=str, default=DEFAULT_USER)
    parser.add_argument("--password", type=str, default="")
    return parser.parse_args()


def read_parquet_rows(input_path: Path) -> list[dict]:
    table = pq.read_table(input_path)
    return [normalize_row(row) for row in table.to_pylist()]


def normalize_row(row: dict) -> dict:
    normalized = dict(row)

    if isinstance(normalized["date"], str):
        normalized["date"] = date.fromisoformat(normalized["date"])

    return normalized


def load_rows_to_clickhouse(
    rows: list[dict],
    database: str,
    table: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> None:
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database,
    )

    client.command(f"TRUNCATE TABLE {database}.{table}")

    if not rows:
        return

    columns = [
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
        "p95_latency_ms",
        "success_rate",
        "error_rate",
    ]

    data = [[row[column] for column in columns] for row in rows]

    client.insert(
        table=f"{database}.{table}",
        data=data,
        column_names=columns,
    )


def main() -> None:
    args = parse_args()

    rows = read_parquet_rows(args.input)
    load_rows_to_clickhouse(
        rows=rows,
        database=args.database,
        table=args.table,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    print(f"Loaded {len(rows)} rows into {args.database}.{args.table}")


if __name__ == "__main__":
    main()

