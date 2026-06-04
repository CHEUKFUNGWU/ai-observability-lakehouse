import argparse
from datetime import datetime
from pathlib import Path

DEFAULT_RAW_PATH = Path("data/raw/mock_llm_requests/events.jsonl")
DEFAULT_ODS_PATH = Path("data/warehouse/ods/llm_request/events.parquet")
DEFAULT_DWD_PATH = Path("data/warehouse/llm_request/events.parquet")
DEFAULT_ADS_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")
DEFAULT_START_TIME = "2026-01-01T00:00:00+00:00"

from scripts.generate_mock_llm_logs import write_jsonl

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=str, default=DEFAULT_START_TIME)
    parser.add_argument("--raw-output", type=Path, default=DEFAULT_RAW_PATH)
    parser.add_argument("--ods-output", type=Path, default=DEFAULT_ODS_PATH)
    parser.add_argument("--dwd-output", type=Path, default=DEFAULT_DWD_PATH)
    parser.add_argument("--ads-output", type=Path, default=DEFAULT_ADS_PATH)
    return parser.parse_args()

from scripts.spark_build_ods_llm_events import build_ods_llm_events, load_source_events, write_ods_events


def build_ods(raw_path: Path, ods_path: Path) -> int:
    spark = build_spark_session()

    try:
        raw_events = load_source_events(spark, raw_path)
        ods_events = build_ods_llm_events(raw_events, source_name="mock_llm_generator")

        write_ods_events(ods_events, ods_path)

        written_events = spark.read.parquet(str(ods_path))
        return written_events.count()
    finally:
        spark.stop()

from scripts.spark_transform_llm_events import (
    build_spark_session,
    count_invalid_token_totals,
    load_ods_events,
    transform_llm_events,
    write_parquet,
)

def build_dwd(ods_path: Path, dwd_path: Path) -> int:
    spark = build_spark_session()

    try:
        ods_events = load_ods_events(spark, ods_path)
        events = transform_llm_events(ods_events)

        invalid_token_total_count = count_invalid_token_totals(events)
        if invalid_token_total_count > 0:
            raise ValueError("Found rows where total_tokens != prompt_tokens + completion_tokens")

        write_parquet(events, dwd_path)

        written_events = spark.read.parquet(str(dwd_path))
        return written_events.count()
    finally:
        spark.stop()

from scripts.spark_build_ads_llm_feature_daily_metrics import (
    build_feature_daily_metrics,
    load_dwd_events,
    write_ads_metrics,
)

def build_ads(dwd_path: Path, ads_path: Path) -> int:
    spark = build_spark_session()

    try:
        events = load_dwd_events(spark, dwd_path)
        metrics = build_feature_daily_metrics(events)

        write_ads_metrics(metrics, ads_path)

        written_metrics = spark.read.parquet(str(ads_path))
        return written_metrics.count()
    
    finally:
        spark.stop()

def main() -> None:
    args = parse_args()
    start_time = datetime.fromisoformat(args.start_time)

    write_jsonl(
        count=args.count,
        output_path=args.raw_output,
        seed=args.seed,
        start_time=start_time,
    )

    print(f"Generated raw events: {args.raw_output}")

    ods_rows = build_ods(args.raw_output, args.ods_output)
    print(f"Built ODS events: {args.ods_output} ({ods_rows} rows)")

    dwd_rows = build_dwd(args.ods_output, args.dwd_output)
    print(f"Built DWD events: {args.dwd_output} ({dwd_rows} rows)")

    ads_rows = build_ads(args.dwd_output, args.ads_output)
    print(f"Built ADS feature daily metrics: {args.ads_output} ({ads_rows} rows)")

if __name__ == "__main__":
    main()
