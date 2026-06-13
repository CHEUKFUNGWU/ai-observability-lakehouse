import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.generate_mock_llm_logs import write_jsonl
from scripts.spark_build_ads_llm_feature_daily_metrics import (
    build_feature_daily_metrics,
    load_dwd_events,
    write_ads_metrics,
)
from scripts.spark_build_ods_llm_events import (
    build_ods_llm_events,
    load_source_events,
    write_ods_events,
)
from scripts.spark_transform_llm_events import (
    load_ods_events,
    transform_llm_events,
    write_parquet,
)
from app.data_quality import split_valid_quarantine, validate_llm_events
from scripts.spark_utils import build_spark_session


DEFAULT_RAW_PATH = Path("data/raw/mock_llm_requests/events.jsonl")
DEFAULT_ODS_PATH = Path("data/warehouse/ods/llm_request/events.parquet")
DEFAULT_DWD_PATH = Path("data/warehouse/llm_request/events.parquet")
DEFAULT_ADS_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")
DEFAULT_QUARANTINE_PATH = Path("data/warehouse/quarantine/llm_request/events.parquet")
DEFAULT_START_TIME = "2026-01-01T00:00:00+00:00"
LOGGER = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=str, default=DEFAULT_START_TIME)
    parser.add_argument("--raw-output", type=Path, default=DEFAULT_RAW_PATH)
    parser.add_argument("--ods-output", type=Path, default=DEFAULT_ODS_PATH)
    parser.add_argument("--dwd-output", type=Path, default=DEFAULT_DWD_PATH)
    parser.add_argument("--ads-output", type=Path, default=DEFAULT_ADS_PATH)
    parser.add_argument("--quarantine-output", type=Path, default=DEFAULT_QUARANTINE_PATH)
    return parser.parse_args()


def build_ods(spark: SparkSession, raw_path: Path, ods_path: Path) -> int:
    raw_events = load_source_events(spark, raw_path)
    ods_events = build_ods_llm_events(raw_events, source_name="mock_llm_generator")

    write_ods_events(ods_events, ods_path)

    written_events = spark.read.parquet(str(ods_path))
    return written_events.count()


def build_dwd(spark: SparkSession, ods_path: Path, dwd_path: Path, quarantine_path: Path) -> tuple[int, int]:
    ods_events = load_ods_events(spark, ods_path)
    events = transform_llm_events(ods_events)
    validated_events = validate_llm_events(events)
    valid_events, quarantine_events = split_valid_quarantine(validated_events)
    quarantine_count = quarantine_events.count()

    write_parquet(valid_events, dwd_path)
    if quarantine_count > 0:
        quarantine_events.write.mode("overwrite").partitionBy("date").parquet(str(quarantine_path))

    written_events = spark.read.parquet(str(dwd_path))
    return written_events.count(), quarantine_count


def build_ads(spark: SparkSession, dwd_path: Path, ads_path: Path) -> int:
    events = load_dwd_events(spark, dwd_path)
    metrics = build_feature_daily_metrics(events)

    write_ads_metrics(metrics, ads_path)

    written_metrics = spark.read.parquet(str(ads_path))
    return written_metrics.count()


def main() -> None:
    args = parse_args()
    start_time = datetime.fromisoformat(args.start_time)

    write_jsonl(
        count=args.count,
        output_path=args.raw_output,
        seed=args.seed,
        start_time=start_time,
    )
    log_info(LOGGER, "mock_llm_events_generated", output=str(args.raw_output), count=args.count)

    spark = build_spark_session("ai-observability-local-batch-pipeline")
    pipeline_started_at = datetime.now(timezone.utc)
    try:
        ods_rows = build_ods(spark, args.raw_output, args.ods_output)
        log_info(LOGGER, "ods_llm_events_built", output=str(args.ods_output), rows=ods_rows)

        dwd_rows, quarantine_rows = build_dwd(
            spark,
            args.ods_output,
            args.dwd_output,
            args.quarantine_output,
        )
        log_info(
            LOGGER,
            "dwd_llm_events_built",
            output=str(args.dwd_output),
            rows=dwd_rows,
            quarantine_rows=quarantine_rows,
        )

        ads_rows = build_ads(spark, args.dwd_output, args.ads_output)
        log_info(LOGGER, "ads_llm_metrics_built", output=str(args.ads_output), rows=ads_rows)
        append_pipeline_run(
            pipeline_name="run_local_batch_pipeline",
            layer="ads",
            start_time=pipeline_started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=ods_rows,
            output_rows=ads_rows,
            quarantine_rows=quarantine_rows,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
