import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.data_quality import split_valid_quarantine, validate_llm_events
from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from app.warehouse_contract import build_llm_request_projection
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_llm_request_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_QUARANTINE_OUTPUT_PATH = Path("data/warehouse/quarantine/dwd_ai_llm_request_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_llm_events(raw_events: DataFrame) -> DataFrame:
    return build_llm_request_projection(raw_events)


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--quarantine-output", type=Path, default=DEFAULT_QUARANTINE_OUTPUT_PATH)
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument("--show-sample", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-llm-events-batch")
    started_at = datetime.now(timezone.utc)

    try:
        ods_events = load_ods_events(spark, args.input)
        if args.start_date:
            ods_events = ods_events.filter(F.col("date") >= F.lit(args.start_date))
        if args.end_date:
            ods_events = ods_events.filter(F.col("date") <= F.lit(args.end_date))
        events = transform_llm_events(ods_events)
        validated_events = validate_llm_events(events)
        valid_events, quarantine_events = split_valid_quarantine(validated_events)

        input_rows = events.count()
        valid_count = valid_events.count()
        quarantine_count = quarantine_events.count()

        log_info(
            LOGGER,
            "dwd_llm_events_validated",
            rows=input_rows,
            valid=valid_count,
            quarantine=quarantine_count,
        )

        if args.show_sample:
            valid_events.printSchema()
            valid_events.show(5, truncate=False)

        write_parquet(valid_events, args.output)
        quarantine_events.write.mode("overwrite").partitionBy("date").parquet(str(args.quarantine_output))
        log_info(LOGGER, "dwd_llm_events_written", output=str(args.output))

        written_events = spark.read.parquet(str(args.output))
        log_info(LOGGER, "dwd_llm_events_verified", rows=written_events.count())
        append_pipeline_run(
            pipeline_name="spark_transform_llm_events",
            layer="dwd",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=input_rows,
            output_rows=valid_count,
            quarantine_rows=quarantine_count,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
