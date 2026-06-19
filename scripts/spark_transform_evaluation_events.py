import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import build_evaluation_judgment_projection
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_evaluation_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_evaluation_judgment_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_evaluation_events(raw_events: DataFrame) -> DataFrame:
    return build_evaluation_judgment_projection(raw_events)


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-sample", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-evaluation-events-batch")

    try:
        events = transform_evaluation_events(load_ods_events(spark, args.input))
        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        write_parquet(events, args.output)
        log_info(LOGGER, "dwd_evaluation_events_written", rows=events.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
