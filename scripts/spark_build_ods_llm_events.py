import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/raw/mock_llm_requests/events.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ods/llm_request/events.parquet")
LOGGER = get_logger(__name__)


def load_source_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.json(str(input_path))


def build_ods_llm_events(raw_events: DataFrame, source_name: str) -> DataFrame:
    return (
        raw_events.withColumn("source_name", F.lit(source_name))
        .withColumn("source_event_type", F.lit("llm_request"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("raw_event_json", F.to_json(F.struct(*[F.col(c) for c in raw_events.columns])))
    )


def write_ods_events(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--source-name", type=str, default="mock_llm_generator")
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-ods-llm-events")
    try:
        raw_events = load_source_events(spark, args.input)
        ods_events = build_ods_llm_events(raw_events, args.source_name)
        write_ods_events(ods_events, args.output)
        log_info(LOGGER, "ods_llm_events_written", output=str(args.output), rows=ods_events.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
