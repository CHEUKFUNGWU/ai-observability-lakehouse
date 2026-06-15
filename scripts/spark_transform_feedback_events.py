import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_feedback_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_feedback_action_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_feedback_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("feedback_id").cast("string").alias("feedback_id"),
        event_col("trace_id", "").cast("string").alias("trace_id"),
        event_col("request_id", "").cast("string").alias("request_id"),
        event_col("run_id", "").cast("string").alias("run_id"),
        F.col("session_id").cast("string").alias("session_id"),
        event_col("conversation_id", "").cast("string").alias("conversation_id"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("feature_name").cast("string").alias("feature_name"),
        event_col("agent_id", "").cast("string").alias("agent_id"),
        F.col("feedback_type").cast("string").alias("feedback_type"),
        F.col("rating_value").cast("int").alias("rating_value"),
        event_col("feedback_text_hash", "").cast("string").alias("feedback_text_hash"),
        event_col("feedback_text_length", 0).cast("int").alias("feedback_text_length"),
        F.col("response_latency_ms").cast("int").alias("response_latency_ms"),
        F.col("model_name").cast("string").alias("model_name"),
        F.col("prompt_version").cast("string").alias("prompt_version"),
        F.col("mode").cast("string").alias("mode"),
        F.col("environment").cast("string").alias("environment"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_date("date").alias("date"),
    )


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-sample", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-feedback-events-batch")

    try:
        events = transform_feedback_events(load_ods_events(spark, args.input))
        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        write_parquet(events, args.output)
        log_info(LOGGER, "dwd_feedback_events_written", rows=events.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
