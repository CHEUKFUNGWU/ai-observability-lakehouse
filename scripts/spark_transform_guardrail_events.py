import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_guardrail_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_guardrail_check_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_guardrail_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("guardrail_event_id").cast("string").alias("guardrail_event_id"),
        event_col("trace_id", "").cast("string").alias("trace_id"),
        event_col("request_id", "").cast("string").alias("request_id"),
        event_col("run_id", "").cast("string").alias("run_id"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("feature_name").cast("string").alias("feature_name"),
        F.col("guardrail_stage").cast("string").alias("guardrail_stage"),
        F.col("rule_name").cast("string").alias("rule_name"),
        F.col("rule_category").cast("string").alias("rule_category"),
        F.col("triggered").cast("boolean").alias("triggered"),
        F.col("action_taken").cast("string").alias("action_taken"),
        F.col("severity").cast("string").alias("severity"),
        event_col("matched_pattern_hash", "").cast("string").alias("matched_pattern_hash"),
        event_col("input_text_length", 0).cast("int").alias("input_text_length"),
        F.col("guardrail_latency_ms").cast("int").alias("guardrail_latency_ms"),
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
    spark = build_spark_session("ai-observability-guardrail-events-batch")

    try:
        events = transform_guardrail_events(load_ods_events(spark, args.input))
        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        write_parquet(events, args.output)
        log_info(LOGGER, "dwd_guardrail_events_written", rows=events.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
