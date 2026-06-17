import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_evaluation_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_evaluation_judgment_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_evaluation_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("evaluation_id").cast("string").alias("evaluation_id"),
        event_col("trace_id", "").cast("string").alias("trace_id"),
        event_col("request_id", "").cast("string").alias("request_id"),
        event_col("run_id", "").cast("string").alias("run_id"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("feature_name").cast("string").alias("feature_name"),
        F.col("evaluator_type").cast("string").alias("evaluator_type"),
        event_col("evaluator_model", "").cast("string").alias("evaluator_model"),
        F.col("evaluation_dimension").cast("string").alias("evaluation_dimension"),
        F.col("score").cast("double").alias("score"),
        event_col("raw_score", "").cast("string").alias("raw_score"),
        F.col("pass_threshold").cast("double").alias("pass_threshold"),
        F.col("passed").cast("boolean").alias("passed"),
        F.col("evaluated_model_name").cast("string").alias("evaluated_model_name"),
        F.col("evaluated_prompt_version").cast("string").alias("evaluated_prompt_version"),
        F.col("evaluation_latency_ms").cast("int").alias("evaluation_latency_ms"),
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
