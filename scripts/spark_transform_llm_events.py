import argparse
import os
from pathlib import Path

os.environ.pop("SPARK_HOME", None)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ai-observability-llm-events-batch")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/llm_request/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/llm_request/events.parquet")


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_llm_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("request_id").cast("string").alias("request_id"),
        event_col("trace_id", "").cast("string").alias("trace_id"),
        event_col("run_id", "").cast("string").alias("run_id"),
        event_col("span_id", "").cast("string").alias("span_id"),
        event_col("agent_id", "").cast("string").alias("agent_id"),
        event_col("agent_name", "").cast("string").alias("agent_name"),
        event_col("channel", "").cast("string").alias("channel"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("session_id").cast("string").alias("session_id"),
        event_col("conversation_id", "").cast("string").alias("conversation_id"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("feature_name").cast("string").alias("feature_name"),
        F.col("prompt_category").cast("string").alias("prompt_category"),
        F.col("prompt_id").cast("string").alias("prompt_id"),
        F.col("prompt_version").cast("string").alias("prompt_version"),
        F.col("model_name").cast("string").alias("model_name"),
        F.col("provider").cast("string").alias("provider"),
        F.col("prompt_text").cast("string").alias("prompt_text"),
        F.col("response_text").cast("string").alias("response_text"),
        event_col("prompt_hash", "").cast("string").alias("prompt_hash"),
        event_col("response_hash", "").cast("string").alias("response_hash"),
        event_col("input_chars", 0).cast("int").alias("input_chars"),
        event_col("output_chars", 0).cast("int").alias("output_chars"),
        F.col("prompt_tokens").cast("int").alias("prompt_tokens"),
        F.col("completion_tokens").cast("int").alias("completion_tokens"),
        F.col("total_tokens").cast("int").alias("total_tokens"),
        event_col("request_type", "chat").cast("string").alias("request_type"),
        event_col("is_streaming", False).cast("boolean").alias("is_streaming"),
        event_col("temperature", 0.0).cast("double").alias("temperature"),
        event_col("max_tokens", 0).cast("int").alias("max_tokens"),
        event_col("finish_reason", "").cast("string").alias("finish_reason"),
        event_col("retry_count", 0).cast("int").alias("retry_count"),
        F.col("latency_ms").cast("int").alias("latency_ms"),
        F.col("status").cast("string").alias("status"),
        F.col("error_type").cast("string").alias("error_type"),
        F.col("http_status").cast("int").alias("http_status"),
        F.col("estimated_cost_usd").cast("double").alias("estimated_cost_usd"),
        F.col("mode").cast("string").alias("mode"),
        F.col("region").cast("string").alias("region"),
        F.col("environment").cast("string").alias("environment"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_date("date").alias("date"),
    )


def count_invalid_token_totals(events: DataFrame) -> int:
    return events.filter(
        F.col("total_tokens") != F.col("prompt_tokens") + F.col("completion_tokens")
    ).count()


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-sample", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session()

    try:
        ods_events = load_ods_events(spark, args.input)
        events = transform_llm_events(ods_events)
        invalid_token_total_count = count_invalid_token_totals(events)

        print(f"Rows: {events.count()}")
        print(f"Invalid token totals: {invalid_token_total_count}")

        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        if invalid_token_total_count > 0:
            raise ValueError("Found rows where total_tokens != prompt_tokens + completion_tokens")

        write_parquet(events, args.output)
        print(f"Wrote parquet to {args.output}")

        written_events = spark.read.parquet(str(args.output))
        print(f"Read back rows: {written_events.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
