import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_retrieval_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_retrieval_request_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_retrieval_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("retrieval_id").cast("string").alias("retrieval_id"),
        event_col("trace_id", "").cast("string").alias("trace_id"),
        event_col("run_id", "").cast("string").alias("run_id"),
        event_col("span_id", "").cast("string").alias("span_id"),
        event_col("request_id", "").cast("string").alias("request_id"),
        event_col("agent_id", "").cast("string").alias("agent_id"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("feature_name").cast("string").alias("feature_name"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("knowledge_base_id").cast("string").alias("knowledge_base_id"),
        event_col("knowledge_base_name", "").cast("string").alias("knowledge_base_name"),
        F.col("embedding_model").cast("string").alias("embedding_model"),
        F.col("retrieval_strategy").cast("string").alias("retrieval_strategy"),
        event_col("query_text_hash", "").cast("string").alias("query_text_hash"),
        event_col("query_length", 0).cast("int").alias("query_length"),
        F.col("top_k").cast("int").alias("top_k"),
        F.col("returned_count").cast("int").alias("returned_count"),
        F.col("hit_count").cast("int").alias("hit_count"),
        F.col("max_similarity_score").cast("double").alias("max_similarity_score"),
        F.col("min_similarity_score").cast("double").alias("min_similarity_score"),
        F.col("avg_similarity_score").cast("double").alias("avg_similarity_score"),
        F.col("embedding_latency_ms").cast("int").alias("embedding_latency_ms"),
        F.col("search_latency_ms").cast("int").alias("search_latency_ms"),
        F.col("total_latency_ms").cast("int").alias("total_latency_ms"),
        F.col("status").cast("string").alias("status"),
        F.col("error_type").cast("string").alias("error_type"),
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
    spark = build_spark_session("ai-observability-retrieval-events-batch")

    try:
        events = transform_retrieval_events(load_ods_events(spark, args.input))
        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        write_parquet(events, args.output)
        log_info(LOGGER, "dwd_retrieval_events_written", rows=events.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
