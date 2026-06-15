import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_RUN_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_agent_run_events_di/events.parquet")
DEFAULT_SPAN_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_agent_span_events_di/events.parquet")
DEFAULT_RUN_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_run_di/events.parquet")
DEFAULT_SPAN_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_span_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_agent_run_events(raw_runs: DataFrame) -> DataFrame:
    source_columns = set(raw_runs.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_runs.select(
        F.col("run_id").cast("string").alias("run_id"),
        F.col("trace_id").cast("string").alias("trace_id"),
        F.col("agent_id").cast("string").alias("agent_id"),
        F.col("agent_name").cast("string").alias("agent_name"),
        F.col("agent_version").cast("string").alias("agent_version"),
        F.col("app_name").cast("string").alias("app_name"),
        F.col("user_id").cast("string").alias("user_id"),
        F.col("session_id").cast("string").alias("session_id"),
        F.col("conversation_id").cast("string").alias("conversation_id"),
        F.col("task_type").cast("string").alias("task_type"),
        F.col("channel").cast("string").alias("channel"),
        event_col("toolsets_used", "[]").cast("string").alias("toolsets_used"),
        F.col("input_text_hash").cast("string").alias("input_text_hash"),
        F.col("output_text_hash").cast("string").alias("output_text_hash"),
        F.to_timestamp("start_time").alias("start_time"),
        F.to_timestamp("end_time").alias("end_time"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        F.col("status").cast("string").alias("status"),
        F.col("error_type").cast("string").alias("error_type"),
        F.col("turn_count").cast("int").alias("turn_count"),
        F.col("llm_call_count").cast("int").alias("llm_call_count"),
        F.col("tool_call_count").cast("int").alias("tool_call_count"),
        F.col("retrieval_count").cast("int").alias("retrieval_count"),
        F.col("total_tokens").cast("int").alias("total_tokens"),
        F.col("estimated_cost_usd").cast("double").alias("estimated_cost_usd"),
        F.col("mode").cast("string").alias("mode"),
        F.col("region").cast("string").alias("region"),
        F.col("environment").cast("string").alias("environment"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_date("date").alias("date"),
    )


def transform_agent_span_events(raw_spans: DataFrame) -> DataFrame:
    return raw_spans.select(
        F.col("span_id").cast("string").alias("span_id"),
        F.col("parent_span_id").cast("string").alias("parent_span_id"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("trace_id").cast("string").alias("trace_id"),
        F.col("agent_id").cast("string").alias("agent_id"),
        F.col("span_name").cast("string").alias("span_name"),
        F.col("span_type").cast("string").alias("span_type"),
        F.col("span_order").cast("int").alias("span_order"),
        F.to_timestamp("start_time").alias("start_time"),
        F.to_timestamp("end_time").alias("end_time"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        F.col("status").cast("string").alias("status"),
        F.col("error_type").cast("string").alias("error_type"),
        F.col("retry_count").cast("int").alias("retry_count"),
        F.col("input_size").cast("int").alias("input_size"),
        F.col("output_size").cast("int").alias("output_size"),
        F.col("model_name").cast("string").alias("model_name"),
        F.col("tool_name").cast("string").alias("tool_name"),
        F.col("mode").cast("string").alias("mode"),
        F.col("region").cast("string").alias("region"),
        F.col("environment").cast("string").alias("environment"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_date("date").alias("date"),
    )


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-input", type=Path, default=DEFAULT_RUN_INPUT_PATH)
    parser.add_argument("--span-input", type=Path, default=DEFAULT_SPAN_INPUT_PATH)
    parser.add_argument("--run-output", type=Path, default=DEFAULT_RUN_OUTPUT_PATH)
    parser.add_argument("--span-output", type=Path, default=DEFAULT_SPAN_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-agent-events-batch")
    try:
        runs = transform_agent_run_events(load_ods_events(spark, args.run_input))
        spans = transform_agent_span_events(load_ods_events(spark, args.span_input))
        write_parquet(runs, args.run_output)
        write_parquet(spans, args.span_output)
        log_info(LOGGER, "dwd_agent_runs_written", output=str(args.run_output), rows=runs.count())
        log_info(LOGGER, "dwd_agent_spans_written", output=str(args.span_output), rows=spans.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
