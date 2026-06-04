import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/agent_tool_call/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/agent_tool_call/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_agent_tool_call_events(ods_events: DataFrame) -> DataFrame:
    return ods_events.select(
        F.col("tool_call_id").cast("string").alias("tool_call_id"),
        F.col("span_id").cast("string").alias("span_id"),
        F.col("run_id").cast("string").alias("run_id"),
        F.col("trace_id").cast("string").alias("trace_id"),
        F.col("agent_id").cast("string").alias("agent_id"),
        F.col("tool_name").cast("string").alias("tool_name"),
        F.col("tool_type").cast("string").alias("tool_type"),
        F.col("arguments_json").cast("string").alias("arguments_json"),
        F.col("result_text").cast("string").alias("result_text"),
        F.col("result_size").cast("int").alias("result_size"),
        F.col("duration_ms").cast("int").alias("duration_ms"),
        F.col("status").cast("string").alias("status"),
        F.col("error_type").cast("string").alias("error_type"),
        F.col("retry_count").cast("int").alias("retry_count"),
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
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-agent-tool-calls-batch")
    try:
        tool_calls = transform_agent_tool_call_events(load_ods_events(spark, args.input))
        write_parquet(tool_calls, args.output)
        log_info(LOGGER, "dwd_agent_tool_calls_written", output=str(args.output), rows=tool_calls.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
