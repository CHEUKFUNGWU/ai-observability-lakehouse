import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/agent_tool_call/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/agent_tool_daily_metrics.parquet")
LOGGER = get_logger(__name__)


def load_tool_calls(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_agent_tool_daily_metrics(tool_calls: DataFrame) -> DataFrame:
    return tool_calls.groupBy("date", "agent_id", "tool_name", "tool_type").agg(
        F.count("*").alias("tool_call_count"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
        F.sum("retry_count").alias("retry_count"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
        F.round(F.avg("result_size"), 2).alias("avg_result_size"),
        F.max("result_size").alias("max_result_size"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-dws-agent-tool-daily-metrics")
    try:
        tool_calls = load_tool_calls(spark, args.input)
        metrics = build_agent_tool_daily_metrics(tool_calls)
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_agent_tool_daily_metrics_written", output=str(args.output), rows=metrics.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
