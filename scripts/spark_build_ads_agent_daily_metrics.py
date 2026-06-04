import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_RUN_INPUT_PATH = Path("data/warehouse/agent_run/events.parquet")
DEFAULT_SPAN_INPUT_PATH = Path("data/warehouse/agent_span/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/agent_daily_metrics.parquet")
LOGGER = get_logger(__name__)


def load_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_agent_daily_metrics(runs: DataFrame, spans: DataFrame) -> DataFrame:
    keys = ["date", "app_name", "agent_id", "agent_name", "task_type"]

    run_metrics = runs.groupBy(*keys).agg(
        F.count("*").alias("run_count"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
        F.sum("turn_count").alias("turn_count"),
        F.sum("llm_call_count").alias("llm_call_count"),
        F.sum("tool_call_count").alias("tool_call_count"),
        F.sum("retrieval_count").alias("retrieval_count"),
        F.sum("total_tokens").alias("total_tokens"),
        F.sum("estimated_cost_usd").alias("estimated_cost_usd"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
    )

    span_metrics = spans.groupBy("date", "agent_id").agg(
        F.count("*").alias("span_count"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("failed_span_count"),
        F.sum(F.when(F.col("span_type") == "tool_call", 1).otherwise(0)).alias("tool_span_count"),
        F.sum(F.when(F.col("span_type") == "llm_call", 1).otherwise(0)).alias("llm_span_count"),
    )

    return run_metrics.join(span_metrics, on=["date", "agent_id"], how="left").withColumn(
        "span_failure_rate",
        F.round(F.col("failed_span_count") / F.col("span_count"), 4),
    )


def write_ads_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-input", type=Path, default=DEFAULT_RUN_INPUT_PATH)
    parser.add_argument("--span-input", type=Path, default=DEFAULT_SPAN_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-ads-agent-daily-metrics")
    try:
        runs = load_events(spark, args.run_input)
        spans = load_events(spark, args.span_input)
        metrics = build_agent_daily_metrics(runs, spans)
        write_ads_metrics(metrics, args.output)
        log_info(LOGGER, "ads_agent_daily_metrics_written", output=str(args.output), rows=metrics.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
