import argparse
import os
from pathlib import Path

os.environ.pop("SPARK_HOME", None)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT_PATH = Path("data/warehouse/agent_tool_call/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/agent_tool_daily_metrics.parquet")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ai-observability-ads-agent-tool-daily-metrics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_tool_calls(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_agent_tool_daily_metrics(tool_calls: DataFrame) -> DataFrame:
    return (
        tool_calls.groupBy("date", "agent_id", "tool_name", "tool_type")
        .agg(
            F.count("*").alias("tool_call_count"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
            F.sum("retry_count").alias("retry_count"),
            F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
            F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
            F.round(F.avg("result_size"), 2).alias("avg_result_size"),
            F.max("result_size").alias("max_result_size"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("success_count") / F.col("tool_call_count"), 4),
        )
        .withColumn(
            "error_rate",
            F.round(F.col("error_count") / F.col("tool_call_count"), 4),
        )
    )


def write_ads_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        tool_calls = load_tool_calls(spark, args.input)
        metrics = build_agent_tool_daily_metrics(tool_calls)
        write_ads_metrics(metrics, args.output)
        print(f"Built ADS agent tool daily metrics: {args.output} ({metrics.count()} rows)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
