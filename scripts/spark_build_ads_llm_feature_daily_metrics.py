import argparse
import os
from pathlib import Path

os.environ.pop("SPARK_HOME", None)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_INPUT_PATH = Path("data/warehouse/llm_request/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ai-observability-ads-feature-daily-metrics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))

def build_feature_daily_metrics(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("date", "app_name", "feature_name", "model_name")
        .agg(
            F.count("*").alias("request_count"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
            F.sum("prompt_tokens").alias("prompt_tokens"),
            F.sum("completion_tokens").alias("completion_tokens"),
            F.sum("total_tokens").alias("total_tokens"),
            F.sum("estimated_cost_usd").alias("estimated_cost_usd"),
            F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
            F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("success_count") / F.col("request_count"), 4),
        )
        .withColumn(
            "error_rate",
            F.round(F.col("error_count") / F.col("request_count"), 4),
        )
    )

def write_ads_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-input-sample", action="store_true")
    args = parser.parse_args()

    spark = build_spark_session()

    try:
        events = load_dwd_events(spark, args.input)
        metrics = build_feature_daily_metrics(events)

        print(f"Input: {args.input}")
        print(f"Rows: {events.count()}")
        if args.show_input_sample:
            events.printSchema()
            events.show(5, truncate=False)

        print(f"Metric rows: {metrics.count()}")
        metrics.orderBy("date", "app_name", "feature_name", "model_name").show(truncate=False)
        
        write_ads_metrics(metrics, args.output)
        print(f"Wrote ADS metrics to {args.output}")

        written_metrics = spark.read.parquet(str(args.output))
        print(f"Read back metric rows: {written_metrics.count()}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()