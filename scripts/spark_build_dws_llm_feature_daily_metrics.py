import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_feature_daily_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy("date", "app_name", "feature_name", "model_name").agg(
        F.count("*").alias("request_count"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
        F.sum("prompt_tokens").alias("prompt_tokens"),
        F.sum("completion_tokens").alias("completion_tokens"),
        F.sum("total_tokens").alias("total_tokens"),
        F.sum("estimated_cost_usd").alias("estimated_cost_usd"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.max("latency_ms").alias("max_latency_ms"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-input-sample", action="store_true")
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-dws-feature-daily-metrics")

    try:
        events = load_dwd_events(spark, args.input)
        metrics = build_feature_daily_metrics(events)

        log_info(LOGGER, "dws_llm_input_loaded", input=str(args.input), rows=events.count())
        if args.show_input_sample:
            events.printSchema()
            events.show(5, truncate=False)

        metric_rows = metrics.count()
        log_info(LOGGER, "dws_llm_metrics_built", rows=metric_rows)
        metrics.orderBy("date", "app_name", "feature_name", "model_name").show(truncate=False)

        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_llm_metrics_written", output=str(args.output))

        written_metrics = spark.read.parquet(str(args.output))
        log_info(LOGGER, "dws_llm_metrics_verified", rows=written_metrics.count())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
