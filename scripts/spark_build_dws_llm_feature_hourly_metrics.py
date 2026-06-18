import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1h.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_feature_hourly_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy(
        "date",
        F.hour("created_at").alias("hour"),
        "app_name",
        "feature_name",
        "model_name",
    ).agg(
        F.count("*").alias("request_cnt_1h"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1h"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1h"),
        F.sum("prompt_tokens").alias("prompt_token_cnt_1h"),
        F.sum("completion_tokens").alias("completion_token_cnt_1h"),
        F.sum("total_tokens").alias("total_token_cnt_1h"),
        F.sum("estimated_cost_usd").alias("estimated_cost_amt_1h"),
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
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-llm-feature-hourly-metrics")

    try:
        metrics = build_feature_hourly_metrics(load_dwd_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_llm_feature_hourly_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
