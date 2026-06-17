import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_guardrail_check_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_guardrail_rule_check_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_guardrail_daily_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy("date", "app_name", "rule_category", "action_taken").agg(
        F.count("*").alias("check_cnt_1d"),
        F.sum(F.when(F.col("triggered"), 1).otherwise(0)).alias("triggered_cnt_1d"),
        F.sum(F.when(F.col("action_taken") == "block", 1).otherwise(0)).alias("block_cnt_1d"),
        F.sum(F.when(F.col("action_taken") == "redact", 1).otherwise(0)).alias("redact_cnt_1d"),
        F.sum(F.when(F.col("action_taken") == "warn", 1).otherwise(0)).alias("warn_cnt_1d"),
        F.round(F.avg("guardrail_latency_ms"), 2).alias("avg_guardrail_latency_ms"),
        F.countDistinct("user_id").alias("distinct_user_cnt_1d"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-guardrail-daily-metrics")

    try:
        metrics = build_guardrail_daily_metrics(load_dwd_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_guardrail_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
