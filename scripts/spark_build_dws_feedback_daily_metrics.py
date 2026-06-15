import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_feedback_action_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_feedback_feature_action_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_feedback_daily_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy("date", "app_name", "feature_name", "agent_id").agg(
        F.count("*").alias("feedback_cnt_1d"),
        F.sum(F.when(F.col("feedback_type") == "thumbs_up", 1).otherwise(0)).alias("thumbs_up_cnt_1d"),
        F.sum(F.when(F.col("feedback_type") == "thumbs_down", 1).otherwise(0)).alias(
            "thumbs_down_cnt_1d"
        ),
        F.sum(F.when(F.col("feedback_type") == "regenerate", 1).otherwise(0)).alias(
            "regenerate_cnt_1d"
        ),
        F.sum(F.when(F.col("feedback_type") == "report", 1).otherwise(0)).alias("report_cnt_1d"),
        F.round(F.avg("rating_value"), 2).alias("avg_rating"),
        F.countDistinct("request_id").alias("rated_request_cnt_1d"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-feedback-daily-metrics")

    try:
        metrics = build_feedback_daily_metrics(load_dwd_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_feedback_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
