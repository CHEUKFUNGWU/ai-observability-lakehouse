import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_evaluation_judgment_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_evaluation_feature_judgment_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_evaluation_daily_metrics(events: DataFrame) -> DataFrame:
    return events.groupBy(
        "date",
        "app_name",
        "feature_name",
        "evaluation_dimension",
        "evaluated_model_name",
    ).agg(
        F.count("*").alias("evaluation_cnt_1d"),
        F.sum(F.when(F.col("passed"), 1).otherwise(0)).alias("pass_cnt_1d"),
        F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("fail_cnt_1d"),
        F.round(F.avg("score"), 4).alias("avg_score"),
        F.expr("percentile_approx(score, 0.10)").alias("p10_score"),
        F.round(F.avg("evaluation_latency_ms"), 2).alias("avg_evaluation_latency_ms"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-evaluation-daily-metrics")

    try:
        metrics = build_evaluation_daily_metrics(load_dwd_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_evaluation_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
