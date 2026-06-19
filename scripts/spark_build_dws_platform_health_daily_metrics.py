import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import build_platform_health_metric_projection
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path(
    "data/warehouse/ods/ods_ai_observability_platform_health_metrics_di/events.parquet"
)
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_platform_component_health_1d.parquet")
LOGGER = get_logger(__name__)


def load_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_platform_health_metrics(raw_metrics: DataFrame) -> DataFrame:
    return build_platform_health_metric_projection(raw_metrics)


def build_platform_health_daily_metrics(metrics: DataFrame) -> DataFrame:
    daily = metrics.groupBy("date", "component", "metric_name").agg(
        F.max("metric_value").alias("metric_value"),
        F.max("threshold").alias("threshold"),
    )
    return daily.withColumn("is_breach", F.col("metric_value") > F.col("threshold"))


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-platform-health-daily")
    try:
        raw_metrics = load_events(spark, args.input)
        metrics = build_platform_health_daily_metrics(transform_platform_health_metrics(raw_metrics))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_platform_health_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
