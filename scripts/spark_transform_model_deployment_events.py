import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_model_deployment_events_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_model_deployment_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_model_deployment_events(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)

    def event_col(name: str, default):
        return F.col(name) if name in source_columns else F.lit(default)

    return raw_events.select(
        F.col("deployment_id").cast("string").alias("deployment_id"),
        F.col("model_name").cast("string").alias("model_name"),
        F.col("model_version").cast("string").alias("model_version"),
        F.col("provider").cast("string").alias("provider"),
        F.col("deployment_action").cast("string").alias("deployment_action"),
        F.col("traffic_percentage").cast("double").alias("traffic_percentage"),
        F.col("target_environment").cast("string").alias("target_environment"),
        F.col("deployer_user_id").cast("string").alias("deployer_user_id"),
        event_col("deploy_reason", "").cast("string").alias("deploy_reason"),
        F.col("status").cast("string").alias("status"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_date("date").alias("date"),
    )


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--show-sample", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-model-deployment-events-batch")

    try:
        events = transform_model_deployment_events(load_ods_events(spark, args.input))
        if args.show_sample:
            events.printSchema()
            events.show(5, truncate=False)

        write_parquet(events, args.output)
        log_info(LOGGER, "dwd_model_deployment_events_written", rows=events.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
