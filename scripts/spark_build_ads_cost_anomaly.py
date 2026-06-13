import argparse
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import Window

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_spark_session

DEFAULT_INPUT_PATH = Path("data/warehouse/ads/llm_feature_daily_metrics.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/cost_anomaly_daily.parquet")
LOGGER = get_logger(__name__)


def build_cost_anomaly_metrics(metrics):
    window = Window.partitionBy("app_name", "feature_name", "model_name").orderBy("date")
    return (
        metrics.withColumn("prev_day_cost", F.lag("estimated_cost_usd").over(window))
        .withColumn(
            "cost_change_rate",
            F.when(
                F.col("prev_day_cost") > 0,
                (F.col("estimated_cost_usd") - F.col("prev_day_cost")) / F.col("prev_day_cost"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("is_anomaly", F.coalesce(F.col("cost_change_rate") > 2.0, F.lit(False)))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    from datetime import datetime, timezone

    started_at = datetime.now(timezone.utc)
    spark = build_spark_session("ai-observability-ads-cost-anomaly")
    try:
        metrics = spark.read.parquet(str(args.input))
        result = build_cost_anomaly_metrics(metrics)
        result.write.mode("overwrite").partitionBy("date").parquet(str(args.output))
        row_count = result.count()
        log_info(LOGGER, "ads_cost_anomaly_written", output=str(args.output), rows=row_count)
        append_pipeline_run(
            pipeline_name="spark_build_ads_cost_anomaly",
            layer="ads",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=metrics.count(),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
