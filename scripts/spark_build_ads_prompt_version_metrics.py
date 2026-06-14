import argparse
from pathlib import Path

from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_paimon_spark_session

DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/prompt_version_daily_metrics.parquet")
DEFAULT_INPUT_TABLE = "paimon_lake.dwd.llm_request_events"
LOGGER = get_logger(__name__)


def build_prompt_version_metrics(events):
    return events.groupBy("date", "prompt_id", "prompt_version", "model_name").agg(
        F.count("*").alias("request_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
        F.sum("estimated_cost_usd").alias("estimated_cost_usd"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-table", type=str, default=DEFAULT_INPUT_TABLE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    from datetime import datetime, timezone

    started_at = datetime.now(timezone.utc)
    spark = build_paimon_spark_session("ai-observability-ads-prompt-version")
    try:
        events = spark.table(args.input_table)
        result = build_prompt_version_metrics(events)
        result.write.mode("overwrite").partitionBy("date").parquet(str(args.output))
        row_count = result.count()
        log_info(LOGGER, "ads_prompt_version_metrics_written", output=str(args.output), rows=row_count)
        append_pipeline_run(
            pipeline_name="spark_build_ads_prompt_version_metrics",
            layer="ads",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=events.count(),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
