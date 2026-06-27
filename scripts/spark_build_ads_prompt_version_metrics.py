import argparse
from pathlib import Path

from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_paimon_spark_session

DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_prompt_prompt_version_metrics.parquet")
DEFAULT_INPUT_TABLE = "paimon_lake.dws.dws_ai_prompt_version_request_1d"
LOGGER = get_logger(__name__)


def _ratio(numerator, denominator):
    return F.when(denominator > 0, F.round(numerator / denominator, 6)).otherwise(F.lit(None).cast("double"))


def build_prompt_version_metrics(metrics):
    return metrics.select(
        "date",
        "prompt_id",
        "prompt_version",
        "model_name",
        F.col("request_cnt_1d").alias("request_count"),
        F.col("success_cnt_1d").alias("success_count"),
        F.col("error_cnt_1d").alias("error_count"),
        F.col("total_token_cnt_1d").alias("total_tokens"),
        F.col("estimated_cost_amt_1d").alias("estimated_cost_usd"),
        "avg_latency_ms",
        "p95_latency_ms",
        F.col("evaluation_cnt_1d").alias("evaluation_count"),
        F.col("pass_cnt_1d").alias("pass_count"),
        F.col("fail_cnt_1d").alias("fail_count"),
        F.col("evaluation_score_num_1d").alias("evaluation_score_numerator"),
        F.col("evaluation_score_den_1d").alias("evaluation_score_denominator"),
        "avg_evaluation_score",
        F.col("metadata_conflict_cnt_1d").alias("metadata_conflict_count"),
    )


def build_prompt_version_comparison(metrics, group_by=("prompt_id", "prompt_version", "model_name")):
    daily_metrics = build_prompt_version_metrics(metrics)
    grouped = daily_metrics.groupBy(*group_by).agg(
        F.sum("request_count").alias("request_count"),
        F.sum("success_count").alias("success_count"),
        F.sum("error_count").alias("error_count"),
        F.sum("total_tokens").alias("total_tokens"),
        F.sum("estimated_cost_usd").alias("estimated_cost_usd"),
        F.when(
            F.sum("request_count") > 0,
            F.round(F.sum(F.col("avg_latency_ms") * F.col("request_count")) / F.sum("request_count"), 2),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("avg_latency_ms"),
        F.max("p95_latency_ms").alias("max_daily_p95_latency_ms"),
        F.sum("evaluation_count").alias("evaluation_count"),
        F.sum("pass_count").alias("pass_count"),
        F.sum("fail_count").alias("fail_count"),
        F.sum("evaluation_score_numerator").alias("evaluation_score_numerator"),
        F.sum("evaluation_score_denominator").alias("evaluation_score_denominator"),
        F.sum("metadata_conflict_count").alias("metadata_conflict_count"),
    )
    return (
        grouped.withColumn("success_rate", _ratio(F.col("success_count"), F.col("request_count")))
        .withColumn("error_rate", _ratio(F.col("error_count"), F.col("request_count")))
        .withColumn("pass_rate", _ratio(F.col("pass_count"), F.col("evaluation_count")))
        .withColumn("avg_evaluation_score", _ratio(F.col("evaluation_score_numerator"), F.col("evaluation_score_denominator")))
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
