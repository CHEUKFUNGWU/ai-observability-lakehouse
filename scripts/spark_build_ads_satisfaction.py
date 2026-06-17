import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_FEEDBACK_INPUT_PATH = Path("data/warehouse/dws/dws_ai_feedback_feature_action_1d.parquet")
DEFAULT_LLM_INPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_feedback_daily_satisfaction.parquet")
LOGGER = get_logger(__name__)


def load_dws_metrics(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_satisfaction_daily(
    feedback_metrics: DataFrame,
    llm_metrics: DataFrame | None = None,
    satisfaction_rate_min: float = 0.80,
    regeneration_rate_max: float = 0.15,
) -> DataFrame:
    base = feedback_metrics
    if llm_metrics is not None:
        requests = llm_metrics.groupBy("date", "app_name", "feature_name").agg(
            F.sum("request_count").alias("request_cnt_1d")
        )
        base = base.join(requests, on=["date", "app_name", "feature_name"], how="left")
    else:
        base = base.withColumn("request_cnt_1d", F.lit(None).cast("bigint"))

    feedback_votes = F.col("thumbs_up_cnt_1d") + F.col("thumbs_down_cnt_1d")
    satisfaction_rate = F.when(feedback_votes > 0, F.col("thumbs_up_cnt_1d") / feedback_votes)
    regeneration_rate = F.when(F.col("request_cnt_1d") > 0, F.col("regenerate_cnt_1d") / F.col("request_cnt_1d"))

    return (
        base.withColumn("satisfaction_rate_1d", F.round(satisfaction_rate, 4))
        .withColumn("regeneration_rate_1d", F.round(regeneration_rate, 4))
        .withColumn("satisfaction_rate_min", F.lit(satisfaction_rate_min).cast("double"))
        .withColumn("regeneration_rate_max", F.lit(regeneration_rate_max).cast("double"))
        .withColumn("is_satisfaction_breach", F.col("satisfaction_rate_1d") < F.col("satisfaction_rate_min"))
        .withColumn("is_regeneration_breach", F.col("regeneration_rate_1d") > F.col("regeneration_rate_max"))
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-input", type=Path, default=DEFAULT_FEEDBACK_INPUT_PATH)
    parser.add_argument("--llm-input", type=Path, default=DEFAULT_LLM_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-satisfaction")

    try:
        ads = build_satisfaction_daily(
            load_dws_metrics(spark, args.feedback_input),
            load_dws_metrics(spark, args.llm_input),
        )
        write_ads(ads, args.output)
        log_info(LOGGER, "ads_satisfaction_written", rows=ads.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
