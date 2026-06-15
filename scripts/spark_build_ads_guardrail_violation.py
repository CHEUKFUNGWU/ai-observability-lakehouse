import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_guardrail_rule_check_1d.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_guardrail_daily_violation.parquet")
LOGGER = get_logger(__name__)


def load_dws_metrics(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_guardrail_violation(
    metrics: DataFrame,
    trigger_rate_max: float = 0.20,
    p95_latency_ms_max: int = 300,
) -> DataFrame:
    trigger_rate = F.when(F.col("check_cnt_1d") > 0, F.col("triggered_cnt_1d") / F.col("check_cnt_1d"))
    block_rate = F.when(F.col("check_cnt_1d") > 0, F.col("block_cnt_1d") / F.col("check_cnt_1d"))
    return (
        metrics.withColumn("trigger_rate_1d", F.round(trigger_rate, 4))
        .withColumn("block_rate_1d", F.round(block_rate, 4))
        .withColumn("trigger_rate_max", F.lit(trigger_rate_max).cast("double"))
        .withColumn("p95_latency_ms_max", F.lit(p95_latency_ms_max).cast("bigint"))
        .withColumn("is_trigger_rate_breach", F.col("trigger_rate_1d") > F.col("trigger_rate_max"))
        .withColumn("is_latency_breach", F.col("avg_guardrail_latency_ms") > F.col("p95_latency_ms_max"))
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-guardrail-violation")

    try:
        ads = build_guardrail_violation(load_dws_metrics(spark, args.input))
        write_ads(ads, args.output)
        log_info(LOGGER, "ads_guardrail_violation_written", rows=ads.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
