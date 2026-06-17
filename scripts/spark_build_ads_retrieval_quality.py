import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_retrieval_knowledge_base_request_1d.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_retrieval_daily_quality.parquet")
LOGGER = get_logger(__name__)


def load_dws_metrics(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_retrieval_quality(
    metrics: DataFrame,
    p95_total_latency_ms_max: int = 2000,
    zero_result_rate_max: float = 0.10,
) -> DataFrame:
    zero_rate = F.when(F.col("retrieval_cnt_1d") > 0, F.col("zero_result_cnt_1d") / F.col("retrieval_cnt_1d"))
    hit_rate = F.when(F.col("returned_cnt_1d") > 0, F.col("hit_cnt_1d") / F.col("returned_cnt_1d"))
    return (
        metrics.withColumn("zero_result_rate_1d", F.round(zero_rate, 4))
        .withColumn("hit_rate_1d", F.round(hit_rate, 4))
        .withColumn("p95_total_latency_ms_max", F.lit(p95_total_latency_ms_max).cast("bigint"))
        .withColumn("zero_result_rate_max", F.lit(zero_result_rate_max).cast("double"))
        .withColumn("is_latency_breach", F.col("p95_total_latency_ms") > F.col("p95_total_latency_ms_max"))
        .withColumn("is_zero_result_breach", F.col("zero_result_rate_1d") > F.col("zero_result_rate_max"))
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-retrieval-quality")

    try:
        ads = build_retrieval_quality(load_dws_metrics(spark, args.input))
        write_ads(ads, args.output)
        log_info(LOGGER, "ads_retrieval_quality_written", rows=ads.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
