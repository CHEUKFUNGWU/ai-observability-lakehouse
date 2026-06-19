import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import build_retrieval_knowledge_base_request_1d_projection
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_retrieval_request_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_retrieval_knowledge_base_request_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_retrieval_daily_metrics(events: DataFrame) -> DataFrame:
    return build_retrieval_knowledge_base_request_1d_projection(
        events.groupBy(
            "date",
            "app_name",
            "knowledge_base_id",
            "embedding_model",
            "retrieval_strategy",
        ).agg(
            F.count("*").alias("retrieval_cnt_1d"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1d"),
            F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1d"),
            F.sum(F.when(F.col("returned_count") == 0, 1).otherwise(0)).alias("zero_result_cnt_1d"),
            F.sum("returned_count").alias("returned_cnt_1d"),
            F.sum("hit_count").alias("hit_cnt_1d"),
            F.round(F.avg("avg_similarity_score"), 4).alias("avg_similarity_score"),
            F.round(F.avg("total_latency_ms"), 2).alias("avg_total_latency_ms"),
            F.expr("percentile_approx(total_latency_ms, 0.95)").alias("p95_total_latency_ms"),
            F.round(F.avg("embedding_latency_ms"), 2).alias("avg_embedding_latency_ms"),
            F.round(F.avg("search_latency_ms"), 2).alias("avg_search_latency_ms"),
        )
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-retrieval-daily-metrics")

    try:
        metrics = build_retrieval_daily_metrics(load_dwd_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_retrieval_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
