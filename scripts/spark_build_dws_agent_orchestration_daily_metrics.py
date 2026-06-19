import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import build_agent_orchestration_handoff_1d_projection
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_orchestration_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_agent_orchestration_handoff_1d.parquet")
LOGGER = get_logger(__name__)


def load_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_agent_orchestration_daily_metrics(events: DataFrame) -> DataFrame:
    keys = ["date", "parent_agent_id", "child_agent_id", "handoff_type"]
    return build_agent_orchestration_handoff_1d_projection(
        events.groupBy(*keys).agg(
            F.count("*").alias("handoff_cnt_1d"),
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1d"),
            F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1d"),
            F.sum(F.when(F.col("status") == "timeout", 1).otherwise(0)).alias("timeout_cnt_1d"),
            F.round(F.avg("handoff_latency_ms"), 2).alias("avg_handoff_latency_ms"),
            F.expr("percentile_approx(handoff_latency_ms, 0.95)").cast("long").alias("p95_handoff_latency_ms"),
        )
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-agent-orchestration-daily")
    try:
        metrics = build_agent_orchestration_daily_metrics(load_events(spark, args.input))
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_agent_orchestration_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
