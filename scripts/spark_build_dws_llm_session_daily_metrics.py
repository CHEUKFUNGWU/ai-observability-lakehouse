import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.warehouse_contract import build_llm_session_request_1d_projection
from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_LLM_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_FEEDBACK_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_feedback_action_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_session_request_1d.parquet")
LOGGER = get_logger(__name__)


def load_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_session_daily_metrics(llm_events: DataFrame, feedback_events: DataFrame) -> DataFrame:
    keys = ["date", "app_name", "feature_name", "session_id"]
    start_time_ms = F.col("created_at").cast("double") * F.lit(1000.0)
    end_time_ms = start_time_ms + F.col("latency_ms")
    per_session = llm_events.groupBy(*keys).agg(
        F.count("*").alias("turn_cnt"),
        F.sum("total_tokens").alias("token_cnt"),
        F.round(F.max(end_time_ms) - F.min(start_time_ms), 2).alias("session_duration_ms"),
    )
    resolved_sessions = feedback_events.groupBy(*keys).agg(
        F.max(
            F.when(
                (F.col("feedback_type") == "thumbs_up")
                | ((F.col("feedback_type") == "rating") & (F.col("rating_value") >= 4)),
                1,
            ).otherwise(0)
        ).alias("is_resolved")
    )

    return build_llm_session_request_1d_projection(
        per_session.join(resolved_sessions, on=keys, how="left")
        .fillna({"is_resolved": 0})
        .groupBy("date", "app_name", "feature_name")
        .agg(
            F.count("*").alias("session_cnt_1d"),
            F.round(F.avg("turn_cnt"), 2).alias("avg_turns_per_session"),
            F.round(F.avg("token_cnt"), 2).alias("avg_tokens_per_session"),
            F.round(F.avg("session_duration_ms"), 2).alias("avg_duration_per_session_ms"),
            F.sum("is_resolved").alias("resolved_session_cnt_1d"),
        )
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-input", type=Path, default=DEFAULT_LLM_INPUT_PATH)
    parser.add_argument("--feedback-input", type=Path, default=DEFAULT_FEEDBACK_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-llm-session-daily-metrics")

    try:
        metrics = build_session_daily_metrics(
            load_parquet(spark, args.llm_input),
            load_parquet(spark, args.feedback_input),
        )
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_llm_session_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
