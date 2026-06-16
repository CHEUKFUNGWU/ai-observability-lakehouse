import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_RUN_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_run_di/events.parquet")
DEFAULT_SPAN_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_span_di/events.parquet")
DEFAULT_USER_DIM_PATH = Path("data/warehouse/dim/dim_user_df.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_agent_team_run_1d.parquet")
LOGGER = get_logger(__name__)


def load_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_agent_team_daily_metrics(runs: DataFrame, spans: DataFrame, user_dim: DataFrame) -> DataFrame:
    user_team = user_dim.select("user_id", "team_id")
    runs_with_team = runs.join(user_team, on="user_id", how="left").withColumn(
        "team_id", F.coalesce(F.col("team_id"), F.lit("unknown"))
    )
    keys = ["date", "team_id", "app_name", "agent_id", "agent_name", "task_type"]
    run_span_keys = ["date", "agent_id", "run_id"]

    run_metrics = runs_with_team.groupBy(*keys).agg(
        F.count("*").alias("run_cnt_1d"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1d"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1d"),
        F.sum("turn_count").alias("turn_cnt_1d"),
        F.sum("llm_call_count").alias("llm_call_cnt_1d"),
        F.sum("tool_call_count").alias("tool_call_cnt_1d"),
        F.sum("retrieval_count").alias("retrieval_cnt_1d"),
        F.sum("total_tokens").alias("total_token_cnt_1d"),
        F.sum("estimated_cost_usd").alias("estimated_cost_amt_1d"),
        F.round(F.avg("duration_ms"), 2).alias("avg_duration_ms"),
        F.expr("percentile_approx(duration_ms, 0.95)").alias("p95_duration_ms"),
    )

    run_dimensions = runs_with_team.select(
        *run_span_keys,
        "team_id",
        "app_name",
        "agent_name",
        "task_type",
    )
    spans_with_run_dimensions = spans.join(run_dimensions, on=run_span_keys, how="inner")
    span_metrics = spans_with_run_dimensions.groupBy(*keys).agg(
        F.count("*").alias("span_cnt_1d"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("failed_span_cnt_1d"),
        F.sum(F.when(F.col("span_type") == "tool_call", 1).otherwise(0)).alias("tool_span_cnt_1d"),
        F.sum(F.when(F.col("span_type") == "llm_call", 1).otherwise(0)).alias("llm_span_cnt_1d"),
    )

    return run_metrics.join(span_metrics, on=keys, how="left").select(
        *keys,
        "run_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "turn_cnt_1d",
        "llm_call_cnt_1d",
        "tool_call_cnt_1d",
        "retrieval_cnt_1d",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "avg_duration_ms",
        "p95_duration_ms",
        F.coalesce(F.col("span_cnt_1d"), F.lit(0)).alias("span_cnt_1d"),
        F.coalesce(F.col("failed_span_cnt_1d"), F.lit(0)).alias("failed_span_cnt_1d"),
        F.coalesce(F.col("tool_span_cnt_1d"), F.lit(0)).alias("tool_span_cnt_1d"),
        F.coalesce(F.col("llm_span_cnt_1d"), F.lit(0)).alias("llm_span_cnt_1d"),
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-input", type=Path, default=DEFAULT_RUN_INPUT_PATH)
    parser.add_argument("--span-input", type=Path, default=DEFAULT_SPAN_INPUT_PATH)
    parser.add_argument("--user-dim", type=Path, default=DEFAULT_USER_DIM_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-agent-team-daily-metrics")

    try:
        metrics = build_agent_team_daily_metrics(
            load_events(spark, args.run_input),
            load_events(spark, args.span_input),
            load_events(spark, args.user_dim),
        )
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_agent_team_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
