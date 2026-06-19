import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.warehouse_contract import build_cost_team_request_1d_projection
from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_LLM_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_USER_DIM_PATH = Path("data/warehouse/dim/dim_user_df.parquet")
DEFAULT_AGENT_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_run_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_cost_team_request_1d.parquet")
LOGGER = get_logger(__name__)


def load_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_cost_team_daily_metrics(
    llm_events: DataFrame,
    user_dim: DataFrame,
    agent_runs: DataFrame | None = None,
) -> DataFrame:
    user_team = user_dim.select("user_id", "team_id")
    llm_joined = llm_events.join(user_team, on="user_id", how="left").withColumn(
        "team_id", F.coalesce(F.col("team_id"), F.lit("unknown"))
    )
    llm_metrics = llm_joined.groupBy("date", "team_id", "app_name", "model_name").agg(
        F.count("*").alias("request_cnt_1d"),
        F.sum("total_tokens").alias("total_token_cnt_1d"),
        F.sum("estimated_cost_usd").alias("estimated_cost_amt_1d"),
    )

    if agent_runs is None:
        return build_cost_team_request_1d_projection(
            llm_metrics.withColumn("agent_run_cnt_1d", F.lit(0).cast("bigint"))
            .withColumn("agent_cost_amt_1d", F.lit(0.0).cast("double"))
        )

    agent_joined = agent_runs.join(user_team, on="user_id", how="left").withColumn(
        "team_id", F.coalesce(F.col("team_id"), F.lit("unknown"))
    )
    agent_metrics = agent_joined.groupBy("date", "team_id", "app_name").agg(
        F.count("*").alias("agent_run_cnt_1d"),
        F.sum("estimated_cost_usd").alias("agent_cost_amt_1d"),
    )

    return build_cost_team_request_1d_projection(
        llm_metrics.join(agent_metrics, on=["date", "team_id", "app_name"], how="left")
        .fillna({"agent_run_cnt_1d": 0, "agent_cost_amt_1d": 0.0})
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-input", type=Path, default=DEFAULT_LLM_INPUT_PATH)
    parser.add_argument("--user-dim", type=Path, default=DEFAULT_USER_DIM_PATH)
    parser.add_argument("--agent-input", type=Path, default=DEFAULT_AGENT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--include-agent-runs", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-cost-team-daily-metrics")

    try:
        agent_runs = load_parquet(spark, args.agent_input) if args.include_agent_runs else None
        metrics = build_cost_team_daily_metrics(
            load_parquet(spark, args.llm_input),
            load_parquet(spark, args.user_dim),
            agent_runs,
        )
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_cost_team_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
