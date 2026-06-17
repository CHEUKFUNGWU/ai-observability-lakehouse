import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_cost_team_request_1d.parquet")
DEFAULT_TEAM_DIM_PATH = Path("data/warehouse/dim/dim_team_df.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_cost_monthly_chargeback.parquet")
LOGGER = get_logger(__name__)


def load_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_cost_monthly_chargeback(metrics: DataFrame, team_dim: DataFrame) -> DataFrame:
    monthly = metrics.groupBy(
        F.trunc("date", "month").alias("month_start_date"),
        "team_id",
        "app_name",
    ).agg(
        F.sum("request_cnt_1d").alias("request_cnt_1m"),
        F.sum("total_token_cnt_1d").alias("total_token_cnt_1m"),
        F.sum("estimated_cost_amt_1d").alias("llm_cost_amt_1m"),
        F.sum("agent_run_cnt_1d").alias("agent_run_cnt_1m"),
        F.sum("agent_cost_amt_1d").alias("agent_cost_amt_1m"),
    )
    enriched = monthly.join(
        team_dim.select("team_id", "team_name", "department", "cost_center", "budget_monthly_usd"),
        on="team_id",
        how="left",
    )
    total_chargeback = F.col("llm_cost_amt_1m") + F.col("agent_cost_amt_1m")

    return (
        enriched.withColumn("chargeback_amt_1m", F.round(total_chargeback, 6))
        .withColumn("budget_monthly_amt", F.col("budget_monthly_usd").cast("double"))
        .withColumn("budget_variance_amt_1m", F.round(F.col("budget_monthly_amt") - F.col("chargeback_amt_1m"), 6))
        .withColumn(
            "budget_utilization_rate_1m",
            F.round(
                F.when(F.col("budget_monthly_amt") > 0, F.col("chargeback_amt_1m") / F.col("budget_monthly_amt")),
                4,
            ),
        )
        .withColumn("is_budget_overrun", F.col("chargeback_amt_1m") > F.col("budget_monthly_amt"))
        .drop("budget_monthly_usd")
        .select(
            "month_start_date",
            "team_id",
            "team_name",
            "department",
            "cost_center",
            "app_name",
            "request_cnt_1m",
            "total_token_cnt_1m",
            "llm_cost_amt_1m",
            "agent_run_cnt_1m",
            "agent_cost_amt_1m",
            "chargeback_amt_1m",
            "budget_monthly_amt",
            "budget_variance_amt_1m",
            "budget_utilization_rate_1m",
            "is_budget_overrun",
        )
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("month_start_date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--team-dim", type=Path, default=DEFAULT_TEAM_DIM_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-cost-monthly-chargeback")

    try:
        ads = build_cost_monthly_chargeback(load_parquet(spark, args.input), load_parquet(spark, args.team_dim))
        write_ads(ads, args.output)
        log_info(LOGGER, "ads_cost_monthly_chargeback_written", rows=ads.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
