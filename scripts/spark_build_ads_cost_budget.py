import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_cost_team_request_1d.parquet")
DEFAULT_TEAM_DIM_PATH = Path("data/warehouse/dim/dim_team_df.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_cost_daily_budget.parquet")
LOGGER = get_logger(__name__)


def load_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_cost_budget_daily(metrics: DataFrame, team_dim: DataFrame) -> DataFrame:
    daily = metrics.groupBy("date", "team_id", "app_name").agg(
        F.sum("request_cnt_1d").alias("request_cnt_1d"),
        F.sum("total_token_cnt_1d").alias("total_token_cnt_1d"),
        F.sum("estimated_cost_amt_1d").alias("estimated_cost_amt_1d"),
        F.sum("agent_run_cnt_1d").alias("agent_run_cnt_1d"),
        F.sum("agent_cost_amt_1d").alias("agent_cost_amt_1d"),
    )
    enriched = daily.join(
        team_dim.select("team_id", "team_name", "department", "cost_center", "budget_monthly_usd"),
        on="team_id",
        how="left",
    )
    month_window = (
        Window.partitionBy("team_id", "app_name", F.trunc("date", "month"))
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    total_cost = F.col("estimated_cost_amt_1d") + F.col("agent_cost_amt_1d")
    day_of_month = F.dayofmonth("date")
    days_in_month = F.dayofmonth(F.last_day("date"))
    mtd_cost = F.sum(total_cost).over(month_window)
    projected_month_end = F.when(day_of_month > 0, mtd_cost / day_of_month * days_in_month)

    return (
        enriched.withColumn("total_cost_amt_1d", total_cost)
        .withColumn("cost_mtd_amt", F.round(mtd_cost, 6))
        .withColumn("projected_month_end_cost_amt", F.round(projected_month_end, 6))
        .withColumn("budget_monthly_amt", F.col("budget_monthly_usd").cast("double"))
        .withColumn(
            "budget_utilization_rate_mtd",
            F.round(F.when(F.col("budget_monthly_amt") > 0, F.col("cost_mtd_amt") / F.col("budget_monthly_amt")), 4),
        )
        .withColumn("is_budget_breach", F.col("projected_month_end_cost_amt") > F.col("budget_monthly_amt"))
        .drop("budget_monthly_usd")
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--team-dim", type=Path, default=DEFAULT_TEAM_DIM_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-cost-budget")

    try:
        ads = build_cost_budget_daily(load_parquet(spark, args.input), load_parquet(spark, args.team_dim))
        write_ads(ads, args.output)
        log_info(LOGGER, "ads_cost_budget_written", rows=ads.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
