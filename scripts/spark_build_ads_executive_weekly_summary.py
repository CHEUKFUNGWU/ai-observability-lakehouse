import argparse
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_LLM_INPUT_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet")
DEFAULT_AGENT_INPUT_PATH = Path("data/warehouse/dws/dws_ai_agent_agent_run_1d.parquet")
DEFAULT_RETRIEVAL_INPUT_PATH = Path("data/warehouse/dws/dws_ai_retrieval_knowledge_base_request_1d.parquet")
DEFAULT_FEEDBACK_INPUT_PATH = Path("data/warehouse/dws/dws_ai_feedback_feature_action_1d.parquet")
DEFAULT_GUARDRAIL_INPUT_PATH = Path("data/warehouse/dws/dws_ai_guardrail_rule_check_1d.parquet")
DEFAULT_EVALUATION_INPUT_PATH = Path("data/warehouse/dws/dws_ai_evaluation_feature_judgment_1d.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_executive_weekly_summary.parquet")
LOGGER = get_logger(__name__)


def load_parquet(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def _week_start() -> F.Column:
    return F.date_trunc("week", F.col("date")).cast("date")


def build_executive_weekly_summary(
    llm_metrics: DataFrame,
    agent_metrics: DataFrame,
    retrieval_metrics: DataFrame,
    feedback_metrics: DataFrame,
    guardrail_metrics: DataFrame,
    evaluation_metrics: DataFrame,
) -> DataFrame:
    keys = ["week_start_date", "app_name"]
    llm = llm_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("request_count").alias("request_cnt_1w"),
        F.sum("success_count").alias("success_cnt_1w"),
        F.sum("error_count").alias("error_cnt_1w"),
        F.sum("total_tokens").alias("total_token_cnt_1w"),
        F.sum("estimated_cost_usd").alias("llm_cost_amt_1w"),
        F.sum(F.col("avg_latency_ms") * F.col("request_count")).alias("latency_weighted_sum"),
        F.max("p95_latency_ms").alias("p95_latency_ms_max"),
    )
    agent = agent_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("run_count").alias("agent_run_cnt_1w"),
        F.sum("success_count").alias("agent_success_cnt_1w"),
        F.sum("error_count").alias("agent_error_cnt_1w"),
        F.sum("estimated_cost_usd").alias("agent_cost_amt_1w"),
    )
    retrieval = retrieval_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("retrieval_cnt_1d").alias("retrieval_cnt_1w"),
        F.sum("returned_cnt_1d").alias("retrieval_returned_cnt_1w"),
        F.sum("hit_cnt_1d").alias("retrieval_hit_cnt_1w"),
    )
    feedback = feedback_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("feedback_cnt_1d").alias("feedback_cnt_1w"),
        F.sum("thumbs_up_cnt_1d").alias("thumbs_up_cnt_1w"),
        F.sum("thumbs_down_cnt_1d").alias("thumbs_down_cnt_1w"),
    )
    guardrail = guardrail_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("check_cnt_1d").alias("guardrail_check_cnt_1w"),
        F.sum("triggered_cnt_1d").alias("guardrail_triggered_cnt_1w"),
        F.sum("block_cnt_1d").alias("guardrail_block_cnt_1w"),
    )
    evaluation = evaluation_metrics.groupBy(_week_start().alias("week_start_date"), "app_name").agg(
        F.sum("evaluation_cnt_1d").alias("evaluation_cnt_1w"),
        F.sum("pass_cnt_1d").alias("evaluation_pass_cnt_1w"),
        F.sum("fail_cnt_1d").alias("evaluation_fail_cnt_1w"),
        F.sum(F.col("avg_score") * F.col("evaluation_cnt_1d")).alias("evaluation_score_weighted_sum"),
    )

    joined = reduce(lambda left, right: left.join(right, on=keys, how="full"), [llm, agent, retrieval, feedback, guardrail, evaluation])
    zero_columns = [
        "request_cnt_1w", "success_cnt_1w", "error_cnt_1w", "total_token_cnt_1w", "llm_cost_amt_1w",
        "latency_weighted_sum", "agent_run_cnt_1w", "agent_success_cnt_1w", "agent_error_cnt_1w",
        "agent_cost_amt_1w", "retrieval_cnt_1w", "retrieval_returned_cnt_1w", "retrieval_hit_cnt_1w",
        "feedback_cnt_1w", "thumbs_up_cnt_1w", "thumbs_down_cnt_1w", "guardrail_check_cnt_1w",
        "guardrail_triggered_cnt_1w", "guardrail_block_cnt_1w", "evaluation_cnt_1w",
        "evaluation_pass_cnt_1w", "evaluation_fail_cnt_1w", "evaluation_score_weighted_sum",
    ]
    satisfaction_denominator = F.col("thumbs_up_cnt_1w") + F.col("thumbs_down_cnt_1w")

    return (
        joined.fillna(0, subset=zero_columns)
        .withColumn(
            "avg_latency_ms",
            F.round(F.when(F.col("request_cnt_1w") > 0, F.col("latency_weighted_sum") / F.col("request_cnt_1w")), 2),
        )
        .withColumn(
            "retrieval_hit_rate_1w",
            F.round(F.when(F.col("retrieval_returned_cnt_1w") > 0, F.col("retrieval_hit_cnt_1w") / F.col("retrieval_returned_cnt_1w")), 4),
        )
        .withColumn(
            "satisfaction_rate_1w",
            F.round(F.when(satisfaction_denominator > 0, F.col("thumbs_up_cnt_1w") / satisfaction_denominator), 4),
        )
        .withColumn(
            "evaluation_pass_rate_1w",
            F.round(F.when(F.col("evaluation_cnt_1w") > 0, F.col("evaluation_pass_cnt_1w") / F.col("evaluation_cnt_1w")), 4),
        )
        .withColumn(
            "avg_evaluation_score",
            F.round(F.when(F.col("evaluation_cnt_1w") > 0, F.col("evaluation_score_weighted_sum") / F.col("evaluation_cnt_1w")), 4),
        )
        .withColumn("total_ai_cost_amt_1w", F.col("llm_cost_amt_1w") + F.col("agent_cost_amt_1w"))
        .drop("latency_weighted_sum", "evaluation_score_weighted_sum")
    )


def write_ads(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("week_start_date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-input", type=Path, default=DEFAULT_LLM_INPUT_PATH)
    parser.add_argument("--agent-input", type=Path, default=DEFAULT_AGENT_INPUT_PATH)
    parser.add_argument("--retrieval-input", type=Path, default=DEFAULT_RETRIEVAL_INPUT_PATH)
    parser.add_argument("--feedback-input", type=Path, default=DEFAULT_FEEDBACK_INPUT_PATH)
    parser.add_argument("--guardrail-input", type=Path, default=DEFAULT_GUARDRAIL_INPUT_PATH)
    parser.add_argument("--evaluation-input", type=Path, default=DEFAULT_EVALUATION_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-ads-executive-weekly-summary")

    try:
        summary = build_executive_weekly_summary(
            load_parquet(spark, args.llm_input), load_parquet(spark, args.agent_input),
            load_parquet(spark, args.retrieval_input), load_parquet(spark, args.feedback_input),
            load_parquet(spark, args.guardrail_input), load_parquet(spark, args.evaluation_input),
        )
        write_ads(summary, args.output)
        log_info(LOGGER, "ads_executive_weekly_summary_written", rows=summary.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
