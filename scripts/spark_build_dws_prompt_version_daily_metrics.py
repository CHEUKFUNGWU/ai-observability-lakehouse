import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.warehouse_contract import build_prompt_version_request_1d_projection
from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_LLM_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_EVALUATION_INPUT_PATH = Path("data/warehouse/dwd/dwd_ai_evaluation_judgment_di/events.parquet")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/dws/dws_ai_prompt_version_request_1d.parquet")
LOGGER = get_logger(__name__)


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def build_prompt_version_daily_metrics(
    llm_events: DataFrame,
    evaluation_events: DataFrame | None = None,
) -> DataFrame:
    request_metrics = llm_events.groupBy("date", "prompt_id", "prompt_version", "model_name").agg(
        F.count("*").alias("request_cnt_1d"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1d"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1d"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
        F.sum("total_tokens").alias("total_token_cnt_1d"),
        F.sum("estimated_cost_usd").alias("estimated_cost_amt_1d"),
    )

    if evaluation_events is None:
        return build_prompt_version_request_1d_projection(
            request_metrics.withColumn("avg_evaluation_score", F.lit(None).cast("double"))
        )

    evaluation_scores = evaluation_events.groupBy(
        "date",
        F.col("evaluated_prompt_version").alias("prompt_version"),
        F.col("evaluated_model_name").alias("model_name"),
    ).agg(F.round(F.avg("score"), 4).alias("avg_evaluation_score"))

    return build_prompt_version_request_1d_projection(
        request_metrics.join(
            evaluation_scores,
            on=["date", "prompt_version", "model_name"],
            how="left",
        )
    )


def write_dws_metrics(metrics: DataFrame, output_path: Path) -> None:
    metrics.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-input", type=Path, default=DEFAULT_LLM_INPUT_PATH)
    parser.add_argument("--evaluation-input", type=Path, default=DEFAULT_EVALUATION_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--include-evaluations", action="store_true")
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-dws-prompt-version-daily-metrics")

    try:
        evaluation_events = load_dwd_events(spark, args.evaluation_input) if args.include_evaluations else None
        metrics = build_prompt_version_daily_metrics(load_dwd_events(spark, args.llm_input), evaluation_events)
        write_dws_metrics(metrics, args.output)
        log_info(LOGGER, "dws_prompt_version_metrics_written", rows=metrics.count(), output=str(args.output))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
