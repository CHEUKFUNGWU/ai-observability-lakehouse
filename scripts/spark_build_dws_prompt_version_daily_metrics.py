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
UNKNOWN_PROMPT_METADATA = "unknown"
PROMPT_KEYS = ("date", "prompt_id", "prompt_version", "model_name")


def load_dwd_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def _ensure_column(events: DataFrame, name: str, value) -> DataFrame:
    if name in events.columns:
        return events
    return events.withColumn(name, F.lit(value))


def _normalized_text_column(name: str, default: str = UNKNOWN_PROMPT_METADATA):
    return F.when(F.col(name).isNull() | (F.trim(F.col(name)) == ""), F.lit(default)).otherwise(F.col(name))


def _normalize_prompt_request_keys(llm_events: DataFrame) -> DataFrame:
    events = llm_events
    for column in ("request_id", "prompt_id", "prompt_version", "model_name"):
        events = _ensure_column(events, column, "")
    return (
        events.withColumn("prompt_id", _normalized_text_column("prompt_id"))
        .withColumn("prompt_version", _normalized_text_column("prompt_version"))
        .withColumn("model_name", _normalized_text_column("model_name"))
    )


def _metadata_conflicts_by_prompt_key(llm_events: DataFrame) -> DataFrame:
    request_keys = (
        llm_events.where(F.col("request_id").isNotNull() & (F.trim(F.col("request_id")) != ""))
        .select("date", "request_id", "prompt_id", "prompt_version", "model_name")
        .dropDuplicates()
    )
    conflict_request_ids = (
        request_keys.groupBy("date", "request_id")
        .agg(F.count("*").alias("prompt_metadata_key_cnt"))
        .where(F.col("prompt_metadata_key_cnt") > 1)
        .select("date", "request_id")
    )
    return (
        request_keys.join(conflict_request_ids, on=["date", "request_id"], how="inner")
        .groupBy(*PROMPT_KEYS)
        .agg(F.countDistinct("request_id").alias("metadata_conflict_cnt_1d"))
    )


def _unique_request_prompt_keys(llm_events: DataFrame) -> DataFrame:
    request_keys = (
        llm_events.where(F.col("request_id").isNotNull() & (F.trim(F.col("request_id")) != ""))
        .select("date", "request_id", "prompt_id", "prompt_version", "model_name")
        .dropDuplicates()
    )
    key_counts = request_keys.groupBy("date", "request_id").agg(F.count("*").alias("prompt_metadata_key_cnt"))
    return (
        request_keys.join(key_counts, on=["date", "request_id"], how="inner")
        .where(F.col("prompt_metadata_key_cnt") == 1)
        .drop("prompt_metadata_key_cnt")
    )


def _build_evaluation_metrics(llm_events: DataFrame, evaluation_events: DataFrame) -> DataFrame:
    evaluations = evaluation_events
    for column in ("request_id", "evaluated_prompt_version", "evaluated_model_name"):
        evaluations = _ensure_column(evaluations, column, "")
    evaluations = _ensure_column(evaluations, "score", None)
    evaluations = _ensure_column(evaluations, "passed", None)

    request_keys = _unique_request_prompt_keys(llm_events).alias("request_keys")
    joined = evaluations.alias("evaluations").join(
        request_keys,
        on=[
            F.col("evaluations.date") == F.col("request_keys.date"),
            F.col("evaluations.request_id") == F.col("request_keys.request_id"),
        ],
        how="left",
    )

    attributed = joined.select(
        F.col("evaluations.date").alias("date"),
        F.coalesce(F.col("request_keys.prompt_id"), F.lit(UNKNOWN_PROMPT_METADATA)).alias("prompt_id"),
        F.coalesce(
            F.col("request_keys.prompt_version"),
            F.when(
                F.col("evaluations.evaluated_prompt_version").isNull()
                | (F.trim(F.col("evaluations.evaluated_prompt_version")) == ""),
                F.lit(UNKNOWN_PROMPT_METADATA),
            ).otherwise(F.col("evaluations.evaluated_prompt_version")),
        ).alias("prompt_version"),
        F.coalesce(
            F.col("request_keys.model_name"),
            F.when(
                F.col("evaluations.evaluated_model_name").isNull()
                | (F.trim(F.col("evaluations.evaluated_model_name")) == ""),
                F.lit(UNKNOWN_PROMPT_METADATA),
            ).otherwise(F.col("evaluations.evaluated_model_name")),
        ).alias("model_name"),
        F.col("evaluations.score").cast("double").alias("score"),
        F.col("evaluations.passed").cast("boolean").alias("passed"),
    )

    return attributed.groupBy(*PROMPT_KEYS).agg(
        F.count("*").alias("evaluation_cnt_1d"),
        F.sum(F.when(F.col("passed") == F.lit(True), 1).otherwise(0)).alias("pass_cnt_1d"),
        F.sum(F.when(F.col("passed") == F.lit(False), 1).otherwise(0)).alias("fail_cnt_1d"),
        F.coalesce(F.sum("score"), F.lit(0.0)).alias("evaluation_score_num_1d"),
        F.sum(F.when(F.col("score").isNotNull(), 1).otherwise(0)).alias("evaluation_score_den_1d"),
    )


def _with_evaluation_defaults(metrics: DataFrame) -> DataFrame:
    return (
        metrics.withColumn("evaluation_cnt_1d", F.lit(0).cast("bigint"))
        .withColumn("pass_cnt_1d", F.lit(0).cast("bigint"))
        .withColumn("fail_cnt_1d", F.lit(0).cast("bigint"))
        .withColumn("evaluation_score_num_1d", F.lit(0.0).cast("double"))
        .withColumn("evaluation_score_den_1d", F.lit(0).cast("bigint"))
        .withColumn("avg_evaluation_score", F.lit(None).cast("double"))
    )


def build_prompt_version_daily_metrics(
    llm_events: DataFrame,
    evaluation_events: DataFrame | None = None,
) -> DataFrame:
    normalized_llm_events = _normalize_prompt_request_keys(llm_events)
    request_metrics = normalized_llm_events.groupBy(*PROMPT_KEYS).agg(
        F.count("*").alias("request_cnt_1d"),
        F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("success_cnt_1d"),
        F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_cnt_1d"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.expr("percentile_approx(latency_ms, 0.95)").alias("p95_latency_ms"),
        F.sum("total_tokens").alias("total_token_cnt_1d"),
        F.sum("estimated_cost_usd").alias("estimated_cost_amt_1d"),
    )
    metadata_conflicts = _metadata_conflicts_by_prompt_key(normalized_llm_events)

    if evaluation_events is None:
        enriched = _with_evaluation_defaults(request_metrics)
    else:
        evaluation_metrics = _build_evaluation_metrics(normalized_llm_events, evaluation_events)
        enriched = request_metrics.join(
            evaluation_metrics,
            on=list(PROMPT_KEYS),
            how="left",
        )
        for column in ("evaluation_cnt_1d", "pass_cnt_1d", "fail_cnt_1d", "evaluation_score_den_1d"):
            enriched = enriched.withColumn(column, F.coalesce(F.col(column), F.lit(0)).cast("bigint"))
        enriched = enriched.withColumn(
            "evaluation_score_num_1d",
            F.coalesce(F.col("evaluation_score_num_1d"), F.lit(0.0)).cast("double"),
        ).withColumn(
            "avg_evaluation_score",
            F.when(
                F.col("evaluation_score_den_1d") > 0,
                F.round(F.col("evaluation_score_num_1d") / F.col("evaluation_score_den_1d"), 4),
            ).otherwise(F.lit(None).cast("double")),
        )

    return build_prompt_version_request_1d_projection(
        enriched.join(
            metadata_conflicts,
            on=list(PROMPT_KEYS),
            how="left",
        ).withColumn("metadata_conflict_cnt_1d", F.coalesce(F.col("metadata_conflict_cnt_1d"), F.lit(0)).cast("bigint"))
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
