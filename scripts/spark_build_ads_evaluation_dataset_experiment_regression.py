import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_paimon_spark_session


DEFAULT_EVALUATION_TABLE = "paimon_lake.dwd.dwd_ai_evaluation_judgment_di"
DEFAULT_LLM_REQUEST_TABLE = "paimon_lake.dwd.dwd_ai_llm_request_di"
DEFAULT_ASSIGNMENT_INPUT = Path("data/config/evaluation_experiment_assignments.parquet")
DEFAULT_COMPARISON_CONFIG_INPUT = Path("data/config/evaluation_experiment_comparisons.parquet")
DEFAULT_OUTPUT_PATH = Path(
    "data/warehouse/ads/ads_observability_evaluation_dataset_experiment_regression.parquet"
)
LOGGER = get_logger(__name__)


IDENTIFIER_COLUMNS = (
    "dataset_name",
    "experiment_name",
    "baseline_variant",
    "candidate_variant",
    "baseline_model_name",
    "baseline_prompt_version",
    "candidate_model_name",
    "candidate_prompt_version",
    "evaluation_dimension",
)

COMPONENT_COLUMNS = (
    "baseline_evaluation_count",
    "baseline_pass_count",
    "baseline_fail_count",
    "baseline_score_numerator",
    "baseline_score_denominator",
    "baseline_latency_ms_numerator",
    "baseline_latency_ms_denominator",
    "baseline_estimated_cost_usd_numerator",
    "baseline_estimated_cost_usd_denominator",
    "candidate_evaluation_count",
    "candidate_pass_count",
    "candidate_fail_count",
    "candidate_score_numerator",
    "candidate_score_denominator",
    "candidate_latency_ms_numerator",
    "candidate_latency_ms_denominator",
    "candidate_estimated_cost_usd_numerator",
    "candidate_estimated_cost_usd_denominator",
)


def _non_empty(column_name: str):
    return F.col(column_name).isNotNull() & (F.length(F.trim(F.col(column_name))) > 0)


def _safe_ratio(numerator, denominator, scale: int = 6):
    return F.when(denominator > 0, F.round(numerator / denominator, scale)).otherwise(
        F.lit(None).cast("double")
    )


def _nullable_threshold_flag(metric, threshold, comparison: str):
    if comparison == "less_than":
        predicate = metric < threshold
    elif comparison == "greater_than":
        predicate = metric > threshold
    else:
        raise ValueError(f"Unsupported comparison: {comparison}")
    return F.when(metric.isNull(), F.lit(None).cast("boolean")).otherwise(predicate)


def _prepare_assignments(assignments: DataFrame) -> DataFrame:
    cleaned = (
        assignments.select(
            F.trim(F.col("request_id")).alias("request_id"),
            F.trim(F.col("dataset_name")).alias("dataset_name"),
            F.trim(F.col("experiment_name")).alias("experiment_name"),
            F.trim(F.col("variant_name")).alias("variant_name"),
        )
        .filter(
            _non_empty("request_id")
            & _non_empty("dataset_name")
            & _non_empty("experiment_name")
            & _non_empty("variant_name")
        )
        .dropDuplicates()
    )

    return (
        cleaned.groupBy("request_id", "dataset_name", "experiment_name")
        .agg(
            F.countDistinct("variant_name").alias("_variant_count"),
            F.first("variant_name").alias("variant_name"),
        )
        .filter(F.col("_variant_count") == 1)
        .drop("_variant_count")
    )


def _prepare_comparison_config(comparison_config: DataFrame) -> DataFrame:
    return (
        comparison_config.select(
            F.trim(F.col("dataset_name")).alias("dataset_name"),
            F.trim(F.col("experiment_name")).alias("experiment_name"),
            F.trim(F.col("baseline_variant")).alias("baseline_variant"),
            F.trim(F.col("candidate_variant")).alias("candidate_variant"),
        )
        .filter(
            _non_empty("dataset_name")
            & _non_empty("experiment_name")
            & _non_empty("baseline_variant")
            & _non_empty("candidate_variant")
            & (F.col("baseline_variant") != F.col("candidate_variant"))
        )
        .dropDuplicates()
    )


def _build_variant_metrics(
    evaluation_judgments: DataFrame,
    llm_requests: DataFrame,
    assignments: DataFrame,
) -> DataFrame:
    evaluations = evaluation_judgments.alias("evaluation")
    requests = llm_requests.select(
        "request_id", "model_name", "prompt_version", "latency_ms", "estimated_cost_usd"
    ).alias("request")
    metadata = _prepare_assignments(assignments).alias("metadata")

    joined = (
        evaluations.join(
            metadata,
            F.col("evaluation.request_id") == F.col("metadata.request_id"),
            "inner",
        )
        .join(
            requests,
            F.col("evaluation.request_id") == F.col("request.request_id"),
            "left",
        )
        .select(
            F.col("metadata.dataset_name").alias("dataset_name"),
            F.col("metadata.experiment_name").alias("experiment_name"),
            F.col("metadata.variant_name").alias("variant_name"),
            F.col("evaluation.evaluation_dimension").alias("evaluation_dimension"),
            F.coalesce(
                F.when(
                    F.length(F.trim(F.col("evaluation.evaluated_model_name"))) > 0,
                    F.trim(F.col("evaluation.evaluated_model_name")),
                ),
                F.when(
                    F.length(F.trim(F.col("request.model_name"))) > 0,
                    F.trim(F.col("request.model_name")),
                ),
                F.lit("unknown"),
            ).alias("model_name"),
            F.coalesce(
                F.when(
                    F.length(F.trim(F.col("evaluation.evaluated_prompt_version"))) > 0,
                    F.trim(F.col("evaluation.evaluated_prompt_version")),
                ),
                F.when(
                    F.length(F.trim(F.col("request.prompt_version"))) > 0,
                    F.trim(F.col("request.prompt_version")),
                ),
                F.lit("unknown"),
            ).alias("prompt_version"),
            F.to_date(F.col("evaluation.date")).alias("evaluation_date"),
            F.col("evaluation.score").cast("double").alias("score"),
            F.col("evaluation.passed").cast("boolean").alias("passed"),
            F.col("request.latency_ms").cast("long").alias("latency_ms"),
            F.col("request.estimated_cost_usd").cast("double").alias("estimated_cost_usd"),
        )
    )

    return joined.groupBy(
        "dataset_name",
        "experiment_name",
        "variant_name",
        "model_name",
        "prompt_version",
        "evaluation_dimension",
    ).agg(
        F.min("evaluation_date").alias("experiment_start_date"),
        F.max("evaluation_date").alias("experiment_end_date"),
        F.count(F.lit(1)).alias("evaluation_count"),
        F.sum(F.when(F.col("passed") == F.lit(True), 1).otherwise(0)).alias("pass_count"),
        F.sum(F.when(F.col("passed") == F.lit(False), 1).otherwise(0)).alias("fail_count"),
        F.sum(F.coalesce(F.col("score"), F.lit(0.0))).alias("score_numerator"),
        F.count("score").alias("score_denominator"),
        F.sum(F.coalesce(F.col("latency_ms"), F.lit(0))).alias("latency_ms_numerator"),
        F.count("latency_ms").alias("latency_ms_denominator"),
        F.sum(F.coalesce(F.col("estimated_cost_usd"), F.lit(0.0))).alias(
            "estimated_cost_usd_numerator"
        ),
        F.count("estimated_cost_usd").alias("estimated_cost_usd_denominator"),
    )


def build_evaluation_dataset_experiment_regression_metrics(
    evaluation_judgments: DataFrame,
    llm_requests: DataFrame,
    assignments: DataFrame,
    comparison_config: DataFrame,
) -> DataFrame:
    """Build additive baseline/candidate components from existing DWD-compatible facts."""
    variants = _build_variant_metrics(evaluation_judgments, llm_requests, assignments)
    config = _prepare_comparison_config(comparison_config).alias("config")
    baseline = variants.alias("baseline")
    candidate = variants.alias("candidate")

    baseline_join = (
        (F.col("config.dataset_name") == F.col("baseline.dataset_name"))
        & (F.col("config.experiment_name") == F.col("baseline.experiment_name"))
        & (F.col("config.baseline_variant") == F.col("baseline.variant_name"))
    )
    candidate_join = (
        (F.col("config.dataset_name") == F.col("candidate.dataset_name"))
        & (F.col("config.experiment_name") == F.col("candidate.experiment_name"))
        & (F.col("config.candidate_variant") == F.col("candidate.variant_name"))
        & (
            F.col("baseline.evaluation_dimension")
            == F.col("candidate.evaluation_dimension")
        )
    )

    return (
        config.join(baseline, baseline_join, "inner")
        .join(candidate, candidate_join, "inner")
        .select(
            F.col("config.dataset_name").alias("dataset_name"),
            F.col("config.experiment_name").alias("experiment_name"),
            F.col("config.baseline_variant").alias("baseline_variant"),
            F.col("config.candidate_variant").alias("candidate_variant"),
            F.col("baseline.model_name").alias("baseline_model_name"),
            F.col("baseline.prompt_version").alias("baseline_prompt_version"),
            F.col("candidate.model_name").alias("candidate_model_name"),
            F.col("candidate.prompt_version").alias("candidate_prompt_version"),
            F.col("baseline.evaluation_dimension").alias("evaluation_dimension"),
            F.least(
                F.col("baseline.experiment_start_date"),
                F.col("candidate.experiment_start_date"),
            ).alias("experiment_start_date"),
            F.greatest(
                F.col("baseline.experiment_end_date"),
                F.col("candidate.experiment_end_date"),
            ).alias("experiment_end_date"),
            *[
                F.col(f"baseline.{column}").alias(f"baseline_{column}")
                for column in (
                    "evaluation_count",
                    "pass_count",
                    "fail_count",
                    "score_numerator",
                    "score_denominator",
                    "latency_ms_numerator",
                    "latency_ms_denominator",
                    "estimated_cost_usd_numerator",
                    "estimated_cost_usd_denominator",
                )
            ],
            *[
                F.col(f"candidate.{column}").alias(f"candidate_{column}")
                for column in (
                    "evaluation_count",
                    "pass_count",
                    "fail_count",
                    "score_numerator",
                    "score_denominator",
                    "latency_ms_numerator",
                    "latency_ms_denominator",
                    "estimated_cost_usd_numerator",
                    "estimated_cost_usd_denominator",
                )
            ],
        )
    )


def build_evaluation_dataset_experiment_regression_comparison(
    metrics: DataFrame,
    quality_drop_threshold: float = 0.0,
    cost_increase_rate_threshold: float = 0.0,
    latency_increase_rate_threshold: float = 0.0,
) -> DataFrame:
    """Derive rates and regression flags from summed ADS numerator/denominator components."""
    grouped = metrics.groupBy(*IDENTIFIER_COLUMNS).agg(
        F.min("experiment_start_date").alias("experiment_start_date"),
        F.max("experiment_end_date").alias("experiment_end_date"),
        *[F.sum(column).alias(column) for column in COMPONENT_COLUMNS],
    )

    derived = (
        grouped.withColumn(
            "baseline_pass_rate",
            _safe_ratio(F.col("baseline_pass_count"), F.col("baseline_evaluation_count")),
        )
        .withColumn(
            "candidate_pass_rate",
            _safe_ratio(F.col("candidate_pass_count"), F.col("candidate_evaluation_count")),
        )
        .withColumn(
            "baseline_avg_score",
            _safe_ratio(
                F.col("baseline_score_numerator"), F.col("baseline_score_denominator")
            ),
        )
        .withColumn(
            "candidate_avg_score",
            _safe_ratio(
                F.col("candidate_score_numerator"), F.col("candidate_score_denominator")
            ),
        )
        .withColumn(
            "baseline_avg_latency_ms",
            _safe_ratio(
                F.col("baseline_latency_ms_numerator"),
                F.col("baseline_latency_ms_denominator"),
                2,
            ),
        )
        .withColumn(
            "candidate_avg_latency_ms",
            _safe_ratio(
                F.col("candidate_latency_ms_numerator"),
                F.col("candidate_latency_ms_denominator"),
                2,
            ),
        )
        .withColumn(
            "baseline_avg_estimated_cost_usd",
            _safe_ratio(
                F.col("baseline_estimated_cost_usd_numerator"),
                F.col("baseline_estimated_cost_usd_denominator"),
                8,
            ),
        )
        .withColumn(
            "candidate_avg_estimated_cost_usd",
            _safe_ratio(
                F.col("candidate_estimated_cost_usd_numerator"),
                F.col("candidate_estimated_cost_usd_denominator"),
                8,
            ),
        )
    )

    derived = (
        derived.withColumn(
            "score_delta", F.col("candidate_avg_score") - F.col("baseline_avg_score")
        )
        .withColumn(
            "pass_rate_delta",
            F.col("candidate_pass_rate") - F.col("baseline_pass_rate"),
        )
        .withColumn(
            "cost_increase_rate",
            _safe_ratio(
                F.col("candidate_avg_estimated_cost_usd")
                - F.col("baseline_avg_estimated_cost_usd"),
                F.col("baseline_avg_estimated_cost_usd"),
            ),
        )
        .withColumn(
            "latency_increase_rate",
            _safe_ratio(
                F.col("candidate_avg_latency_ms") - F.col("baseline_avg_latency_ms"),
                F.col("baseline_avg_latency_ms"),
            ),
        )
    )

    quality_regression = (
        (F.col("score_delta") < F.lit(-quality_drop_threshold))
        | (F.col("pass_rate_delta") < F.lit(-quality_drop_threshold))
    )
    quality_inputs_missing = F.col("score_delta").isNull() | F.col("pass_rate_delta").isNull()

    return (
        derived.withColumn(
            "is_quality_regression",
            F.when(quality_inputs_missing, F.lit(None).cast("boolean")).otherwise(
                quality_regression
            ),
        )
        .withColumn(
            "is_cost_increase",
            _nullable_threshold_flag(
                F.col("cost_increase_rate"),
                F.lit(cost_increase_rate_threshold),
                "greater_than",
            ),
        )
        .withColumn(
            "is_latency_increase",
            _nullable_threshold_flag(
                F.col("latency_increase_rate"),
                F.lit(latency_increase_rate_threshold),
                "greater_than",
            ),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evaluation-table", default=DEFAULT_EVALUATION_TABLE)
    parser.add_argument("--llm-request-table", default=DEFAULT_LLM_REQUEST_TABLE)
    parser.add_argument("--assignment-input", type=Path, default=DEFAULT_ASSIGNMENT_INPUT)
    parser.add_argument(
        "--comparison-config-input", type=Path, default=DEFAULT_COMPARISON_CONFIG_INPUT
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    from datetime import datetime, timezone

    started_at = datetime.now(timezone.utc)
    spark = build_paimon_spark_session("ai-observability-ads-evaluation-experiment-regression")
    try:
        evaluations = spark.table(args.evaluation_table)
        requests = spark.table(args.llm_request_table)
        assignments = spark.read.parquet(str(args.assignment_input))
        comparison_config = spark.read.parquet(str(args.comparison_config_input))
        result = build_evaluation_dataset_experiment_regression_metrics(
            evaluations, requests, assignments, comparison_config
        )
        result.write.mode("overwrite").parquet(str(args.output))
        row_count = result.count()
        log_info(
            LOGGER,
            "ads_evaluation_dataset_experiment_regression_written",
            output=str(args.output),
            rows=row_count,
        )
        append_pipeline_run(
            pipeline_name="spark_build_ads_evaluation_dataset_experiment_regression",
            layer="ads",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=evaluations.count(),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
