from datetime import date

from scripts.load_dws_metrics_to_doris import columns_for_table
from scripts.spark_build_ads_evaluation_dataset_experiment_regression import (
    build_evaluation_dataset_experiment_regression_comparison,
    build_evaluation_dataset_experiment_regression_metrics,
)


def _evaluation_rows():
    rows = []
    scores = {
        "baseline_1": {"faithfulness": (0.9, True), "coherence": (0.7, True)},
        "baseline_2": {"faithfulness": (0.8, True), "coherence": (0.6, False)},
        "candidate_1": {"faithfulness": (0.7, True), "coherence": (0.8, True)},
        "candidate_2": {"faithfulness": (0.5, False), "coherence": (0.7, True)},
    }
    for request_id, dimensions in scores.items():
        is_candidate = request_id.startswith("candidate")
        for dimension, (score, passed) in dimensions.items():
            rows.append(
                {
                    "request_id": request_id,
                    "evaluation_dimension": dimension,
                    "evaluated_model_name": "model-b" if is_candidate else "model-a",
                    "evaluated_prompt_version": "v2" if is_candidate else "v1",
                    "score": score,
                    "passed": passed,
                    "date": date(2026, 6, 26),
                }
            )
    return rows


def _llm_rows():
    return [
        {
            "request_id": "baseline_1",
            "model_name": "model-a",
            "prompt_version": "v1",
            "latency_ms": 100,
            "estimated_cost_usd": 0.10,
        },
        {
            "request_id": "baseline_2",
            "model_name": "model-a",
            "prompt_version": "v1",
            "latency_ms": 200,
            "estimated_cost_usd": 0.20,
        },
        {
            "request_id": "candidate_1",
            "model_name": "model-b",
            "prompt_version": "v2",
            "latency_ms": 300,
            "estimated_cost_usd": 0.30,
        },
        {
            "request_id": "candidate_2",
            "model_name": "model-b",
            "prompt_version": "v2",
            "latency_ms": 400,
            "estimated_cost_usd": 0.40,
        },
    ]


def _assignment_rows():
    return [
        {
            "request_id": request_id,
            "dataset_name": "support-golden",
            "experiment_name": "prompt-v2-rollout",
            "variant_name": "candidate" if request_id.startswith("candidate") else "baseline",
        }
        for request_id in ("baseline_1", "baseline_2", "candidate_1", "candidate_2")
    ]


def _config_rows():
    return [
        {
            "dataset_name": "support-golden",
            "experiment_name": "prompt-v2-rollout",
            "baseline_variant": "baseline",
            "candidate_variant": "candidate",
        }
    ]


def _build_metrics(spark, assignments=None):
    return build_evaluation_dataset_experiment_regression_metrics(
        spark.createDataFrame(_evaluation_rows()),
        spark.createDataFrame(_llm_rows()),
        spark.createDataFrame(assignments or _assignment_rows()),
        spark.createDataFrame(_config_rows()),
    )


def test_baseline_candidate_comparison_derives_regression_indicators(spark):
    comparison = build_evaluation_dataset_experiment_regression_comparison(
        _build_metrics(spark)
    )
    row = comparison.filter("evaluation_dimension = 'faithfulness'").collect()[0].asDict()

    assert row["dataset_name"] == "support-golden"
    assert row["experiment_name"] == "prompt-v2-rollout"
    assert row["baseline_variant"] == "baseline"
    assert row["candidate_variant"] == "candidate"
    assert row["baseline_model_name"] == "model-a"
    assert row["candidate_model_name"] == "model-b"
    assert row["baseline_prompt_version"] == "v1"
    assert row["candidate_prompt_version"] == "v2"
    assert row["baseline_pass_count"] == 2
    assert row["candidate_pass_count"] == 1
    assert row["baseline_pass_rate"] == 1.0
    assert row["candidate_pass_rate"] == 0.5
    assert row["baseline_avg_score"] == 0.85
    assert row["candidate_avg_score"] == 0.6
    assert row["baseline_avg_latency_ms"] == 150.0
    assert row["candidate_avg_latency_ms"] == 350.0
    assert row["baseline_avg_estimated_cost_usd"] == 0.15
    assert row["candidate_avg_estimated_cost_usd"] == 0.35
    assert row["is_quality_regression"] is True
    assert row["is_cost_increase"] is True
    assert row["is_latency_increase"] is True


def test_stored_metrics_match_doris_loader_and_exclude_derived_rates(spark):
    metrics = _build_metrics(spark)

    assert metrics.columns == columns_for_table(
        "ads_observability_evaluation_dataset_experiment_regression"
    )
    assert "baseline_pass_rate" not in metrics.columns
    assert "is_quality_regression" not in metrics.columns


def test_comparison_keeps_multiple_evaluation_dimensions(spark):
    rows = build_evaluation_dataset_experiment_regression_comparison(
        _build_metrics(spark)
    ).collect()

    assert {row["evaluation_dimension"] for row in rows} == {"faithfulness", "coherence"}
    coherence = next(row for row in rows if row["evaluation_dimension"] == "coherence")
    assert coherence["is_quality_regression"] is False


def test_missing_or_conflicting_assignment_metadata_is_excluded(spark):
    assignments = _assignment_rows()
    assignments[0]["dataset_name"] = ""
    assignments.extend(
        [
            {
                "request_id": "baseline_2",
                "dataset_name": "support-golden",
                "experiment_name": "prompt-v2-rollout",
                "variant_name": "candidate",
            }
        ]
    )

    rows = _build_metrics(spark, assignments).collect()

    assert rows == []


def test_zero_denominators_produce_null_rates_and_indicators(spark):
    row = _component_row()
    for key in list(row):
        if key.endswith("_count") or key.endswith("_numerator") or key.endswith("_denominator"):
            row[key] = 0

    result = build_evaluation_dataset_experiment_regression_comparison(
        spark.createDataFrame([row])
    ).collect()[0]

    assert result["baseline_pass_rate"] is None
    assert result["candidate_pass_rate"] is None
    assert result["baseline_avg_score"] is None
    assert result["candidate_avg_estimated_cost_usd"] is None
    assert result["cost_increase_rate"] is None
    assert result["latency_increase_rate"] is None
    assert result["is_quality_regression"] is None
    assert result["is_cost_increase"] is None
    assert result["is_latency_increase"] is None


def test_rates_are_derived_from_summed_components_not_averaged(spark):
    first = _component_row()
    second = _component_row()
    first.update(
        baseline_evaluation_count=1,
        baseline_pass_count=1,
        baseline_fail_count=0,
        baseline_score_numerator=1.0,
        baseline_score_denominator=1,
        candidate_evaluation_count=1,
        candidate_pass_count=1,
        candidate_fail_count=0,
        candidate_score_numerator=1.0,
        candidate_score_denominator=1,
    )
    second.update(
        baseline_evaluation_count=9,
        baseline_pass_count=0,
        baseline_fail_count=9,
        baseline_score_numerator=0.0,
        baseline_score_denominator=9,
        candidate_evaluation_count=9,
        candidate_pass_count=0,
        candidate_fail_count=9,
        candidate_score_numerator=0.0,
        candidate_score_denominator=9,
    )

    result = build_evaluation_dataset_experiment_regression_comparison(
        spark.createDataFrame([first, second])
    ).collect()[0]

    assert result["baseline_pass_rate"] == 0.1
    assert result["candidate_pass_rate"] == 0.1
    assert result["baseline_avg_score"] == 0.1
    assert result["candidate_avg_score"] == 0.1


def _component_row():
    return {
        "dataset_name": "support-golden",
        "experiment_name": "prompt-v2-rollout",
        "baseline_variant": "baseline",
        "candidate_variant": "candidate",
        "baseline_model_name": "model-a",
        "baseline_prompt_version": "v1",
        "candidate_model_name": "model-b",
        "candidate_prompt_version": "v2",
        "evaluation_dimension": "faithfulness",
        "experiment_start_date": date(2026, 6, 26),
        "experiment_end_date": date(2026, 6, 26),
        "baseline_evaluation_count": 1,
        "baseline_pass_count": 1,
        "baseline_fail_count": 0,
        "baseline_score_numerator": 1.0,
        "baseline_score_denominator": 1,
        "baseline_latency_ms_numerator": 100,
        "baseline_latency_ms_denominator": 1,
        "baseline_estimated_cost_usd_numerator": 0.1,
        "baseline_estimated_cost_usd_denominator": 1,
        "candidate_evaluation_count": 1,
        "candidate_pass_count": 1,
        "candidate_fail_count": 0,
        "candidate_score_numerator": 1.0,
        "candidate_score_denominator": 1,
        "candidate_latency_ms_numerator": 100,
        "candidate_latency_ms_denominator": 1,
        "candidate_estimated_cost_usd_numerator": 0.1,
        "candidate_estimated_cost_usd_denominator": 1,
    }
