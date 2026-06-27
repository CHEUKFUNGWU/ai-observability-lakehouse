import pytest

from scripts.load_dws_metrics_to_doris import columns_for_table, normalize_row, qualified_table_name


def test_qualified_table_name_accepts_safe_identifiers():
    assert qualified_table_name("ai_observability", "dws_ai_llm_feature_request_1d") == (
        "`ai_observability`.`dws_ai_llm_feature_request_1d`"
    )


@pytest.mark.parametrize(
    ("database", "table"),
    [
        ("ai_observability;DROP TABLE x", "dws_ai_llm_feature_request_1d"),
        ("ai_observability", "dws_ai_llm_feature_request_1d;DROP TABLE x"),
        ("ai-observability", "dws_ai_llm_feature_request_1d"),
        ("ai_observability", "dws llm feature daily metrics"),
        ("ai_observability", "`dws_ai_llm_feature_request_1d`"),
    ],
)
def test_qualified_table_name_rejects_unsafe_identifiers(database, table):
    with pytest.raises(ValueError):
        qualified_table_name(database, table)


def test_columns_for_table_supports_executive_weekly_summary():
    columns = columns_for_table("ads_observability_executive_weekly_summary")

    assert columns[0] == "week_start_date"
    assert columns[-1] == "total_ai_cost_amt_1w"


def test_columns_for_table_supports_trace_health_detail_without_raw_text():
    columns = columns_for_table("ads_observability_trace_health_detail")

    assert columns[0] == "date"
    assert "trace_id" in columns
    assert "bottleneck_node_type" in columns
    assert "child_observation_summary" == columns[-1]
    assert "prompt_text" not in columns
    assert "response_text" not in columns
    assert "arguments_json" not in columns
    assert "result_text" not in columns


def test_columns_for_table_supports_evaluation_dataset_experiment_regression():
    columns = columns_for_table(
        "ads_observability_evaluation_dataset_experiment_regression"
    )

    assert columns[:4] == [
        "dataset_name",
        "experiment_name",
        "baseline_variant",
        "candidate_variant",
    ]
    assert "baseline_score_numerator" in columns
    assert "candidate_estimated_cost_usd_denominator" == columns[-1]
    assert "baseline_pass_rate" not in columns
    assert "is_quality_regression" not in columns


def test_columns_for_table_supports_platform_health():
    columns = columns_for_table("dws_ai_platform_component_health_1d")

    assert columns == [
        "date",
        "component",
        "metric_name",
        "metric_value",
        "threshold",
        "is_breach",
    ]


def test_normalize_row_converts_week_start_date():
    row = normalize_row({"week_start_date": "2026-01-05", "app_name": "support"})

    assert row["week_start_date"].isoformat() == "2026-01-05"


def test_normalize_row_converts_release_date():
    row = normalize_row({"release_date": "2026-02-01", "model_name": "deepseek-chat"})

    assert row["release_date"].isoformat() == "2026-02-01"
