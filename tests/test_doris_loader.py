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


def test_normalize_row_converts_week_start_date():
    row = normalize_row({"week_start_date": "2026-01-05", "app_name": "support"})

    assert row["week_start_date"].isoformat() == "2026-01-05"
