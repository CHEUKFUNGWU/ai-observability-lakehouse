import pytest

from scripts.load_dws_metrics_to_doris import qualified_table_name


def test_qualified_table_name_accepts_safe_identifiers():
    assert qualified_table_name("ai_observability", "dws_llm_feature_daily_metrics") == (
        "`ai_observability`.`dws_llm_feature_daily_metrics`"
    )


@pytest.mark.parametrize(
    ("database", "table"),
    [
        ("ai_observability;DROP TABLE x", "dws_llm_feature_daily_metrics"),
        ("ai_observability", "dws_llm_feature_daily_metrics;DROP TABLE x"),
        ("ai-observability", "dws_llm_feature_daily_metrics"),
        ("ai_observability", "dws llm feature daily metrics"),
        ("ai_observability", "`dws_llm_feature_daily_metrics`"),
    ],
)
def test_qualified_table_name_rejects_unsafe_identifiers(database, table):
    with pytest.raises(ValueError):
        qualified_table_name(database, table)
