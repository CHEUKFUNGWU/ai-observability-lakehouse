import pytest

from scripts.load_dws_metrics_to_doris import qualified_table_name


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
