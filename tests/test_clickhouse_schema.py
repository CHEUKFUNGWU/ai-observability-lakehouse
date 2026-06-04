from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_clickhouse_schema_defines_llm_agent_and_tool_tables():
    sql = (REPO_ROOT / "sql" / "create_clickhouse_tables.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_llm_feature_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_tool_daily_metrics" in sql


def test_clickhouse_schema_uses_date_column_consistently():
    sql = (REPO_ROOT / "sql" / "create_clickhouse_tables.sql").read_text(encoding="utf-8")

    assert "event_date" not in sql
    assert "PARTITION BY toYYYYMM(date)" in sql


def test_clickhouse_ads_tables_do_not_store_success_or_error_rates():
    sql = (REPO_ROOT / "sql" / "create_clickhouse_tables.sql").read_text(encoding="utf-8")

    assert "success_rate" not in sql
    assert "error_rate" not in sql
