from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_doris_schema_defines_llm_agent_and_tool_tables():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_llm_feature_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_tool_daily_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_model" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_cost_anomaly_daily" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_sla_daily_report" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_prompt_version_daily_metrics" in sql
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS ai_observability.mv_daily_summary" in sql


def test_doris_schema_uses_date_column_consistently():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "event_date" not in sql
    assert "PARTITION BY RANGE(`date`)" in sql


def test_primary_ads_fact_tables_do_not_store_derived_rates():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    llm_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.ads_llm_feature_daily_metrics", 1)[1]
    llm_section = llm_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dim_model", 1)[0]
    agent_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_daily_metrics", 1)[1]
    agent_section = agent_section.split("CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_tool_daily_metrics", 1)[0]

    assert "success_rate" not in llm_section
    assert "error_rate" not in llm_section
    assert "span_failure_rate" not in agent_section
