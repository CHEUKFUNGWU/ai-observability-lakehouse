from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_doris_schema_defines_llm_agent_and_tool_tables():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_llm_request_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_run_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_span_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_tool_call_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_retrieval_request_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_feedback_action_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_guardrail_check_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_evaluation_judgment_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_model_deployment_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_feedback_feature_action_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_guardrail_rule_check_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_cost_team_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_prompt_version_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_model_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_knowledge_base_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_guardrail_rule_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_team_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_user_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_prompt_version_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dim_model_version_df" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_feature_anomaly" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_sla_feature_report" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_prompt_prompt_version_metrics" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_retrieval_daily_quality" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_feedback_daily_satisfaction" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_guardrail_daily_violation" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_daily_budget" in sql
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS ai_observability.mv_daily_summary" in sql


def test_doris_schema_uses_date_column_consistently():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "event_date" not in sql
    assert "PARTITION BY RANGE(`date`)" in sql


def test_primary_ads_fact_tables_do_not_store_derived_rates():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    llm_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1d", 1)[1]
    llm_section = llm_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dim_model_df", 1)[0]
    agent_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d", 1)[1]
    agent_section = agent_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d", 1)[0]

    assert "success_rate" not in llm_section
    assert "error_rate" not in llm_section
    assert "span_failure_rate" not in agent_section
    assert "max_latency_ms BIGINT NOT NULL" in llm_section


def test_doris_paimon_catalog_sql_assets_exist():
    catalog_sql = (REPO_ROOT / "sql" / "doris_create_paimon_catalog.sql").read_text(encoding="utf-8")
    sync_sql = (REPO_ROOT / "sql" / "doris_sync_paimon_dws.sql").read_text(encoding="utf-8")

    assert "'type' = 'paimon'" in catalog_sql
    assert "warehouse' = 'file:///workspace/data/paimon'" in catalog_sql
    assert "INSERT INTO ai_observability.dws_ai_llm_feature_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_feature_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_retrieval_knowledge_base_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_feedback_feature_action_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_feedback_feature_action_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_guardrail_rule_check_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_guardrail_rule_check_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_cost_team_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_cost_team_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_evaluation_feature_judgment_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_prompt_version_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_prompt_version_request_1d" in sync_sql
