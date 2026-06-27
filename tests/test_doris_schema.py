from pathlib import Path

from app.warehouse_contract import (
    render_agent_team_run_1d_doris_columns,
    render_agent_run_1d_doris_columns,
    render_agent_run_doris_columns,
    render_agent_span_doris_columns,
    render_agent_tool_call_1d_doris_columns,
    render_agent_tool_call_doris_columns,
    render_agent_orchestration_doris_columns,
    render_agent_orchestration_handoff_1d_doris_columns,
    render_compliance_access_audit_doris_columns,
    render_compliance_data_retention_doris_columns,
    render_cost_team_request_1d_doris_columns,
    render_evaluation_feature_judgment_1d_doris_columns,
    render_evaluation_dataset_experiment_regression_doris_columns,
    render_evaluation_judgment_doris_columns,
    render_feedback_action_doris_columns,
    render_feedback_feature_action_1d_doris_columns,
    render_guardrail_check_doris_columns,
    render_guardrail_rule_check_1d_doris_columns,
    render_llm_feature_env_request_1d_doris_columns,
    render_llm_feature_request_1h_doris_columns,
    render_llm_request_doris_columns,
    render_llm_region_request_1d_doris_columns,
    render_llm_session_request_1d_doris_columns,
    render_model_deployment_doris_columns,
    render_platform_component_health_1d_doris_columns,
    render_prompt_version_request_1d_doris_columns,
    render_retrieval_knowledge_base_request_1d_doris_columns,
    render_retrieval_request_doris_columns,
    render_trace_health_detail_doris_columns,
)

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
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_data_retention_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_orchestration_di" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1h" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_session_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_feedback_feature_action_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_guardrail_rule_check_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_cost_team_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_prompt_version_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_env_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_region_request_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_team_run_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_platform_component_health_1d" in sql
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
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_monthly_chargeback" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_executive_weekly_summary" in sql
    assert "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_trace_health_detail" in sql
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "ai_observability.ads_observability_evaluation_dataset_experiment_regression"
    ) in sql
    assert (
        "DROP TABLE IF EXISTS "
        "ai_observability.ads_observability_evaluation_dataset_experiment_regression;"
    ) in sql
    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS ai_observability.mv_daily_summary" in sql


def test_doris_schema_uses_date_column_consistently():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")

    assert "event_date" not in sql
    assert "PARTITION BY RANGE(`date`)" in sql


def test_doris_llm_request_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    llm_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_llm_request_di", 1)[1]
    llm_section = llm_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_run_di", 1)[0]

    assert render_llm_request_doris_columns() in llm_section


def test_doris_agent_run_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    run_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_run_di", 1)[1]
    run_section = run_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_span_di", 1)[0]

    assert render_agent_run_doris_columns() in run_section


def test_doris_agent_span_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    span_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_span_di", 1)[1]
    span_section = span_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_tool_call_di", 1)[0]

    assert render_agent_span_doris_columns() in span_section


def test_doris_agent_tool_call_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    tool_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_tool_call_di", 1)[1]
    tool_section = tool_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_retrieval_request_di", 1)[0]

    assert render_agent_tool_call_doris_columns() in tool_section


def test_doris_retrieval_request_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    retrieval_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_retrieval_request_di", 1)[1]
    retrieval_section = retrieval_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1d", 1)[0]

    assert render_retrieval_request_doris_columns() in retrieval_section


def test_doris_retrieval_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    retrieval_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d", 1)[1]
    retrieval_section = retrieval_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_feedback_feature_action_1d", 1)[0]

    assert render_retrieval_knowledge_base_request_1d_doris_columns() in retrieval_section


def test_doris_feedback_action_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    feedback_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_feedback_action_di", 1)[1]
    feedback_section = feedback_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_guardrail_check_di", 1)[0]

    assert render_feedback_action_doris_columns() in feedback_section


def test_doris_guardrail_check_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    guardrail_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_guardrail_check_di", 1)[1]
    guardrail_section = guardrail_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_evaluation_judgment_di", 1)[0]

    assert render_guardrail_check_doris_columns() in guardrail_section


def test_doris_evaluation_judgment_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    evaluation_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_evaluation_judgment_di", 1)[1]
    evaluation_section = evaluation_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_model_deployment_di", 1)[0]

    assert render_evaluation_judgment_doris_columns() in evaluation_section


def test_doris_model_deployment_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    deployment_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_model_deployment_di", 1)[1]
    deployment_section = deployment_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di", 1)[0]

    assert render_model_deployment_doris_columns() in deployment_section


def test_doris_compliance_access_audit_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    access_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di", 1)[1]
    access_section = access_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_data_retention_di", 1)[0]

    assert render_compliance_access_audit_doris_columns() in access_section


def test_doris_compliance_data_retention_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    retention_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_data_retention_di", 1)[1]
    retention_section = retention_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_orchestration_di", 1)[0]

    assert render_compliance_data_retention_doris_columns() in retention_section


def test_doris_agent_orchestration_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    orchestration_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_orchestration_di", 1)[1]
    orchestration_section = orchestration_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d", 1)[0]

    assert render_agent_orchestration_doris_columns() in orchestration_section


def test_doris_feedback_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    feedback_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_feedback_feature_action_1d", 1)[1]
    feedback_section = feedback_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_guardrail_rule_check_1d", 1)[0]

    assert render_feedback_feature_action_1d_doris_columns() in feedback_section


def test_doris_guardrail_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    guardrail_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_guardrail_rule_check_1d", 1)[1]
    guardrail_section = guardrail_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_cost_team_request_1d", 1)[0]

    assert render_guardrail_rule_check_1d_doris_columns() in guardrail_section


def test_doris_evaluation_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    evaluation_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d", 1)[1]
    evaluation_section = evaluation_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_prompt_version_request_1d", 1)[0]

    assert render_evaluation_feature_judgment_1d_doris_columns() in evaluation_section


def test_doris_cost_team_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    cost_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_cost_team_request_1d", 1)[1]
    cost_section = cost_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d", 1)[0]

    assert render_cost_team_request_1d_doris_columns() in cost_section


def test_doris_prompt_version_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    prompt_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_prompt_version_request_1d", 1)[1]
    prompt_section = prompt_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_env_request_1d", 1)[0]

    assert render_prompt_version_request_1d_doris_columns() in prompt_section


def test_doris_prompt_version_ads_contains_comparison_numerators():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    ads_section = sql.split(
        "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_prompt_prompt_version_metrics",
        1,
    )[1]
    ads_section = ads_section.split("CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_retrieval_daily_quality", 1)[0]

    for column in (
        "success_count BIGINT NOT NULL",
        "error_count BIGINT NOT NULL",
        "total_tokens BIGINT NOT NULL",
        "evaluation_count BIGINT NOT NULL",
        "pass_count BIGINT NOT NULL",
        "fail_count BIGINT NOT NULL",
        "evaluation_score_numerator DOUBLE NOT NULL",
        "evaluation_score_denominator BIGINT NOT NULL",
        "metadata_conflict_count BIGINT NOT NULL",
    ):
        assert column in ads_section
    assert "success_rate" not in ads_section
    assert "pass_rate" not in ads_section


def test_doris_trace_health_detail_columns_match_contract_and_privacy_rules():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    ads_section = sql.split(
        "CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_trace_health_detail",
        1,
    )[1]
    ads_section = ads_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di", 1)[0]

    assert render_trace_health_detail_doris_columns() in ads_section
    assert "prompt_text" not in ads_section
    assert "response_text" not in ads_section
    assert "arguments_json" not in ads_section
    assert "result_text" not in ads_section
    assert "DUPLICATE KEY(`date`, trace_id)" in ads_section


def test_doris_evaluation_dataset_experiment_regression_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    ads_section = sql.split(
        "CREATE TABLE IF NOT EXISTS "
        "ai_observability.ads_observability_evaluation_dataset_experiment_regression",
        1,
    )[1]
    ads_section = ads_section.split(
        "CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di",
        1,
    )[0]

    assert render_evaluation_dataset_experiment_regression_doris_columns() in ads_section
    assert (
        "DUPLICATE KEY(\n"
        "    dataset_name,\n"
        "    experiment_name,\n"
        "    baseline_variant,\n"
        "    candidate_variant,\n"
        "    baseline_model_name,\n"
        "    baseline_prompt_version,\n"
        "    candidate_model_name,\n"
        "    candidate_prompt_version,\n"
        "    evaluation_dimension\n"
        ")"
    ) in ads_section
    for derived_column in (
        "baseline_pass_rate",
        "candidate_pass_rate",
        "score_delta",
        "cost_increase_rate",
        "latency_increase_rate",
        "is_quality_regression",
        "is_cost_increase",
        "is_latency_increase",
    ):
        assert derived_column not in ads_section


def test_doris_llm_feature_env_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    feature_env_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_env_request_1d", 1)[1]
    feature_env_section = feature_env_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_region_request_1d", 1)[0]

    assert render_llm_feature_env_request_1d_doris_columns() in feature_env_section


def test_doris_llm_region_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    region_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_region_request_1d", 1)[1]
    region_section = region_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_team_run_1d", 1)[0]

    assert render_llm_region_request_1d_doris_columns() in region_section


def test_doris_agent_team_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    agent_team_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_team_run_1d", 1)[1]
    agent_team_section = agent_team_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d", 1)[0]

    assert render_agent_team_run_1d_doris_columns() in agent_team_section


def test_doris_llm_feature_hourly_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    hourly_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1h", 1)[1]
    hourly_section = hourly_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_session_request_1d", 1)[0]

    assert render_llm_feature_request_1h_doris_columns() in hourly_section


def test_doris_llm_session_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    session_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_session_request_1d", 1)[1]
    session_section = session_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d", 1)[0]

    assert render_llm_session_request_1d_doris_columns() in session_section


def test_doris_agent_run_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    agent_run_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d", 1)[1]
    agent_run_section = agent_run_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d", 1)[0]

    assert render_agent_run_1d_doris_columns() in agent_run_section


def test_doris_agent_tool_call_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    tool_daily_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d", 1)[1]
    tool_daily_section = tool_daily_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d", 1)[0]

    assert render_agent_tool_call_1d_doris_columns() in tool_daily_section


def test_doris_agent_orchestration_daily_metrics_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    orchestration_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d", 1)[1]
    orchestration_section = orchestration_section.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_platform_component_health_1d", 1)[0]

    assert render_agent_orchestration_handoff_1d_doris_columns() in orchestration_section


def test_doris_platform_component_health_columns_match_contract():
    sql = (REPO_ROOT / "sql" / "create_doris_tables.sql").read_text(encoding="utf-8")
    health_section = sql.split("CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_platform_component_health_1d", 1)[1]
    health_section = health_section.split("CREATE MATERIALIZED VIEW IF NOT EXISTS ai_observability.mv_daily_summary", 1)[0]

    assert render_platform_component_health_1d_doris_columns() in health_section


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
    assert "INSERT INTO ai_observability.dws_ai_llm_feature_env_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_feature_env_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_llm_region_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_region_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_agent_team_run_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_agent_team_run_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_llm_feature_request_1h" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_feature_request_1h" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_llm_session_request_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_llm_session_request_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_agent_orchestration_handoff_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d" in sync_sql
    assert "INSERT INTO ai_observability.dws_ai_platform_component_health_1d" in sync_sql
    assert "FROM paimon_lake.dws.dws_ai_platform_component_health_1d" in sync_sql


def test_doris_dashboard_queries_cover_tier_three_business_questions():
    sql = (REPO_ROOT / "sql" / "doris_dashboard_queries.sql").read_text(encoding="utf-8")

    assert "FROM ai_observability.dwd_ai_compliance_access_audit_di" in sql
    assert "WHERE access_granted = FALSE" in sql
    assert "FROM ai_observability.dwd_ai_compliance_data_retention_di" in sql
    assert "FROM ai_observability.dws_ai_agent_orchestration_handoff_1d" in sql
    assert "FROM ai_observability.dws_ai_platform_component_health_1d" in sql
    assert "WHERE is_breach = TRUE" in sql
    assert (
        "FROM ai_observability."
        "ads_observability_evaluation_dataset_experiment_regression"
    ) in sql
    assert "NULLIF(baseline_evaluation_count, 0)" in sql
    assert "AS is_quality_regression" in sql
    assert "AS is_cost_increase" in sql
    assert "AS is_latency_increase" in sql
