from app.warehouse_contract import TABLE_GRAINS


def test_contract_grains_cover_core_runtime_and_summary_tables():
    assert TABLE_GRAINS["dwd_ai_llm_request_di"] == "one row per LLM provider request attempt result"
    assert TABLE_GRAINS["dwd_ai_agent_run_di"] == "one row per Agent task/run"
    assert TABLE_GRAINS["dwd_ai_agent_tool_call_di"] == "one row per Agent tool invocation"
    assert TABLE_GRAINS["dws_ai_llm_feature_request_1d"] == "one daily row per app, feature, and model"
    assert TABLE_GRAINS["dws_ai_agent_agent_run_1d"] == "one daily row per app, agent, and task type"
    assert TABLE_GRAINS["dws_ai_retrieval_knowledge_base_request_1d"] == (
        "one daily row per app, knowledge base, embedding model, and strategy"
    )
    assert TABLE_GRAINS["dws_ai_agent_orchestration_handoff_1d"] == (
        "one daily row per parent agent, child agent, and handoff type"
    )
    assert TABLE_GRAINS["ads_observability_trace_health_detail"] == "one diagnostic row per unhealthy trace envelope"
    assert TABLE_GRAINS["ads_observability_evaluation_dataset_experiment_regression"] == (
        "one row per dataset, experiment, baseline/candidate variant-model-prompt pair, "
        "and evaluation dimension"
    )
