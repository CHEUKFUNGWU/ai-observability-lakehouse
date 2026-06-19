-- DWS stores reusable daily summary metrics.

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_feature_request_1d (
    `date` DATE,
    app_name STRING,
    feature_name STRING,
    model_name STRING,
    request_count BIGINT,
    success_count BIGINT,
    error_count BIGINT,
    prompt_tokens BIGINT,
    completion_tokens BIGINT,
    total_tokens BIGINT,
    estimated_cost_usd DOUBLE,
    avg_latency_ms DOUBLE,
    max_latency_ms BIGINT,
    p95_latency_ms BIGINT,
    PRIMARY KEY (`date`, app_name, feature_name, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_feature_request_1h (
    `date` DATE,
    `hour` INT,
    app_name STRING,
    feature_name STRING,
    model_name STRING,
    request_cnt_1h BIGINT,
    success_cnt_1h BIGINT,
    error_cnt_1h BIGINT,
    prompt_token_cnt_1h BIGINT,
    completion_token_cnt_1h BIGINT,
    total_token_cnt_1h BIGINT,
    estimated_cost_amt_1h DOUBLE,
    avg_latency_ms DOUBLE,
    max_latency_ms BIGINT,
    p95_latency_ms BIGINT,
    PRIMARY KEY (`date`, `hour`, app_name, feature_name, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_session_request_1d (
    `date` DATE,
    app_name STRING,
    feature_name STRING,
    session_cnt_1d BIGINT,
    avg_turns_per_session DOUBLE,
    avg_tokens_per_session DOUBLE,
    avg_duration_per_session_ms DOUBLE,
    resolved_session_cnt_1d BIGINT,
    PRIMARY KEY (`date`, app_name, feature_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d (
    `date` DATE,
    app_name STRING,
    knowledge_base_id STRING,
    embedding_model STRING,
    retrieval_strategy STRING,
    retrieval_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    zero_result_cnt_1d BIGINT,
    returned_cnt_1d BIGINT,
    hit_cnt_1d BIGINT,
    avg_similarity_score DOUBLE,
    avg_total_latency_ms DOUBLE,
    p95_total_latency_ms BIGINT,
    avg_embedding_latency_ms DOUBLE,
    avg_search_latency_ms DOUBLE,
    PRIMARY KEY (`date`, app_name, knowledge_base_id, embedding_model, retrieval_strategy) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_feedback_feature_action_1d (
    `date` DATE,
    app_name STRING,
    feature_name STRING,
    agent_id STRING,
    feedback_cnt_1d BIGINT,
    thumbs_up_cnt_1d BIGINT,
    thumbs_down_cnt_1d BIGINT,
    regenerate_cnt_1d BIGINT,
    report_cnt_1d BIGINT,
    avg_rating DOUBLE,
    rated_request_cnt_1d BIGINT,
    PRIMARY KEY (`date`, app_name, feature_name, agent_id) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_guardrail_rule_check_1d (
    `date` DATE,
    app_name STRING,
    rule_category STRING,
    action_taken STRING,
    check_cnt_1d BIGINT,
    triggered_cnt_1d BIGINT,
    block_cnt_1d BIGINT,
    redact_cnt_1d BIGINT,
    warn_cnt_1d BIGINT,
    avg_guardrail_latency_ms DOUBLE,
    distinct_user_cnt_1d BIGINT,
    PRIMARY KEY (`date`, app_name, rule_category, action_taken) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_cost_team_request_1d (
    `date` DATE,
    team_id STRING,
    app_name STRING,
    model_name STRING,
    request_cnt_1d BIGINT,
    total_token_cnt_1d BIGINT,
    estimated_cost_amt_1d DOUBLE,
    agent_run_cnt_1d BIGINT,
    agent_cost_amt_1d DOUBLE,
    PRIMARY KEY (`date`, team_id, app_name, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d (
    `date` DATE,
    app_name STRING,
    feature_name STRING,
    evaluation_dimension STRING,
    evaluated_model_name STRING,
    evaluation_cnt_1d BIGINT,
    pass_cnt_1d BIGINT,
    fail_cnt_1d BIGINT,
    avg_score DOUBLE,
    p10_score DOUBLE,
    avg_evaluation_latency_ms DOUBLE,
    PRIMARY KEY (`date`, app_name, feature_name, evaluation_dimension, evaluated_model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_prompt_version_request_1d (
    `date` DATE,
    prompt_id STRING,
    prompt_version STRING,
    model_name STRING,
    request_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    avg_latency_ms DOUBLE,
    p95_latency_ms BIGINT,
    total_token_cnt_1d BIGINT,
    estimated_cost_amt_1d DOUBLE,
    avg_evaluation_score DOUBLE,
    PRIMARY KEY (`date`, prompt_id, prompt_version, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_feature_env_request_1d (
    `date` DATE,
    app_name STRING,
    feature_name STRING,
    model_name STRING,
    environment STRING,
    request_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    prompt_token_cnt_1d BIGINT,
    completion_token_cnt_1d BIGINT,
    total_token_cnt_1d BIGINT,
    estimated_cost_amt_1d DOUBLE,
    avg_latency_ms DOUBLE,
    max_latency_ms BIGINT,
    p95_latency_ms BIGINT,
    PRIMARY KEY (`date`, app_name, feature_name, model_name, environment) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_region_request_1d (
    `date` DATE,
    region STRING,
    environment STRING,
    app_name STRING,
    model_name STRING,
    request_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    prompt_token_cnt_1d BIGINT,
    completion_token_cnt_1d BIGINT,
    total_token_cnt_1d BIGINT,
    estimated_cost_amt_1d DOUBLE,
    avg_latency_ms DOUBLE,
    max_latency_ms BIGINT,
    p95_latency_ms BIGINT,
    PRIMARY KEY (`date`, region, environment, app_name, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_agent_team_run_1d (
    `date` DATE,
    team_id STRING,
    app_name STRING,
    agent_id STRING,
    agent_name STRING,
    task_type STRING,
    run_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    turn_cnt_1d BIGINT,
    llm_call_cnt_1d BIGINT,
    tool_call_cnt_1d BIGINT,
    retrieval_cnt_1d BIGINT,
    total_token_cnt_1d BIGINT,
    estimated_cost_amt_1d DOUBLE,
    avg_duration_ms DOUBLE,
    p95_duration_ms BIGINT,
    span_cnt_1d BIGINT,
    failed_span_cnt_1d BIGINT,
    tool_span_cnt_1d BIGINT,
    llm_span_cnt_1d BIGINT,
    PRIMARY KEY (`date`, team_id, app_name, agent_id, agent_name, task_type) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d (
    `date` DATE,
    parent_agent_id STRING,
    child_agent_id STRING,
    handoff_type STRING,
    handoff_cnt_1d BIGINT,
    success_cnt_1d BIGINT,
    error_cnt_1d BIGINT,
    timeout_cnt_1d BIGINT,
    avg_handoff_latency_ms DOUBLE,
    p95_handoff_latency_ms BIGINT,
    PRIMARY KEY (`date`, parent_agent_id, child_agent_id, handoff_type) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_platform_component_health_1d (
    `date` DATE,
    component STRING,
    metric_name STRING,
    metric_value DOUBLE,
    threshold DOUBLE,
    is_breach BOOLEAN,
    PRIMARY KEY (`date`, component, metric_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);
