-- DWD contains typed and validated business facts.

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_llm_request_di (
    request_id STRING,
    trace_id STRING,
    run_id STRING,
    span_id STRING,
    agent_id STRING,
    agent_name STRING,
    channel STRING,
    user_id STRING,
    session_id STRING,
    conversation_id STRING,
    app_name STRING,
    feature_name STRING,
    prompt_category STRING,
    prompt_id STRING,
    prompt_version STRING,
    model_name STRING,
    provider STRING,
    prompt_hash STRING,
    response_hash STRING,
    input_chars INT,
    output_chars INT,
    prompt_tokens INT,
    completion_tokens INT,
    total_tokens INT,
    request_type STRING,
    is_streaming BOOLEAN,
    temperature DOUBLE,
    max_tokens INT,
    finish_reason STRING,
    retry_count INT,
    latency_ms INT,
    status STRING,
    error_type STRING,
    http_status INT,
    estimated_cost_usd DOUBLE,
    mode STRING,
    region STRING,
    environment STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,
    PRIMARY KEY (request_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_retrieval_request_di (
    retrieval_id STRING,
    trace_id STRING,
    run_id STRING,
    span_id STRING,
    request_id STRING,
    agent_id STRING,
    app_name STRING,
    feature_name STRING,
    user_id STRING,
    knowledge_base_id STRING,
    knowledge_base_name STRING,
    embedding_model STRING,
    retrieval_strategy STRING,
    query_text_hash STRING,
    query_length INT,
    top_k INT,
    returned_count INT,
    hit_count INT,
    max_similarity_score DOUBLE,
    min_similarity_score DOUBLE,
    avg_similarity_score DOUBLE,
    embedding_latency_ms INT,
    search_latency_ms INT,
    total_latency_ms INT,
    status STRING,
    error_type STRING,
    mode STRING,
    environment STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (retrieval_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_feedback_action_di (
    feedback_id STRING,
    trace_id STRING,
    request_id STRING,
    run_id STRING,
    session_id STRING,
    conversation_id STRING,
    user_id STRING,
    app_name STRING,
    feature_name STRING,
    agent_id STRING,
    feedback_type STRING,
    rating_value INT,
    feedback_text_hash STRING,
    feedback_text_length INT,
    response_latency_ms INT,
    model_name STRING,
    prompt_version STRING,
    mode STRING,
    environment STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (feedback_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_guardrail_check_di (
    guardrail_event_id STRING,
    trace_id STRING,
    request_id STRING,
    run_id STRING,
    user_id STRING,
    app_name STRING,
    feature_name STRING,
    guardrail_stage STRING,
    rule_name STRING,
    rule_category STRING,
    triggered BOOLEAN,
    action_taken STRING,
    severity STRING,
    matched_pattern_hash STRING,
    input_text_length INT,
    guardrail_latency_ms INT,
    model_name STRING,
    prompt_version STRING,
    mode STRING,
    environment STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (guardrail_event_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_evaluation_judgment_di (
    evaluation_id STRING,
    trace_id STRING,
    request_id STRING,
    run_id STRING,
    app_name STRING,
    feature_name STRING,
    evaluator_type STRING,
    evaluator_model STRING,
    evaluation_dimension STRING,
    score DOUBLE,
    raw_score STRING,
    pass_threshold DOUBLE,
    passed BOOLEAN,
    evaluated_model_name STRING,
    evaluated_prompt_version STRING,
    evaluation_latency_ms INT,
    mode STRING,
    environment STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (evaluation_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_model_deployment_di (
    deployment_id STRING,
    model_name STRING,
    model_version STRING,
    provider STRING,
    deployment_action STRING,
    traffic_percentage DOUBLE,
    target_environment STRING,
    deployer_user_id STRING,
    deploy_reason STRING,
    status STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (deployment_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_compliance_access_audit_di (
    audit_event_id STRING,
    user_id STRING,
    action_type STRING,
    resource_type STRING,
    resource_id STRING,
    ip_address STRING,
    access_granted BOOLEAN,
    denial_reason STRING,
    data_classification STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (audit_event_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_compliance_data_retention_di (
    retention_event_id STRING,
    table_name STRING,
    partition_date DATE,
    action_type STRING,
    rows_affected BIGINT,
    policy_name STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (retention_event_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);

CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_agent_orchestration_di (
    orchestration_id STRING,
    trace_id STRING,
    parent_run_id STRING,
    child_run_id STRING,
    parent_agent_id STRING,
    child_agent_id STRING,
    handoff_type STRING,
    payload_size INT,
    handoff_latency_ms INT,
    status STRING,
    created_at TIMESTAMP(3),
    `date` DATE,
    PRIMARY KEY (orchestration_id) NOT ENFORCED
) WITH (
    'bucket' = '4'
);
