CREATE DATABASE IF NOT EXISTS ai_observability;

DROP TABLE IF EXISTS ai_observability.dwd_ai_llm_request_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_agent_run_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_agent_span_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_agent_tool_call_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_retrieval_request_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_feedback_action_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_guardrail_check_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_evaluation_judgment_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_model_deployment_di;
DROP TABLE IF EXISTS ai_observability.dws_ai_llm_feature_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_llm_feature_request_1h;
DROP TABLE IF EXISTS ai_observability.dws_ai_llm_session_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_agent_agent_run_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_feedback_feature_action_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_guardrail_rule_check_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_cost_team_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_prompt_version_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_llm_feature_env_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_llm_region_request_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_agent_team_run_1d;
DROP TABLE IF EXISTS ai_observability.dim_model_df;
DROP TABLE IF EXISTS ai_observability.dim_knowledge_base_df;
DROP TABLE IF EXISTS ai_observability.dim_guardrail_rule_df;
DROP TABLE IF EXISTS ai_observability.dim_team_df;
DROP TABLE IF EXISTS ai_observability.dim_user_df;
DROP TABLE IF EXISTS ai_observability.dim_prompt_version_df;
DROP TABLE IF EXISTS ai_observability.dim_model_version_df;
DROP TABLE IF EXISTS ai_observability.ads_observability_cost_feature_anomaly;
DROP TABLE IF EXISTS ai_observability.ads_observability_sla_feature_report;
DROP TABLE IF EXISTS ai_observability.ads_observability_prompt_prompt_version_metrics;
DROP TABLE IF EXISTS ai_observability.ads_observability_retrieval_daily_quality;
DROP TABLE IF EXISTS ai_observability.ads_observability_feedback_daily_satisfaction;
DROP TABLE IF EXISTS ai_observability.ads_observability_guardrail_daily_violation;
DROP TABLE IF EXISTS ai_observability.ads_observability_cost_daily_budget;
DROP TABLE IF EXISTS ai_observability.ads_observability_cost_monthly_chargeback;
DROP TABLE IF EXISTS ai_observability.ads_observability_executive_weekly_summary;
DROP MATERIALIZED VIEW IF EXISTS ai_observability.mv_daily_summary;

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_llm_request_di
(
    `date` DATE NOT NULL,
    request_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    run_id VARCHAR(128) NOT NULL DEFAULT "",
    span_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_name VARCHAR(256) NOT NULL DEFAULT "",
    channel VARCHAR(64) NOT NULL DEFAULT "",
    user_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(128) NOT NULL DEFAULT "",
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    prompt_category VARCHAR(256) NOT NULL,
    prompt_id VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    provider VARCHAR(128) NOT NULL,
    prompt_hash VARCHAR(128) NOT NULL DEFAULT "",
    response_hash VARCHAR(128) NOT NULL DEFAULT "",
    input_chars INT NOT NULL DEFAULT "0",
    output_chars INT NOT NULL DEFAULT "0",
    prompt_tokens INT NOT NULL,
    completion_tokens INT NOT NULL,
    total_tokens INT NOT NULL,
    request_type VARCHAR(64) NOT NULL DEFAULT "chat",
    is_streaming BOOLEAN NOT NULL DEFAULT "false",
    temperature DOUBLE NOT NULL DEFAULT "0",
    max_tokens INT NOT NULL DEFAULT "0",
    finish_reason VARCHAR(64) NOT NULL DEFAULT "",
    retry_count INT NOT NULL DEFAULT "0",
    latency_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(128) NULL,
    http_status SMALLINT NOT NULL,
    estimated_cost_usd DOUBLE NOT NULL,
    mode VARCHAR(32) NOT NULL,
    region VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, request_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(request_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_run_di
(
    `date` DATE NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_id VARCHAR(128) NOT NULL,
    agent_name VARCHAR(256) NOT NULL,
    agent_version VARCHAR(128) NOT NULL DEFAULT "",
    app_name VARCHAR(256) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(128) NOT NULL DEFAULT "",
    task_type VARCHAR(128) NOT NULL,
    channel VARCHAR(64) NOT NULL DEFAULT "",
    toolsets_used STRING NULL,
    input_text_hash VARCHAR(128) NOT NULL DEFAULT "",
    output_text_hash VARCHAR(128) NOT NULL DEFAULT "",
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    duration_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(128) NULL,
    turn_count INT NOT NULL DEFAULT "0",
    llm_call_count INT NOT NULL DEFAULT "0",
    tool_call_count INT NOT NULL DEFAULT "0",
    retrieval_count INT NOT NULL DEFAULT "0",
    total_tokens INT NOT NULL DEFAULT "0",
    estimated_cost_usd DOUBLE NOT NULL DEFAULT "0",
    mode VARCHAR(32) NOT NULL,
    region VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, run_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(run_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_span_di
(
    `date` DATE NOT NULL,
    span_id VARCHAR(128) NOT NULL,
    parent_span_id VARCHAR(128) NULL,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_id VARCHAR(128) NOT NULL,
    span_name VARCHAR(256) NOT NULL,
    span_type VARCHAR(64) NOT NULL,
    span_order INT NOT NULL DEFAULT "0",
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    duration_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(128) NULL,
    retry_count INT NOT NULL DEFAULT "0",
    input_size INT NOT NULL DEFAULT "0",
    output_size INT NOT NULL DEFAULT "0",
    model_name VARCHAR(256) NULL,
    tool_name VARCHAR(256) NULL,
    mode VARCHAR(32) NOT NULL,
    region VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, span_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(span_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_agent_tool_call_di
(
    `date` DATE NOT NULL,
    tool_call_id VARCHAR(128) NOT NULL,
    span_id VARCHAR(128) NOT NULL,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_id VARCHAR(128) NOT NULL,
    tool_name VARCHAR(256) NOT NULL,
    tool_type VARCHAR(64) NOT NULL,
    arguments_json STRING NOT NULL,
    result_text STRING NOT NULL,
    result_size INT NOT NULL DEFAULT "0",
    duration_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(128) NULL,
    retry_count INT NOT NULL DEFAULT "0",
    mode VARCHAR(32) NOT NULL,
    region VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, tool_call_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(tool_call_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_count BIGINT NOT NULL,
    success_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    prompt_tokens BIGINT NOT NULL,
    completion_tokens BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    estimated_cost_usd DOUBLE NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    max_latency_ms BIGINT NOT NULL,
    p95_latency_ms BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_request_1h
(
    `date` DATE NOT NULL,
    `hour` TINYINT NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_cnt_1h BIGINT NOT NULL,
    success_cnt_1h BIGINT NOT NULL,
    error_cnt_1h BIGINT NOT NULL,
    prompt_token_cnt_1h BIGINT NOT NULL,
    completion_token_cnt_1h BIGINT NOT NULL,
    total_token_cnt_1h BIGINT NOT NULL,
    estimated_cost_amt_1h DOUBLE NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    max_latency_ms BIGINT NOT NULL,
    p95_latency_ms BIGINT NOT NULL
)
DUPLICATE KEY(`date`, `hour`, app_name, feature_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_session_request_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    session_cnt_1d BIGINT NOT NULL,
    avg_turns_per_session DOUBLE NOT NULL,
    avg_tokens_per_session DOUBLE NOT NULL,
    avg_duration_per_session_ms DOUBLE NOT NULL,
    resolved_session_cnt_1d BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_retrieval_request_di
(
    `date` DATE NOT NULL,
    retrieval_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    run_id VARCHAR(128) NOT NULL DEFAULT "",
    span_id VARCHAR(128) NOT NULL DEFAULT "",
    request_id VARCHAR(128) NOT NULL DEFAULT "",
    agent_id VARCHAR(128) NOT NULL DEFAULT "",
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    knowledge_base_id VARCHAR(128) NOT NULL,
    knowledge_base_name VARCHAR(256) NOT NULL DEFAULT "",
    embedding_model VARCHAR(256) NOT NULL,
    retrieval_strategy VARCHAR(64) NOT NULL,
    query_text_hash VARCHAR(128) NOT NULL DEFAULT "",
    query_length INT NOT NULL DEFAULT "0",
    top_k INT NOT NULL,
    returned_count INT NOT NULL,
    hit_count INT NOT NULL,
    max_similarity_score DOUBLE NOT NULL,
    min_similarity_score DOUBLE NOT NULL,
    avg_similarity_score DOUBLE NOT NULL,
    embedding_latency_ms INT NOT NULL,
    search_latency_ms INT NOT NULL,
    total_latency_ms INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(128) NULL,
    mode VARCHAR(32) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, retrieval_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(retrieval_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_feedback_action_di
(
    `date` DATE NOT NULL,
    feedback_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    request_id VARCHAR(128) NOT NULL DEFAULT "",
    run_id VARCHAR(128) NOT NULL DEFAULT "",
    session_id VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(128) NOT NULL DEFAULT "",
    user_id VARCHAR(128) NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    agent_id VARCHAR(128) NOT NULL DEFAULT "",
    feedback_type VARCHAR(64) NOT NULL,
    rating_value INT NULL,
    feedback_text_hash VARCHAR(128) NOT NULL DEFAULT "",
    feedback_text_length INT NOT NULL DEFAULT "0",
    response_latency_ms INT NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    mode VARCHAR(32) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, feedback_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(feedback_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_guardrail_check_di
(
    `date` DATE NOT NULL,
    guardrail_event_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    request_id VARCHAR(128) NOT NULL DEFAULT "",
    run_id VARCHAR(128) NOT NULL DEFAULT "",
    user_id VARCHAR(128) NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    guardrail_stage VARCHAR(64) NOT NULL,
    rule_name VARCHAR(256) NOT NULL,
    rule_category VARCHAR(64) NOT NULL,
    triggered BOOLEAN NOT NULL,
    action_taken VARCHAR(64) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    matched_pattern_hash VARCHAR(128) NOT NULL DEFAULT "",
    input_text_length INT NOT NULL DEFAULT "0",
    guardrail_latency_ms INT NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    mode VARCHAR(32) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, guardrail_event_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(guardrail_event_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_evaluation_judgment_di
(
    `date` DATE NOT NULL,
    evaluation_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL DEFAULT "",
    request_id VARCHAR(128) NOT NULL DEFAULT "",
    run_id VARCHAR(128) NOT NULL DEFAULT "",
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    evaluator_type VARCHAR(64) NOT NULL,
    evaluator_model VARCHAR(256) NOT NULL DEFAULT "",
    evaluation_dimension VARCHAR(64) NOT NULL,
    score DOUBLE NOT NULL,
    raw_score VARCHAR(128) NOT NULL DEFAULT "",
    pass_threshold DOUBLE NOT NULL,
    passed BOOLEAN NOT NULL,
    evaluated_model_name VARCHAR(256) NOT NULL,
    evaluated_prompt_version VARCHAR(64) NOT NULL,
    evaluation_latency_ms INT NOT NULL,
    mode VARCHAR(32) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, evaluation_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(evaluation_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_model_deployment_di
(
    `date` DATE NOT NULL,
    deployment_id VARCHAR(128) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    model_version VARCHAR(128) NOT NULL,
    provider VARCHAR(128) NOT NULL,
    deployment_action VARCHAR(64) NOT NULL,
    traffic_percentage DOUBLE NOT NULL,
    target_environment VARCHAR(32) NOT NULL,
    deployer_user_id VARCHAR(128) NOT NULL,
    deploy_reason VARCHAR(256) NOT NULL DEFAULT "",
    status VARCHAR(32) NOT NULL,
    created_at DATETIME NOT NULL
)
DUPLICATE KEY(`date`, deployment_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(deployment_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_model_df
(
    model_name VARCHAR(256) NOT NULL,
    provider VARCHAR(128) NOT NULL,
    input_price_per_1m_tokens DOUBLE NOT NULL,
    output_price_per_1m_tokens DOUBLE NOT NULL,
    max_context_tokens INT NOT NULL,
    release_date DATE NOT NULL,
    status VARCHAR(32) NOT NULL
)
UNIQUE KEY(model_name)
DISTRIBUTED BY HASH(model_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_knowledge_base_df
(
    knowledge_base_id VARCHAR(128) NOT NULL,
    knowledge_base_name VARCHAR(256) NOT NULL,
    knowledge_base_type VARCHAR(64) NOT NULL,
    document_count BIGINT NOT NULL DEFAULT "0",
    owner_team VARCHAR(256) NOT NULL DEFAULT "",
    last_updated_at DATETIME NULL,
    status VARCHAR(32) NOT NULL
)
UNIQUE KEY(knowledge_base_id)
DISTRIBUTED BY HASH(knowledge_base_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_guardrail_rule_df
(
    rule_name VARCHAR(256) NOT NULL,
    rule_category VARCHAR(64) NOT NULL,
    default_severity VARCHAR(32) NOT NULL,
    owner_team VARCHAR(256) NOT NULL DEFAULT "",
    description STRING NULL,
    status VARCHAR(32) NOT NULL
)
UNIQUE KEY(rule_name)
DISTRIBUTED BY HASH(rule_name) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_team_df
(
    team_id VARCHAR(128) NOT NULL,
    team_name VARCHAR(256) NOT NULL,
    department VARCHAR(256) NOT NULL,
    cost_center VARCHAR(128) NOT NULL,
    budget_monthly_usd DOUBLE NOT NULL,
    manager VARCHAR(256) NOT NULL,
    status VARCHAR(32) NOT NULL
)
UNIQUE KEY(team_id)
DISTRIBUTED BY HASH(team_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_user_df
(
    user_id VARCHAR(128) NOT NULL,
    user_name VARCHAR(256) NOT NULL,
    team_id VARCHAR(128) NOT NULL,
    role VARCHAR(128) NOT NULL,
    ai_access_tier VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL
)
UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_prompt_version_df
(
    prompt_id VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    prompt_name VARCHAR(256) NOT NULL,
    owner_team_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    release_date DATE NOT NULL,
    ab_test_group VARCHAR(64) NOT NULL DEFAULT "",
    description STRING NULL
)
UNIQUE KEY(prompt_id, prompt_version)
DISTRIBUTED BY HASH(prompt_id, prompt_version) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dim_model_version_df
(
    model_name VARCHAR(256) NOT NULL,
    model_version VARCHAR(128) NOT NULL,
    provider VARCHAR(128) NOT NULL,
    deployment_status VARCHAR(64) NOT NULL,
    first_deployed_at DATETIME NOT NULL,
    last_deployed_at DATETIME NOT NULL,
    is_current_prod BOOLEAN NOT NULL
)
UNIQUE KEY(model_name, model_version)
DISTRIBUTED BY HASH(model_name, model_version) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_agent_run_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    agent_name VARCHAR(256) NOT NULL,
    task_type VARCHAR(128) NOT NULL,
    run_count BIGINT NOT NULL,
    success_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    turn_count BIGINT NOT NULL,
    llm_call_count BIGINT NOT NULL,
    tool_call_count BIGINT NOT NULL,
    retrieval_count BIGINT NOT NULL,
    total_tokens BIGINT NOT NULL,
    estimated_cost_usd DOUBLE NOT NULL,
    avg_duration_ms DOUBLE NOT NULL,
    p95_duration_ms BIGINT NOT NULL,
    span_count BIGINT NOT NULL,
    failed_span_count BIGINT NOT NULL,
    tool_span_count BIGINT NOT NULL,
    llm_span_count BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, agent_id, agent_name, task_type)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(agent_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_tool_tool_call_1d
(
    `date` DATE NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    tool_name VARCHAR(256) NOT NULL,
    tool_type VARCHAR(64) NOT NULL,
    tool_call_count BIGINT NOT NULL,
    success_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    retry_count BIGINT NOT NULL,
    avg_duration_ms DOUBLE NOT NULL,
    p95_duration_ms BIGINT NOT NULL,
    avg_result_size DOUBLE NOT NULL,
    max_result_size BIGINT NOT NULL
)
DUPLICATE KEY(`date`, agent_id, tool_name, tool_type)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(agent_id, tool_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_retrieval_knowledge_base_request_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    knowledge_base_id VARCHAR(128) NOT NULL,
    embedding_model VARCHAR(256) NOT NULL,
    retrieval_strategy VARCHAR(64) NOT NULL,
    retrieval_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    zero_result_cnt_1d BIGINT NOT NULL,
    returned_cnt_1d BIGINT NOT NULL,
    hit_cnt_1d BIGINT NOT NULL,
    avg_similarity_score DOUBLE NOT NULL,
    avg_total_latency_ms DOUBLE NOT NULL,
    p95_total_latency_ms BIGINT NOT NULL,
    avg_embedding_latency_ms DOUBLE NOT NULL,
    avg_search_latency_ms DOUBLE NOT NULL
)
DUPLICATE KEY(`date`, app_name, knowledge_base_id, embedding_model, retrieval_strategy)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(knowledge_base_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_feedback_feature_action_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    feedback_cnt_1d BIGINT NOT NULL,
    thumbs_up_cnt_1d BIGINT NOT NULL,
    thumbs_down_cnt_1d BIGINT NOT NULL,
    regenerate_cnt_1d BIGINT NOT NULL,
    report_cnt_1d BIGINT NOT NULL,
    avg_rating DOUBLE NULL,
    rated_request_cnt_1d BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, agent_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_guardrail_rule_check_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    rule_category VARCHAR(64) NOT NULL,
    action_taken VARCHAR(64) NOT NULL,
    check_cnt_1d BIGINT NOT NULL,
    triggered_cnt_1d BIGINT NOT NULL,
    block_cnt_1d BIGINT NOT NULL,
    redact_cnt_1d BIGINT NOT NULL,
    warn_cnt_1d BIGINT NOT NULL,
    avg_guardrail_latency_ms DOUBLE NOT NULL,
    distinct_user_cnt_1d BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, rule_category, action_taken)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, rule_category) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_cost_team_request_1d
(
    `date` DATE NOT NULL,
    team_id VARCHAR(128) NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_cnt_1d BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    agent_run_cnt_1d BIGINT NOT NULL,
    agent_cost_amt_1d DOUBLE NOT NULL
)
DUPLICATE KEY(`date`, team_id, app_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(team_id, app_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_evaluation_feature_judgment_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    evaluation_dimension VARCHAR(64) NOT NULL,
    evaluated_model_name VARCHAR(256) NOT NULL,
    evaluation_cnt_1d BIGINT NOT NULL,
    pass_cnt_1d BIGINT NOT NULL,
    fail_cnt_1d BIGINT NOT NULL,
    avg_score DOUBLE NOT NULL,
    p10_score DOUBLE NOT NULL,
    avg_evaluation_latency_ms DOUBLE NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, evaluation_dimension, evaluated_model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_prompt_version_request_1d
(
    `date` DATE NOT NULL,
    prompt_id VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    p95_latency_ms BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    avg_evaluation_score DOUBLE NULL
)
DUPLICATE KEY(`date`, prompt_id, prompt_version, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(prompt_id, prompt_version) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_feature_env_request_1d
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    request_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    prompt_token_cnt_1d BIGINT NOT NULL,
    completion_token_cnt_1d BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    max_latency_ms BIGINT NOT NULL,
    p95_latency_ms BIGINT NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, model_name, environment)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_llm_region_request_1d
(
    `date` DATE NOT NULL,
    region VARCHAR(64) NOT NULL,
    environment VARCHAR(32) NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    prompt_token_cnt_1d BIGINT NOT NULL,
    completion_token_cnt_1d BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    max_latency_ms BIGINT NOT NULL,
    p95_latency_ms BIGINT NOT NULL
)
DUPLICATE KEY(`date`, region, environment, app_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(region, app_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_team_run_1d
(
    `date` DATE NOT NULL,
    team_id VARCHAR(128) NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    agent_name VARCHAR(256) NOT NULL,
    task_type VARCHAR(128) NOT NULL,
    run_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    turn_cnt_1d BIGINT NOT NULL,
    llm_call_cnt_1d BIGINT NOT NULL,
    tool_call_cnt_1d BIGINT NOT NULL,
    retrieval_cnt_1d BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    avg_duration_ms DOUBLE NOT NULL,
    p95_duration_ms BIGINT NOT NULL,
    span_cnt_1d BIGINT NOT NULL,
    failed_span_cnt_1d BIGINT NOT NULL,
    tool_span_cnt_1d BIGINT NOT NULL,
    llm_span_cnt_1d BIGINT NOT NULL
)
DUPLICATE KEY(`date`, team_id, app_name, agent_id, agent_name, task_type)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(team_id, agent_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_feature_anomaly
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_count BIGINT NOT NULL,
    estimated_cost_usd DOUBLE NOT NULL,
    prev_day_cost DOUBLE NULL,
    cost_change_rate DOUBLE NULL,
    is_anomaly BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_sla_feature_report
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    p95_latency_ms BIGINT NOT NULL,
    error_rate DOUBLE NOT NULL,
    p95_latency_ms_max BIGINT NULL,
    error_rate_max DOUBLE NULL,
    is_latency_breach BOOLEAN NOT NULL,
    is_error_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_prompt_prompt_version_metrics
(
    `date` DATE NOT NULL,
    prompt_id VARCHAR(128) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    model_name VARCHAR(256) NOT NULL,
    request_count BIGINT NOT NULL,
    avg_latency_ms DOUBLE NOT NULL,
    p95_latency_ms BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    estimated_cost_usd DOUBLE NOT NULL
)
DUPLICATE KEY(`date`, prompt_id, prompt_version, model_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(prompt_id, prompt_version) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_retrieval_daily_quality
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    knowledge_base_id VARCHAR(128) NOT NULL,
    embedding_model VARCHAR(256) NOT NULL,
    retrieval_strategy VARCHAR(64) NOT NULL,
    retrieval_cnt_1d BIGINT NOT NULL,
    zero_result_cnt_1d BIGINT NOT NULL,
    returned_cnt_1d BIGINT NOT NULL,
    hit_cnt_1d BIGINT NOT NULL,
    zero_result_rate_1d DOUBLE NULL,
    hit_rate_1d DOUBLE NULL,
    p95_total_latency_ms BIGINT NOT NULL,
    p95_total_latency_ms_max BIGINT NOT NULL,
    zero_result_rate_max DOUBLE NOT NULL,
    is_latency_breach BOOLEAN NOT NULL,
    is_zero_result_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, app_name, knowledge_base_id, embedding_model, retrieval_strategy)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(knowledge_base_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_feedback_daily_satisfaction
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    feature_name VARCHAR(256) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    feedback_cnt_1d BIGINT NOT NULL,
    thumbs_up_cnt_1d BIGINT NOT NULL,
    thumbs_down_cnt_1d BIGINT NOT NULL,
    regenerate_cnt_1d BIGINT NOT NULL,
    request_cnt_1d BIGINT NULL,
    satisfaction_rate_1d DOUBLE NULL,
    regeneration_rate_1d DOUBLE NULL,
    satisfaction_rate_min DOUBLE NOT NULL,
    regeneration_rate_max DOUBLE NOT NULL,
    is_satisfaction_breach BOOLEAN NOT NULL,
    is_regeneration_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, app_name, feature_name, agent_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, feature_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_guardrail_daily_violation
(
    `date` DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    rule_category VARCHAR(64) NOT NULL,
    action_taken VARCHAR(64) NOT NULL,
    check_cnt_1d BIGINT NOT NULL,
    triggered_cnt_1d BIGINT NOT NULL,
    block_cnt_1d BIGINT NOT NULL,
    avg_guardrail_latency_ms DOUBLE NOT NULL,
    trigger_rate_1d DOUBLE NULL,
    block_rate_1d DOUBLE NULL,
    trigger_rate_max DOUBLE NOT NULL,
    p95_latency_ms_max BIGINT NOT NULL,
    is_trigger_rate_breach BOOLEAN NOT NULL,
    is_latency_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, app_name, rule_category, action_taken)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(app_name, rule_category) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_daily_budget
(
    `date` DATE NOT NULL,
    team_id VARCHAR(128) NOT NULL,
    team_name VARCHAR(256) NULL,
    department VARCHAR(256) NULL,
    cost_center VARCHAR(128) NULL,
    app_name VARCHAR(256) NOT NULL,
    request_cnt_1d BIGINT NOT NULL,
    total_token_cnt_1d BIGINT NOT NULL,
    estimated_cost_amt_1d DOUBLE NOT NULL,
    agent_run_cnt_1d BIGINT NOT NULL,
    agent_cost_amt_1d DOUBLE NOT NULL,
    total_cost_amt_1d DOUBLE NOT NULL,
    cost_mtd_amt DOUBLE NOT NULL,
    projected_month_end_cost_amt DOUBLE NULL,
    budget_monthly_amt DOUBLE NULL,
    budget_utilization_rate_mtd DOUBLE NULL,
    is_budget_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, team_id, app_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(team_id, app_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_cost_monthly_chargeback
(
    month_start_date DATE NOT NULL,
    team_id VARCHAR(128) NOT NULL,
    team_name VARCHAR(256) NULL,
    department VARCHAR(256) NULL,
    cost_center VARCHAR(128) NULL,
    request_cnt_1m BIGINT NOT NULL,
    total_token_cnt_1m BIGINT NOT NULL,
    llm_cost_amt_1m DOUBLE NOT NULL,
    agent_run_cnt_1m BIGINT NOT NULL,
    agent_cost_amt_1m DOUBLE NOT NULL,
    chargeback_amt_1m DOUBLE NOT NULL,
    budget_monthly_amt DOUBLE NULL,
    budget_variance_amt_1m DOUBLE NULL,
    budget_utilization_rate_1m DOUBLE NULL,
    is_budget_overrun BOOLEAN NOT NULL
)
DUPLICATE KEY(month_start_date, team_id)
PARTITION BY RANGE(month_start_date) ()
DISTRIBUTED BY HASH(team_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE TABLE IF NOT EXISTS ai_observability.ads_observability_executive_weekly_summary
(
    week_start_date DATE NOT NULL,
    app_name VARCHAR(256) NOT NULL,
    request_cnt_1w BIGINT NOT NULL,
    success_cnt_1w BIGINT NOT NULL,
    error_cnt_1w BIGINT NOT NULL,
    total_token_cnt_1w BIGINT NOT NULL,
    llm_cost_amt_1w DOUBLE NOT NULL,
    p95_latency_ms_max BIGINT NULL,
    agent_run_cnt_1w BIGINT NOT NULL,
    agent_success_cnt_1w BIGINT NOT NULL,
    agent_error_cnt_1w BIGINT NOT NULL,
    agent_cost_amt_1w DOUBLE NOT NULL,
    retrieval_cnt_1w BIGINT NOT NULL,
    retrieval_returned_cnt_1w BIGINT NOT NULL,
    retrieval_hit_cnt_1w BIGINT NOT NULL,
    feedback_cnt_1w BIGINT NOT NULL,
    thumbs_up_cnt_1w BIGINT NOT NULL,
    thumbs_down_cnt_1w BIGINT NOT NULL,
    guardrail_check_cnt_1w BIGINT NOT NULL,
    guardrail_triggered_cnt_1w BIGINT NOT NULL,
    guardrail_block_cnt_1w BIGINT NOT NULL,
    evaluation_cnt_1w BIGINT NOT NULL,
    evaluation_pass_cnt_1w BIGINT NOT NULL,
    evaluation_fail_cnt_1w BIGINT NOT NULL,
    avg_latency_ms DOUBLE NULL,
    retrieval_hit_rate_1w DOUBLE NULL,
    satisfaction_rate_1w DOUBLE NULL,
    evaluation_pass_rate_1w DOUBLE NULL,
    avg_evaluation_score DOUBLE NULL,
    total_ai_cost_amt_1w DOUBLE NOT NULL
)
DUPLICATE KEY(week_start_date, app_name)
PARTITION BY RANGE(week_start_date) ()
DISTRIBUTED BY HASH(app_name) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true"
);

CREATE MATERIALIZED VIEW IF NOT EXISTS ai_observability.mv_daily_summary
BUILD IMMEDIATE
REFRESH AUTO ON MANUAL
DISTRIBUTED BY HASH(`date`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    `date`,
    SUM(request_count) AS request_count,
    SUM(success_count) AS success_count,
    SUM(error_count) AS error_count,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_cost_usd) AS estimated_cost_usd
FROM ai_observability.dws_ai_llm_feature_request_1d
GROUP BY `date`;
