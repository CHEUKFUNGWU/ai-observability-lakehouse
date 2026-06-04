CREATE DATABASE IF NOT EXISTS ai_observability;

DROP TABLE IF EXISTS ai_observability.dwd_llm_request_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_run_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_span_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_tool_call_events;
DROP TABLE IF EXISTS ai_observability.ads_llm_feature_daily_metrics;
DROP TABLE IF EXISTS ai_observability.ads_agent_daily_metrics;
DROP TABLE IF EXISTS ai_observability.ads_agent_tool_daily_metrics;

CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events
(
    request_id String,
    trace_id String,
    run_id String,
    span_id String,
    agent_id String,
    agent_name String,
    channel String,
    user_id String,
    session_id String,
    conversation_id String,
    app_name String,
    feature_name String,
    prompt_category String,
    prompt_id String,
    prompt_version String,
    model_name String,
    provider String,
    prompt_hash String,
    response_hash String,
    input_chars UInt32,
    output_chars UInt32,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    total_tokens UInt32,
    request_type String,
    is_streaming Bool,
    temperature Float64,
    max_tokens UInt32,
    finish_reason String,
    retry_count UInt32,
    latency_ms UInt32,
    status String,
    error_type Nullable(String),
    http_status UInt16,
    estimated_cost_usd Float64,
    mode String,
    region String,
    environment String,
    created_at DateTime64(3, 'UTC'),
    date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, app_name, feature_name, model_name, request_id);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events
(
    run_id String,
    trace_id String,
    agent_id String,
    agent_name String,
    agent_version String,
    app_name String,
    user_id String,
    session_id String,
    conversation_id String,
    task_type String,
    channel String,
    toolsets_used String,
    input_text_hash String,
    output_text_hash String,
    start_time DateTime64(3, 'UTC'),
    end_time DateTime64(3, 'UTC'),
    duration_ms UInt32,
    status String,
    error_type Nullable(String),
    turn_count UInt32,
    llm_call_count UInt32,
    tool_call_count UInt32,
    retrieval_count UInt32,
    total_tokens UInt32,
    estimated_cost_usd Float64,
    mode String,
    region String,
    environment String,
    created_at DateTime64(3, 'UTC'),
    date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, app_name, agent_id, run_id);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events
(
    span_id String,
    parent_span_id Nullable(String),
    run_id String,
    trace_id String,
    agent_id String,
    span_name String,
    span_type String,
    span_order UInt32,
    start_time DateTime64(3, 'UTC'),
    end_time DateTime64(3, 'UTC'),
    duration_ms UInt32,
    status String,
    error_type Nullable(String),
    retry_count UInt32,
    input_size UInt32,
    output_size UInt32,
    model_name Nullable(String),
    tool_name Nullable(String),
    mode String,
    region String,
    environment String,
    created_at DateTime64(3, 'UTC'),
    date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, agent_id, run_id, span_id);

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events
(
    tool_call_id String,
    span_id String,
    run_id String,
    trace_id String,
    agent_id String,
    tool_name String,
    tool_type String,
    arguments_json String,
    result_text String,
    result_size UInt32,
    duration_ms UInt32,
    status String,
    error_type Nullable(String),
    retry_count UInt32,
    mode String,
    region String,
    environment String,
    created_at DateTime64(3, 'UTC'),
    date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, agent_id, tool_name, tool_call_id);

CREATE TABLE IF NOT EXISTS ai_observability.ads_llm_feature_daily_metrics
(
    date Date,
    app_name String,
    feature_name String,
    model_name String,
    request_count UInt64,
    success_count UInt64,
    error_count UInt64,
    prompt_tokens UInt64,
    completion_tokens UInt64,
    total_tokens UInt64,
    estimated_cost_usd Float64,
    avg_latency_ms Float64,
    p95_latency_ms UInt64
)

ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, app_name, feature_name, model_name);

CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_daily_metrics
(
    date Date,
    app_name String,
    agent_id String,
    agent_name String,
    task_type String,
    run_count UInt64,
    success_count UInt64,
    error_count UInt64,
    turn_count UInt64,
    llm_call_count UInt64,
    tool_call_count UInt64,
    retrieval_count UInt64,
    total_tokens UInt64,
    estimated_cost_usd Float64,
    avg_duration_ms Float64,
    p95_duration_ms UInt64,
    span_count UInt64,
    failed_span_count UInt64,
    tool_span_count UInt64,
    llm_span_count UInt64,
    span_failure_rate Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, app_name, agent_id, task_type);

CREATE TABLE IF NOT EXISTS ai_observability.ads_agent_tool_daily_metrics
(
    date Date,
    agent_id String,
    tool_name String,
    tool_type String,
    tool_call_count UInt64,
    success_count UInt64,
    error_count UInt64,
    retry_count UInt64,
    avg_duration_ms Float64,
    p95_duration_ms UInt64,
    avg_result_size Float64,
    max_result_size UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, agent_id, tool_name, tool_type);
