CREATE DATABASE IF NOT EXISTS ai_observability;

DROP TABLE IF EXISTS ai_observability.dwd_llm_request_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_run_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_span_events;
DROP TABLE IF EXISTS ai_observability.dwd_agent_tool_call_events;
DROP TABLE IF EXISTS ai_observability.dws_llm_feature_daily_metrics;
DROP TABLE IF EXISTS ai_observability.dws_agent_daily_metrics;
DROP TABLE IF EXISTS ai_observability.dws_agent_tool_daily_metrics;
DROP TABLE IF EXISTS ai_observability.dim_model;
DROP TABLE IF EXISTS ai_observability.ads_cost_anomaly_daily;
DROP TABLE IF EXISTS ai_observability.ads_sla_daily_report;
DROP TABLE IF EXISTS ai_observability.ads_prompt_version_daily_metrics;
DROP MATERIALIZED VIEW IF EXISTS ai_observability.mv_daily_summary;

CREATE TABLE IF NOT EXISTS ai_observability.dwd_llm_request_events
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

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_run_events
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

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_span_events
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

CREATE TABLE IF NOT EXISTS ai_observability.dwd_agent_tool_call_events
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

CREATE TABLE IF NOT EXISTS ai_observability.dws_llm_feature_daily_metrics
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

CREATE TABLE IF NOT EXISTS ai_observability.dim_model
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

CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_daily_metrics
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
DUPLICATE KEY(`date`, app_name, agent_id, task_type)
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

CREATE TABLE IF NOT EXISTS ai_observability.dws_agent_tool_daily_metrics
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

CREATE TABLE IF NOT EXISTS ai_observability.ads_cost_anomaly_daily
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

CREATE TABLE IF NOT EXISTS ai_observability.ads_sla_daily_report
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

CREATE TABLE IF NOT EXISTS ai_observability.ads_prompt_version_daily_metrics
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
FROM ai_observability.dws_llm_feature_daily_metrics
GROUP BY `date`;
