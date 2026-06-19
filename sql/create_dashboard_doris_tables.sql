CREATE DATABASE IF NOT EXISTS ai_observability;

DROP TABLE IF EXISTS ai_observability.dws_ai_llm_feature_request_1d;
DROP TABLE IF EXISTS ai_observability.dim_model_df;
DROP TABLE IF EXISTS ai_observability.dwd_ai_compliance_access_audit_di;
DROP TABLE IF EXISTS ai_observability.dwd_ai_compliance_data_retention_di;
DROP TABLE IF EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d;
DROP TABLE IF EXISTS ai_observability.dws_ai_platform_component_health_1d;

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

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_access_audit_di
(
    `date` DATE NOT NULL,
    audit_event_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    action_type VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(256) NOT NULL,
    ip_address CHAR(64) NOT NULL,
    access_granted BOOLEAN NOT NULL,
    denial_reason VARCHAR(256) NULL,
    data_classification VARCHAR(32) NOT NULL,
    created_at DATETIME(3) NOT NULL
)
DUPLICATE KEY(`date`, audit_event_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(audit_event_id) BUCKETS 4
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

CREATE TABLE IF NOT EXISTS ai_observability.dwd_ai_compliance_data_retention_di
(
    `date` DATE NOT NULL,
    retention_event_id VARCHAR(128) NOT NULL,
    table_name VARCHAR(256) NOT NULL,
    partition_date DATE NOT NULL,
    action_type VARCHAR(32) NOT NULL,
    rows_affected BIGINT NOT NULL,
    policy_name VARCHAR(128) NOT NULL,
    created_at DATETIME(3) NOT NULL
)
DUPLICATE KEY(`date`, retention_event_id)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(retention_event_id) BUCKETS 4
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

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_agent_orchestration_handoff_1d
(
    `date` DATE NOT NULL,
    parent_agent_id VARCHAR(128) NOT NULL,
    child_agent_id VARCHAR(128) NOT NULL,
    handoff_type VARCHAR(32) NOT NULL,
    handoff_cnt_1d BIGINT NOT NULL,
    success_cnt_1d BIGINT NOT NULL,
    error_cnt_1d BIGINT NOT NULL,
    timeout_cnt_1d BIGINT NOT NULL,
    avg_handoff_latency_ms DOUBLE NOT NULL,
    p95_handoff_latency_ms BIGINT NOT NULL
)
DUPLICATE KEY(`date`, parent_agent_id, child_agent_id, handoff_type)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(parent_agent_id, child_agent_id) BUCKETS 4
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

CREATE TABLE IF NOT EXISTS ai_observability.dws_ai_platform_component_health_1d
(
    `date` DATE NOT NULL,
    component VARCHAR(32) NOT NULL,
    metric_name VARCHAR(128) NOT NULL,
    metric_value DOUBLE NOT NULL,
    threshold DOUBLE NOT NULL,
    is_breach BOOLEAN NOT NULL
)
DUPLICATE KEY(`date`, component, metric_name)
PARTITION BY RANGE(`date`) ()
DISTRIBUTED BY HASH(component, metric_name) BUCKETS 4
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
