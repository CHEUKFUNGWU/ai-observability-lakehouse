CREATE DATABASE IF NOT EXISTS ai_observability;

DROP TABLE IF EXISTS ai_observability.ads_llm_feature_daily_metrics;

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
