-- ADS stores dashboard-ready daily metrics.

CREATE TABLE IF NOT EXISTS paimon_lake.ads.llm_feature_daily_metrics (
    event_date DATE,
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
    p95_latency_ms DOUBLE,
    success_rate DOUBLE,
    error_rate DOUBLE,
    PRIMARY KEY (event_date, app_name, feature_name, model_name) NOT ENFORCED
) PARTITIONED BY (event_date) WITH (
    'bucket' = '4'
);
