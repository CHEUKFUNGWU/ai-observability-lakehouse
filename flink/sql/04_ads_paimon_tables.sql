-- ADS stores dashboard-ready daily metrics.

CREATE TABLE IF NOT EXISTS paimon_lake.ads.llm_feature_daily_metrics (
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
    max_latency_ms DOUBLE,
    PRIMARY KEY (`date`, app_name, feature_name, model_name) NOT ENFORCED
) PARTITIONED BY (`date`) WITH (
    'bucket' = '4'
);
