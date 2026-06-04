-- Build daily feature metrics from DWD.

INSERT INTO paimon_lake.ads.llm_feature_daily_metrics
SELECT
    event_date,
    app_name,
    feature_name,
    model_name,
    COUNT(*) AS request_count,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
    SUM(prompt_tokens) AS prompt_tokens,
    SUM(completion_tokens) AS completion_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(estimated_cost_usd) AS estimated_cost_usd,
    AVG(latency_ms) AS avg_latency_ms,
    -- Flink 1.20 SQL does not support PERCENTILE_CONT as a streaming aggregate.
    -- Keep the metric column as a conservative latency upper-bound for the local
    -- stream-batch MVP; Spark/ClickHouse can compute exact or approximate p95.
    CAST(MAX(latency_ms) AS DOUBLE) AS p95_latency_ms
FROM paimon_lake.dwd.llm_request_events
GROUP BY event_date, app_name, feature_name, model_name;
