-- Build daily feature metrics from DWD.

INSERT INTO paimon_lake.ads.llm_feature_daily_metrics
SELECT
    `date`,
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
    -- Store an explicit upper-bound metric instead of naming MAX latency as p95.
    CAST(MAX(latency_ms) AS DOUBLE) AS max_latency_ms
FROM paimon_lake.dwd.llm_request_events
GROUP BY `date`, app_name, feature_name, model_name;
