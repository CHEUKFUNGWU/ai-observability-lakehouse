SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT
    COUNT(*) AS ads_metric_rows,
    SUM(request_count) AS total_request_count
FROM paimon_lake.ads.llm_feature_daily_metrics;
