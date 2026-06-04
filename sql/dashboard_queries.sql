-- Dashboard queries for ai_observability.ads_llm_feature_daily_metrics.
-- These queries are designed for ClickHouse and the ADS feature daily table.

-- 1. Daily traffic, reliability, token usage and cost.
SELECT
    date,
    request_count,
    success_count,
    error_count,
    round(success_count / request_count, 4) AS success_rate,
    round(error_count / request_count, 4) AS error_rate,
    total_tokens,
    estimated_cost_usd
FROM
(
    SELECT
        date,
        sum(request_count) AS request_count,
        sum(success_count) AS success_count,
        sum(error_count) AS error_count,
        sum(total_tokens) AS total_tokens,
        round(sum(estimated_cost_usd), 8) AS estimated_cost_usd
    FROM ai_observability.ads_llm_feature_daily_metrics
    GROUP BY date
)
ORDER BY date;

-- 2. Request volume by feature.
SELECT
    feature_name,
    sum(request_count) AS request_count
FROM ai_observability.ads_llm_feature_daily_metrics
GROUP BY feature_name
ORDER BY request_count DESC;

-- 3. Cost by feature.
SELECT
    feature_name,
    round(sum(estimated_cost_usd), 8) AS estimated_cost_usd
FROM ai_observability.ads_llm_feature_daily_metrics
GROUP BY feature_name
ORDER BY estimated_cost_usd DESC;

-- 4. Reliability by feature.
SELECT
    feature_name,
    request_count,
    success_count,
    error_count,
    round(success_count / request_count, 4) AS success_rate,
    round(error_count / request_count, 4) AS error_rate
FROM
(
    SELECT
        feature_name,
        sum(request_count) AS request_count,
        sum(success_count) AS success_count,
        sum(error_count) AS error_count
    FROM ai_observability.ads_llm_feature_daily_metrics
    GROUP BY feature_name
)
ORDER BY error_rate DESC, request_count DESC;

-- 5. Latency by feature.
SELECT
    feature_name,
    round(sum(avg_latency_ms * request_count) / sum(request_count), 2) AS weighted_avg_latency_ms
FROM ai_observability.ads_llm_feature_daily_metrics
GROUP BY feature_name
ORDER BY weighted_avg_latency_ms DESC;

-- 6. Cost and usage by model.
SELECT
    model_name,
    request_count,
    total_tokens,
    estimated_cost_usd,
    round(estimated_cost_usd / request_count, 8) AS avg_cost_per_request
FROM
(
    SELECT
        model_name,
        sum(request_count) AS request_count,
        sum(total_tokens) AS total_tokens,
        round(sum(estimated_cost_usd), 8) AS estimated_cost_usd
    FROM ai_observability.ads_llm_feature_daily_metrics
    GROUP BY model_name
)
ORDER BY estimated_cost_usd DESC;

-- 7. App and feature leaderboard.
SELECT
    app_name,
    feature_name,
    request_count_sum AS request_count,
    estimated_cost_usd,
    weighted_avg_latency_ms,
    round(error_count_sum / request_count_sum, 4) AS error_rate
FROM
(
    SELECT
        app_name,
        feature_name,
        sum(request_count) AS request_count_sum,
        sum(error_count) AS error_count_sum,
        round(sum(estimated_cost_usd), 8) AS estimated_cost_usd,
        round(sum(avg_latency_ms * request_count) / sum(request_count), 2) AS weighted_avg_latency_ms
    FROM ai_observability.ads_llm_feature_daily_metrics
    GROUP BY
        app_name,
        feature_name
)
ORDER BY request_count_sum DESC;
