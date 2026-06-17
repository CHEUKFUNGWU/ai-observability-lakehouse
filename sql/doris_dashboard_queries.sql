-- Dashboard queries for ai_observability.dws_ai_llm_feature_request_1d.
-- These queries are designed for Doris and the DWS feature daily table.

-- 1. Daily traffic, reliability, token usage and cost.
SELECT
    `date`,
    request_count,
    success_count,
    error_count,
    ROUND(success_count / NULLIF(request_count, 0), 4) AS success_rate,
    ROUND(error_count / NULLIF(request_count, 0), 4) AS error_rate,
    total_tokens,
    estimated_cost_usd
FROM
(
    SELECT
        `date`,
        SUM(request_count) AS request_count,
        SUM(success_count) AS success_count,
        SUM(error_count) AS error_count,
        SUM(total_tokens) AS total_tokens,
        ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd
    FROM ai_observability.dws_ai_llm_feature_request_1d
    GROUP BY `date`
) daily
ORDER BY `date`;

-- 2. Request volume by feature.
SELECT
    feature_name,
    SUM(request_count) AS request_count
FROM ai_observability.dws_ai_llm_feature_request_1d
GROUP BY feature_name
ORDER BY request_count DESC;

-- 3. Cost by feature.
SELECT
    feature_name,
    ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd
FROM ai_observability.dws_ai_llm_feature_request_1d
GROUP BY feature_name
ORDER BY estimated_cost_usd DESC;

-- 4. Reliability by feature.
SELECT
    feature_name,
    request_count,
    success_count,
    error_count,
    ROUND(success_count / NULLIF(request_count, 0), 4) AS success_rate,
    ROUND(error_count / NULLIF(request_count, 0), 4) AS error_rate
FROM
(
    SELECT
        feature_name,
        SUM(request_count) AS request_count,
        SUM(success_count) AS success_count,
        SUM(error_count) AS error_count
    FROM ai_observability.dws_ai_llm_feature_request_1d
    GROUP BY feature_name
) feature_rollup
ORDER BY error_rate DESC, request_count DESC;

-- 5. Latency by feature.
SELECT
    feature_name,
    ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS weighted_avg_latency_ms
FROM ai_observability.dws_ai_llm_feature_request_1d
GROUP BY feature_name
ORDER BY weighted_avg_latency_ms DESC;

-- 6. Cost and usage by model.
SELECT
    model_name,
    request_count,
    total_tokens,
    estimated_cost_usd,
    ROUND(estimated_cost_usd / NULLIF(request_count, 0), 8) AS avg_cost_per_request
FROM
(
    SELECT
        model_name,
        SUM(request_count) AS request_count,
        SUM(total_tokens) AS total_tokens,
        ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd
    FROM ai_observability.dws_ai_llm_feature_request_1d
    GROUP BY model_name
) model_rollup
ORDER BY estimated_cost_usd DESC;

-- 7. App and feature leaderboard.
SELECT
    app_name,
    feature_name,
    request_count_sum AS request_count,
    estimated_cost_usd,
    weighted_avg_latency_ms,
    ROUND(error_count_sum / NULLIF(request_count_sum, 0), 4) AS error_rate
FROM
(
    SELECT
        app_name,
        feature_name,
        SUM(request_count) AS request_count_sum,
        SUM(error_count) AS error_count_sum,
        ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd,
        ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS weighted_avg_latency_ms
    FROM ai_observability.dws_ai_llm_feature_request_1d
    GROUP BY app_name, feature_name
) leaderboard
ORDER BY request_count_sum DESC;

-- 8. Cost by model with pricing metadata.
SELECT
    m.model_name,
    m.provider,
    m.input_price_per_1m_tokens,
    m.output_price_per_1m_tokens,
    a.request_count,
    a.total_tokens,
    a.estimated_cost_usd
FROM
(
    SELECT
        model_name,
        SUM(request_count) AS request_count,
        SUM(total_tokens) AS total_tokens,
        SUM(estimated_cost_usd) AS estimated_cost_usd
    FROM ai_observability.dws_ai_llm_feature_request_1d
    GROUP BY model_name
) a
JOIN ai_observability.dim_model_df m ON a.model_name = m.model_name
ORDER BY estimated_cost_usd DESC;
