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

-- 4. Denied access attempts by classification and action.
SELECT
    `date`,
    data_classification,
    action_type,
    denial_reason,
    COUNT(*) AS denied_access_cnt_1d,
    COUNT(DISTINCT user_id) AS denied_user_cnt_1d
FROM ai_observability.dwd_ai_compliance_access_audit_di
WHERE access_granted = FALSE
GROUP BY `date`, data_classification, action_type, denial_reason
ORDER BY `date` DESC, denied_access_cnt_1d DESC;

-- 5. Retention policy enforcement evidence.
SELECT
    `date`,
    policy_name,
    table_name,
    action_type,
    COUNT(*) AS retention_action_cnt_1d,
    SUM(rows_affected) AS rows_affected_cnt_1d
FROM ai_observability.dwd_ai_compliance_data_retention_di
GROUP BY `date`, policy_name, table_name, action_type
ORDER BY `date` DESC, rows_affected_cnt_1d DESC;

-- 6. Inter-agent handoff bottlenecks.
SELECT
    `date`,
    parent_agent_id,
    child_agent_id,
    handoff_type,
    handoff_cnt_1d,
    error_cnt_1d,
    timeout_cnt_1d,
    avg_handoff_latency_ms,
    p95_handoff_latency_ms
FROM ai_observability.dws_ai_agent_orchestration_handoff_1d
ORDER BY `date` DESC, p95_handoff_latency_ms DESC, timeout_cnt_1d DESC;

-- 7. Platform health threshold breaches.
SELECT
    `date`,
    component,
    metric_name,
    metric_value,
    threshold,
    ROUND(metric_value / NULLIF(threshold, 0), 4) AS threshold_utilization
FROM ai_observability.dws_ai_platform_component_health_1d
WHERE is_breach = TRUE
ORDER BY `date` DESC, threshold_utilization DESC;

-- 8. Reliability by feature.
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

-- 9. Latency by feature.
SELECT
    feature_name,
    ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS weighted_avg_latency_ms
FROM ai_observability.dws_ai_llm_feature_request_1d
GROUP BY feature_name
ORDER BY weighted_avg_latency_ms DESC;

-- 10. Cost and usage by model.
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

-- 11. App and feature leaderboard.
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

-- 12. Cost by model with pricing metadata.
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
