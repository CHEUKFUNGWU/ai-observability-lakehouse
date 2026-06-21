-- Build hourly feature metrics from DWD using event-time windows.

INSERT INTO paimon_lake.dws.dws_ai_llm_feature_request_1h
SELECT
    CAST(window_start AS DATE) AS `date`,
    CAST(EXTRACT(HOUR FROM window_start) AS INT) AS `hour`,
    app_name,
    feature_name,
    model_name,
    COUNT(*) AS request_cnt_1h,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_cnt_1h,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_cnt_1h,
    SUM(prompt_tokens) AS prompt_token_cnt_1h,
    SUM(completion_tokens) AS completion_token_cnt_1h,
    SUM(total_tokens) AS total_token_cnt_1h,
    SUM(estimated_cost_usd) AS estimated_cost_amt_1h,
    AVG(latency_ms) AS avg_latency_ms,
    CAST(MAX(latency_ms) AS BIGINT) AS max_latency_ms,
    CAST(0 AS BIGINT) AS p95_latency_ms
FROM TABLE(
    TUMBLE(
        TABLE paimon_lake.dwd.dwd_ai_llm_request_di,
        DESCRIPTOR(created_at),
        INTERVAL '1' HOUR
    )
)
GROUP BY window_start, app_name, feature_name, model_name;

-- Daily feature metrics roll up the hourly table so both grains share one definition.

INSERT INTO paimon_lake.dws.dws_ai_llm_feature_request_1d
SELECT
    `date`,
    app_name,
    feature_name,
    model_name,
    SUM(request_cnt_1h) AS request_count,
    SUM(success_cnt_1h) AS success_count,
    SUM(error_cnt_1h) AS error_count,
    SUM(prompt_token_cnt_1h) AS prompt_tokens,
    SUM(completion_token_cnt_1h) AS completion_tokens,
    SUM(total_token_cnt_1h) AS total_tokens,
    SUM(estimated_cost_amt_1h) AS estimated_cost_usd,
    SUM(avg_latency_ms * request_cnt_1h) / SUM(request_cnt_1h) AS avg_latency_ms,
    MAX(max_latency_ms) AS max_latency_ms,
    CAST(0 AS BIGINT) AS p95_latency_ms
FROM paimon_lake.dws.dws_ai_llm_feature_request_1h
GROUP BY `date`, app_name, feature_name, model_name;

INSERT INTO paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d
SELECT
    `date`,
    app_name,
    knowledge_base_id,
    embedding_model,
    retrieval_strategy,
    COUNT(*) AS retrieval_cnt_1d,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_cnt_1d,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_cnt_1d,
    SUM(CASE WHEN returned_count = 0 THEN 1 ELSE 0 END) AS zero_result_cnt_1d,
    SUM(returned_count) AS returned_cnt_1d,
    SUM(hit_count) AS hit_cnt_1d,
    AVG(avg_similarity_score) AS avg_similarity_score,
    AVG(total_latency_ms) AS avg_total_latency_ms,
    CAST(MAX(total_latency_ms) AS BIGINT) AS p95_total_latency_ms,
    AVG(embedding_latency_ms) AS avg_embedding_latency_ms,
    AVG(search_latency_ms) AS avg_search_latency_ms
FROM paimon_lake.dwd.dwd_ai_retrieval_request_di
GROUP BY `date`, app_name, knowledge_base_id, embedding_model, retrieval_strategy;

INSERT INTO paimon_lake.dws.dws_ai_feedback_feature_action_1d
SELECT
    `date`,
    app_name,
    feature_name,
    agent_id,
    COUNT(*) AS feedback_cnt_1d,
    SUM(CASE WHEN feedback_type = 'thumbs_up' THEN 1 ELSE 0 END) AS thumbs_up_cnt_1d,
    SUM(CASE WHEN feedback_type = 'thumbs_down' THEN 1 ELSE 0 END) AS thumbs_down_cnt_1d,
    SUM(CASE WHEN feedback_type = 'regenerate' THEN 1 ELSE 0 END) AS regenerate_cnt_1d,
    SUM(CASE WHEN feedback_type = 'report' THEN 1 ELSE 0 END) AS report_cnt_1d,
    AVG(rating_value) AS avg_rating,
    COUNT(DISTINCT request_id) AS rated_request_cnt_1d
FROM paimon_lake.dwd.dwd_ai_feedback_action_di
GROUP BY `date`, app_name, feature_name, agent_id;

INSERT INTO paimon_lake.dws.dws_ai_guardrail_rule_check_1d
SELECT
    `date`,
    app_name,
    rule_category,
    action_taken,
    COUNT(*) AS check_cnt_1d,
    SUM(CASE WHEN triggered THEN 1 ELSE 0 END) AS triggered_cnt_1d,
    SUM(CASE WHEN action_taken = 'block' THEN 1 ELSE 0 END) AS block_cnt_1d,
    SUM(CASE WHEN action_taken = 'redact' THEN 1 ELSE 0 END) AS redact_cnt_1d,
    SUM(CASE WHEN action_taken = 'warn' THEN 1 ELSE 0 END) AS warn_cnt_1d,
    AVG(guardrail_latency_ms) AS avg_guardrail_latency_ms,
    COUNT(DISTINCT user_id) AS distinct_user_cnt_1d
FROM paimon_lake.dwd.dwd_ai_guardrail_check_di
GROUP BY `date`, app_name, rule_category, action_taken;

INSERT INTO paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d
SELECT
    `date`,
    app_name,
    feature_name,
    evaluation_dimension,
    evaluated_model_name,
    COUNT(*) AS evaluation_cnt_1d,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS pass_cnt_1d,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) AS fail_cnt_1d,
    AVG(score) AS avg_score,
    MIN(score) AS p10_score,
    AVG(evaluation_latency_ms) AS avg_evaluation_latency_ms
FROM paimon_lake.dwd.dwd_ai_evaluation_judgment_di
GROUP BY `date`, app_name, feature_name, evaluation_dimension, evaluated_model_name;

INSERT INTO paimon_lake.dws.dws_ai_llm_feature_env_request_1d
SELECT
    `date`,
    app_name,
    feature_name,
    model_name,
    environment,
    COUNT(*) AS request_cnt_1d,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_cnt_1d,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_cnt_1d,
    SUM(prompt_tokens) AS prompt_token_cnt_1d,
    SUM(completion_tokens) AS completion_token_cnt_1d,
    SUM(total_tokens) AS total_token_cnt_1d,
    SUM(estimated_cost_usd) AS estimated_cost_amt_1d,
    AVG(latency_ms) AS avg_latency_ms,
    CAST(MAX(latency_ms) AS BIGINT) AS max_latency_ms,
    CAST(MAX(latency_ms) AS BIGINT) AS p95_latency_ms
FROM paimon_lake.dwd.dwd_ai_llm_request_di
GROUP BY `date`, app_name, feature_name, model_name, environment;

INSERT INTO paimon_lake.dws.dws_ai_llm_region_request_1d
SELECT
    `date`,
    region,
    environment,
    app_name,
    model_name,
    COUNT(*) AS request_cnt_1d,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_cnt_1d,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_cnt_1d,
    SUM(prompt_tokens) AS prompt_token_cnt_1d,
    SUM(completion_tokens) AS completion_token_cnt_1d,
    SUM(total_tokens) AS total_token_cnt_1d,
    SUM(estimated_cost_usd) AS estimated_cost_amt_1d,
    AVG(latency_ms) AS avg_latency_ms,
    CAST(MAX(latency_ms) AS BIGINT) AS max_latency_ms,
    CAST(MAX(latency_ms) AS BIGINT) AS p95_latency_ms
FROM paimon_lake.dwd.dwd_ai_llm_request_di
GROUP BY `date`, region, environment, app_name, model_name;

INSERT INTO paimon_lake.dws.dws_ai_agent_orchestration_handoff_1d
SELECT
    `date`,
    parent_agent_id,
    child_agent_id,
    handoff_type,
    COUNT(*) AS handoff_cnt_1d,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_cnt_1d,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_cnt_1d,
    SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) AS timeout_cnt_1d,
    AVG(CAST(handoff_latency_ms AS DOUBLE)) AS avg_handoff_latency_ms,
    CAST(MAX(handoff_latency_ms) AS BIGINT) AS p95_handoff_latency_ms
FROM paimon_lake.dwd.dwd_ai_agent_orchestration_di
GROUP BY `date`, parent_agent_id, child_agent_id, handoff_type;

INSERT INTO paimon_lake.dws.dws_ai_platform_component_health_1d
SELECT
    `date`,
    component,
    metric_name,
    MAX(metric_value) AS metric_value,
    MAX(threshold) AS threshold,
    MAX(metric_value) > MAX(threshold) AS is_breach
FROM ods_ai_observability_platform_health_metrics_di
WHERE component IN ('kafka', 'flink', 'paimon', 'doris')
  AND metric_value >= 0
  AND threshold >= 0
GROUP BY `date`, component, metric_name;

INSERT INTO paimon_lake.dws.dws_ai_llm_session_request_1d
WITH session_metrics AS (
    SELECT
        `date`,
        app_name,
        feature_name,
        session_id,
        COUNT(*) AS turn_cnt,
        SUM(total_tokens) AS token_cnt,
        TIMESTAMPDIFF(SECOND, MIN(created_at), MAX(created_at)) * 1000
            + MAX(latency_ms) AS session_duration_ms
    FROM paimon_lake.dwd.dwd_ai_llm_request_di
    GROUP BY `date`, app_name, feature_name, session_id
),
resolved_sessions AS (
    SELECT
        `date`,
        app_name,
        feature_name,
        session_id,
        MAX(
            CASE
                WHEN feedback_type = 'thumbs_up' OR (feedback_type = 'rating' AND rating_value >= 4) THEN 1
                ELSE 0
            END
        ) AS is_resolved
    FROM paimon_lake.dwd.dwd_ai_feedback_action_di
    GROUP BY `date`, app_name, feature_name, session_id
)
SELECT
    sessions.`date`,
    sessions.app_name,
    sessions.feature_name,
    COUNT(*) AS session_cnt_1d,
    AVG(sessions.turn_cnt) AS avg_turns_per_session,
    AVG(sessions.token_cnt) AS avg_tokens_per_session,
    AVG(sessions.session_duration_ms) AS avg_duration_per_session_ms,
    SUM(COALESCE(feedback.is_resolved, 0)) AS resolved_session_cnt_1d
FROM session_metrics AS sessions
LEFT JOIN resolved_sessions AS feedback
    ON sessions.`date` = feedback.`date`
    AND sessions.app_name = feedback.app_name
    AND sessions.feature_name = feedback.feature_name
    AND sessions.session_id = feedback.session_id
GROUP BY sessions.`date`, sessions.app_name, sessions.feature_name;
