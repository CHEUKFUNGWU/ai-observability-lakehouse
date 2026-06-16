SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT
    COUNT(*) AS dws_metric_rows,
    SUM(request_count) AS total_request_count
FROM paimon_lake.dws.dws_ai_llm_feature_request_1d;

SELECT
    COUNT(*) AS dws_retrieval_metric_rows,
    SUM(retrieval_cnt_1d) AS total_retrieval_cnt_1d
FROM paimon_lake.dws.dws_ai_retrieval_knowledge_base_request_1d;

SELECT
    COUNT(*) AS dws_feedback_metric_rows,
    SUM(feedback_cnt_1d) AS total_feedback_cnt_1d
FROM paimon_lake.dws.dws_ai_feedback_feature_action_1d;

SELECT
    COUNT(*) AS dws_guardrail_metric_rows,
    SUM(check_cnt_1d) AS total_check_cnt_1d
FROM paimon_lake.dws.dws_ai_guardrail_rule_check_1d;

SELECT
    COUNT(*) AS dws_cost_team_metric_rows,
    SUM(request_cnt_1d) AS total_team_request_cnt_1d
FROM paimon_lake.dws.dws_ai_cost_team_request_1d;

SELECT
    COUNT(*) AS dws_evaluation_metric_rows,
    SUM(evaluation_cnt_1d) AS total_evaluation_cnt_1d
FROM paimon_lake.dws.dws_ai_evaluation_feature_judgment_1d;

SELECT
    COUNT(*) AS dws_prompt_version_metric_rows,
    SUM(request_cnt_1d) AS total_prompt_version_request_cnt_1d
FROM paimon_lake.dws.dws_ai_prompt_version_request_1d;

SELECT
    COUNT(*) AS dws_llm_feature_env_metric_rows,
    SUM(request_cnt_1d) AS total_feature_env_request_cnt_1d
FROM paimon_lake.dws.dws_ai_llm_feature_env_request_1d;
