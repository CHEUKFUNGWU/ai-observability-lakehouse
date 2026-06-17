SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT COUNT(*) AS dwd_row_count
FROM paimon_lake.dwd.dwd_ai_llm_request_di;

SELECT COUNT(*) AS dwd_retrieval_row_count
FROM paimon_lake.dwd.dwd_ai_retrieval_request_di;

SELECT COUNT(*) AS dwd_feedback_row_count
FROM paimon_lake.dwd.dwd_ai_feedback_action_di;

SELECT COUNT(*) AS dwd_guardrail_row_count
FROM paimon_lake.dwd.dwd_ai_guardrail_check_di;

SELECT COUNT(*) AS dwd_evaluation_row_count
FROM paimon_lake.dwd.dwd_ai_evaluation_judgment_di;

SELECT COUNT(*) AS dwd_model_deployment_row_count
FROM paimon_lake.dwd.dwd_ai_model_deployment_di;
