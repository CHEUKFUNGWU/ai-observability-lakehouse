SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT COUNT(*) AS dwd_row_count
FROM paimon_lake.dwd.llm_request_events;
