SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT COUNT(*) AS ods_row_count
FROM paimon_lake.ods.llm_request_events;
