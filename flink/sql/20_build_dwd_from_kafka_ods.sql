-- Build DWD facts from Kafka ODS.
-- Prompt and response text are intentionally not selected into DWD here to keep
-- analytical facts smaller; hashes remain available for deduplication.

INSERT INTO paimon_lake.dwd.llm_request_events
SELECT
    request_id,
    COALESCE(trace_id, '') AS trace_id,
    COALESCE(run_id, '') AS run_id,
    COALESCE(span_id, '') AS span_id,
    COALESCE(agent_id, '') AS agent_id,
    COALESCE(agent_name, '') AS agent_name,
    COALESCE(channel, '') AS channel,
    user_id,
    session_id,
    COALESCE(conversation_id, '') AS conversation_id,
    app_name,
    feature_name,
    prompt_category,
    prompt_id,
    prompt_version,
    model_name,
    provider,
    COALESCE(prompt_hash, '') AS prompt_hash,
    COALESCE(response_hash, '') AS response_hash,
    COALESCE(input_chars, 0) AS input_chars,
    COALESCE(output_chars, 0) AS output_chars,
    prompt_tokens,
    completion_tokens,
    total_tokens,
    COALESCE(request_type, 'chat') AS request_type,
    COALESCE(is_streaming, FALSE) AS is_streaming,
    COALESCE(temperature, 0.0) AS temperature,
    COALESCE(max_tokens, 0) AS max_tokens,
    COALESCE(finish_reason, '') AS finish_reason,
    COALESCE(retry_count, 0) AS retry_count,
    latency_ms,
    status,
    error_type,
    http_status,
    estimated_cost_usd,
    mode,
    region,
    environment,
    created_at,
    `date`
FROM kafka_ods_llm_request_events
WHERE request_id IS NOT NULL
  AND created_at IS NOT NULL
  AND prompt_tokens >= 0
  AND completion_tokens >= 0
  AND total_tokens = prompt_tokens + completion_tokens
  AND latency_ms > 0
  AND status IN ('success', 'error')
  AND estimated_cost_usd >= 0
  AND mode IN ('mock', 'live', 'replay', 'hermes');
