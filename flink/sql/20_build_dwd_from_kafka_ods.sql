-- Build DWD facts from Kafka ODS.
-- Prompt and response text are intentionally not selected into DWD here to keep
-- analytical facts smaller; hashes remain available for deduplication.

INSERT INTO paimon_lake.dwd.dwd_ai_llm_request_di
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
FROM ods_ai_observability_llm_request_events_di
WHERE request_id IS NOT NULL
  AND created_at IS NOT NULL
  AND prompt_tokens >= 0
  AND completion_tokens >= 0
  AND total_tokens = prompt_tokens + completion_tokens
  AND latency_ms > 0
  AND status IN ('success', 'error')
  AND estimated_cost_usd >= 0
  AND mode IN ('mock', 'live', 'replay', 'hermes');

INSERT INTO paimon_lake.dwd.dwd_ai_retrieval_request_di
SELECT
    retrieval_id,
    COALESCE(trace_id, '') AS trace_id,
    COALESCE(run_id, '') AS run_id,
    COALESCE(span_id, '') AS span_id,
    COALESCE(request_id, '') AS request_id,
    COALESCE(agent_id, '') AS agent_id,
    app_name,
    feature_name,
    user_id,
    knowledge_base_id,
    COALESCE(knowledge_base_name, '') AS knowledge_base_name,
    embedding_model,
    retrieval_strategy,
    COALESCE(query_text_hash, '') AS query_text_hash,
    COALESCE(query_length, 0) AS query_length,
    top_k,
    returned_count,
    hit_count,
    max_similarity_score,
    min_similarity_score,
    avg_similarity_score,
    embedding_latency_ms,
    search_latency_ms,
    total_latency_ms,
    status,
    error_type,
    mode,
    environment,
    created_at,
    `date`
FROM ods_ai_observability_retrieval_events_di
WHERE retrieval_id IS NOT NULL
  AND created_at IS NOT NULL
  AND top_k > 0
  AND returned_count >= 0
  AND hit_count >= 0
  AND hit_count <= returned_count
  AND total_latency_ms > 0
  AND status IN ('success', 'error')
  AND mode IN ('mock', 'live', 'replay');

INSERT INTO paimon_lake.dwd.dwd_ai_feedback_action_di
SELECT
    feedback_id,
    COALESCE(trace_id, '') AS trace_id,
    COALESCE(request_id, '') AS request_id,
    COALESCE(run_id, '') AS run_id,
    session_id,
    COALESCE(conversation_id, '') AS conversation_id,
    user_id,
    app_name,
    feature_name,
    COALESCE(agent_id, '') AS agent_id,
    feedback_type,
    rating_value,
    COALESCE(feedback_text_hash, '') AS feedback_text_hash,
    COALESCE(feedback_text_length, 0) AS feedback_text_length,
    response_latency_ms,
    model_name,
    prompt_version,
    mode,
    environment,
    created_at,
    `date`
FROM ods_ai_observability_feedback_events_di
WHERE feedback_id IS NOT NULL
  AND created_at IS NOT NULL
  AND feedback_type IN ('thumbs_up', 'thumbs_down', 'rating', 'regenerate', 'edit', 'report')
  AND (rating_value IS NULL OR rating_value BETWEEN 1 AND 5)
  AND response_latency_ms > 0
  AND mode IN ('mock', 'live');

INSERT INTO paimon_lake.dwd.dwd_ai_guardrail_check_di
SELECT
    guardrail_event_id,
    COALESCE(trace_id, '') AS trace_id,
    COALESCE(request_id, '') AS request_id,
    COALESCE(run_id, '') AS run_id,
    user_id,
    app_name,
    feature_name,
    guardrail_stage,
    rule_name,
    rule_category,
    triggered,
    action_taken,
    severity,
    COALESCE(matched_pattern_hash, '') AS matched_pattern_hash,
    COALESCE(input_text_length, 0) AS input_text_length,
    guardrail_latency_ms,
    model_name,
    prompt_version,
    mode,
    environment,
    created_at,
    `date`
FROM ods_ai_observability_guardrail_events_di
WHERE guardrail_event_id IS NOT NULL
  AND created_at IS NOT NULL
  AND guardrail_stage IN ('pre_request', 'post_response')
  AND rule_category IN ('content_filter', 'pii_detection', 'toxicity', 'topic_block', 'length_limit')
  AND action_taken IN ('pass', 'warn', 'block', 'redact', 'override')
  AND severity IN ('low', 'medium', 'high', 'critical')
  AND guardrail_latency_ms > 0
  AND mode IN ('mock', 'live');

INSERT INTO paimon_lake.dwd.dwd_ai_evaluation_judgment_di
SELECT
    evaluation_id,
    COALESCE(trace_id, '') AS trace_id,
    COALESCE(request_id, '') AS request_id,
    COALESCE(run_id, '') AS run_id,
    app_name,
    feature_name,
    evaluator_type,
    COALESCE(evaluator_model, '') AS evaluator_model,
    evaluation_dimension,
    score,
    COALESCE(raw_score, '') AS raw_score,
    pass_threshold,
    passed,
    evaluated_model_name,
    evaluated_prompt_version,
    evaluation_latency_ms,
    mode,
    environment,
    created_at,
    `date`
FROM ods_ai_observability_evaluation_events_di
WHERE evaluation_id IS NOT NULL
  AND created_at IS NOT NULL
  AND evaluator_type IN ('llm_judge', 'human', 'ground_truth', 'regex', 'classifier')
  AND evaluation_dimension IN ('relevance', 'faithfulness', 'coherence', 'toxicity', 'hallucination')
  AND score BETWEEN 0.0 AND 1.0
  AND pass_threshold BETWEEN 0.0 AND 1.0
  AND evaluation_latency_ms > 0
  AND mode IN ('mock', 'live', 'offline');
