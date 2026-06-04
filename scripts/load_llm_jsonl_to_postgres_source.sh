#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/load_llm_jsonl_to_postgres_source.sh data/raw/mock_llm_requests/events.jsonl" >&2
  exit 2
fi

input_path="$1"
copy_columns="request_id,trace_id,run_id,span_id,agent_id,agent_name,channel,user_id,session_id,conversation_id,app_name,feature_name,prompt_category,prompt_id,prompt_version,model_name,provider,prompt_text,response_text,prompt_hash,response_hash,input_chars,output_chars,prompt_tokens,completion_tokens,total_tokens,request_type,is_streaming,temperature,max_tokens,finish_reason,retry_count,latency_ms,status,error_type,http_status,estimated_cost_usd,mode,region,environment,created_at,date"

uv run python -m scripts.export_llm_jsonl_to_postgres_copy --input "${input_path}" \
  | docker compose exec -T postgres \
      psql -U ai_observability -d ai_observability \
      -c "\copy llm_request_events (${copy_columns}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N')"
