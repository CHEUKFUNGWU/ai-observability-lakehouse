#!/usr/bin/env bash
set -euo pipefail

create_topic() {
  local topic="$1"
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh --create \
    --topic "${topic}" \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=172800000 \
    --bootstrap-server localhost:9092 \
    --if-not-exists
}

create_topic ods_ai_observability_llm_request_events_di
create_topic ods_ai_observability_retrieval_events_di
create_topic ods_ai_observability_feedback_events_di
create_topic ods_ai_observability_guardrail_events_di
create_topic ods_ai_observability_evaluation_events_di
create_topic ods_ai_observability_model_deployment_events_di
