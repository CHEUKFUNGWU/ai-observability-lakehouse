#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --create \
    --topic ods_llm_request_events \
    --partitions 4 \
    --replication-factor 1 \
    --config retention.ms=172800000 \
    --bootstrap-server localhost:9092 \
    --if-not-exists
