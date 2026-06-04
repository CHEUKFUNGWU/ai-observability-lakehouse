#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T flink-jobmanager \
  sh -lc '
    mkdir -p /workspace/data/paimon/_checkpoints /workspace/data/paimon/_savepoints
    if id flink >/dev/null 2>&1; then
      chown -R flink:flink /workspace/data/paimon
    fi
    chmod -R u+rwX,g+rwX /workspace/data/paimon
  '
