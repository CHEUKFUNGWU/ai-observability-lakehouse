#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/flink_cancel_job.sh <flink-job-id>" >&2
  exit 2
fi

job_id="$1"

docker compose exec -T flink-jobmanager \
  /opt/flink/bin/flink cancel "${job_id}"
