#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: scripts/run_flink_sql_file.sh flink/sql/<file>.sql" >&2
  exit 2
fi

sql_file="$1"

scripts/prepare_flink_warehouse.sh

docker compose run -T --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh \
  -f "/workspace/${sql_file}"
