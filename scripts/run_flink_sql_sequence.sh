#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/run_flink_sql_sequence.sh flink/sql/<file1>.sql [flink/sql/<file2>.sql ...]" >&2
  exit 2
fi

tmp_file="flink/sql/.generated_sequence.sql"
trap 'rm -f "${tmp_file}"' EXIT
rm -f "${tmp_file}"

for sql_file in "$@"; do
  {
    echo "-- ${sql_file}"
    cat "${sql_file}"
    echo
  } >> "${tmp_file}"
done

scripts/prepare_flink_warehouse.sh

docker compose run -T --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh \
  -f "/workspace/${tmp_file}"
