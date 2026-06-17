#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: scripts/run_flink_sql_from_savepoint.sh <savepoint-path> flink/sql/<file1>.sql [flink/sql/<file2>.sql ...]" >&2
  exit 2
fi

savepoint_path="$1"
shift

tmp_file="flink/sql/.generated_restore_sequence.sql"
trap 'rm -f "${tmp_file}"' EXIT
rm -f "${tmp_file}"

{
  printf "SET 'execution.savepoint.path' = '%s';\n\n" "${savepoint_path}"
} >> "${tmp_file}"

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
