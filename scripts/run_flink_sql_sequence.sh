#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/run_flink_sql_sequence.sh flink/sql/<file1>.sql [flink/sql/<file2>.sql ...]" >&2
  exit 2
fi

tmp_file="flink/sql/.generated_sequence.sql"
output_file="$(mktemp)"
trap 'rm -f "${tmp_file}" "${output_file}"' EXIT
rm -f "${tmp_file}"

for sql_file in "$@"; do
  {
    echo "-- ${sql_file}"
    cat "${sql_file}"
    echo
  } >> "${tmp_file}"
done

scripts/prepare_flink_warehouse.sh

set +e
docker compose run -T --rm flink-sql-client \
  /opt/flink/bin/sql-client.sh \
  -f "/workspace/${tmp_file}" 2>&1 | tee "${output_file}"
docker_status=${PIPESTATUS[0]}
set -e

if [[ ${docker_status} -ne 0 ]]; then
  exit "${docker_status}"
fi

# Flink SQL Client can print a statement-level error and still exit with status 0.
# Treat any such error as a failed sequence so Make/CI cannot report false success.
if grep -q '\[ERROR\]' "${output_file}"; then
  echo "ERROR: Flink SQL Client reported one or more failed statements." >&2
  exit 1
fi
