#!/usr/bin/env bash
set -euo pipefail

GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"
METALAKE="${GRAVITINO_METALAKE:-ai_observability}"
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "${RESPONSE_FILE}"' EXIT

headers=(
  -H "Accept: application/vnd.gravitino.v1+json"
  -H "Content-Type: application/json"
)
curl_timeout=(--connect-timeout 3 --max-time 10)

get_status() {
  curl -sS "${curl_timeout[@]}" -o "${RESPONSE_FILE}" -w '%{http_code}' \
    "${headers[@]}" "$1"
}

create_resource() {
  local label="$1"
  local url="$2"
  local payload="$3"
  local status

  status="$(
    curl -sS "${curl_timeout[@]}" -o "${RESPONSE_FILE}" -w '%{http_code}' \
      -X POST "${headers[@]}" -d "${payload}" "${url}"
  )"
  if [[ "${status}" != "200" && "${status}" != "201" ]]; then
    echo "ERROR: failed to create ${label} (HTTP ${status})" >&2
    cat "${RESPONSE_FILE}" >&2
    return 1
  fi
  echo "Created ${label}."
}

ready=false
for attempt in $(seq 1 60); do
  if curl -fsS "${curl_timeout[@]}" "${headers[@]}" \
    "${GRAVITINO_URL}/api/version" > /dev/null 2>&1; then
    ready=true
    break
  fi
  echo "Waiting for Gravitino server... (${attempt}/60)"
  sleep 2
done

if [[ "${ready}" != "true" ]]; then
  echo "ERROR: Gravitino server did not become ready within 120 seconds" >&2
  exit 1
fi

metalake_status="$(get_status "${GRAVITINO_URL}/api/metalakes/${METALAKE}")"
case "${metalake_status}" in
  200)
    echo "Metalake ${METALAKE} already exists."
    ;;
  404)
    create_resource "metalake" "${GRAVITINO_URL}/api/metalakes" "$(cat <<JSON
{
  "name": "${METALAKE}",
  "comment": "AI observability lakehouse metadata"
}
JSON
)"
    ;;
  *)
    echo "ERROR: failed to inspect metalake ${METALAKE} (HTTP ${metalake_status})" >&2
    cat "${RESPONSE_FILE}" >&2
    exit 1
    ;;
esac

catalog_status="$(get_status "${GRAVITINO_URL}/api/metalakes/${METALAKE}/catalogs/paimon_lake")"
case "${catalog_status}" in
  200)
    echo "Paimon catalog paimon_lake already exists."
    ;;
  404)
    create_resource "Paimon catalog" "${GRAVITINO_URL}/api/metalakes/${METALAKE}/catalogs" '{
      "name": "paimon_lake",
      "type": "RELATIONAL",
      "provider": "lakehouse-paimon",
      "comment": "Paimon ODS, DWD, DWS, DIM, and ADS metadata",
      "properties": {
        "catalog-backend": "filesystem",
        "warehouse": "file:///workspace/data/paimon"
      }
    }'
    ;;
  *)
    echo "ERROR: failed to inspect Paimon catalog (HTTP ${catalog_status})" >&2
    cat "${RESPONSE_FILE}" >&2
    exit 1
    ;;
esac

echo "Gravitino initialization complete: ${GRAVITINO_URL}"
