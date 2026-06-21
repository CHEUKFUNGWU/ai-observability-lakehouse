#!/usr/bin/env bash
set -euo pipefail

GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"
METALAKE="ai_observability"

# Wait for Gravitino server to be ready
gravitino_ready=false
for i in $(seq 1 30); do
  if curl -sf "${GRAVITINO_URL}/api/version" > /dev/null 2>&1; then
    gravitino_ready=true
    break
  fi
  echo "Waiting for Gravitino server... (${i}/30)"
  sleep 2
done

if [ "$gravitino_ready" = false ]; then
  echo "ERROR: Gravitino server did not start within 60 seconds" >&2
  exit 1
fi

# Create metalake
curl -sf -X POST "${GRAVITINO_URL}/api/metalakes" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"${METALAKE}\",
    \"comment\": \"AI Observability Lakehouse metadata\"
  }" || echo "Metalake already exists"

# Register Paimon catalog
curl -sf -X POST "${GRAVITINO_URL}/api/metalakes/${METALAKE}/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "paimon_lake",
    "type": "RELATIONAL",
    "provider": "lakehouse-paimon",
    "comment": "Paimon stream-batch lakehouse tables (ODS/DWD/DWS/DIM/ADS)",
    "properties": {
      "catalog-backend": "filesystem",
      "warehouse": "file:///workspace/data/paimon"
    }
  }' || echo "Paimon catalog already exists"

# Register Doris catalog
curl -sf -X POST "${GRAVITINO_URL}/api/metalakes/${METALAKE}/catalogs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "doris_serving",
    "type": "RELATIONAL",
    "provider": "jdbc-doris",
    "comment": "Doris OLAP serving layer",
    "properties": {
      "jdbc-url": "jdbc:mysql://doris-fe:9030",
      "jdbc-driver": "com.mysql.jdbc.Driver",
      "jdbc-user": "root",
      "jdbc-password": ""
    }
  }' || echo "Doris catalog already exists"

echo "Gravitino initialization complete."
