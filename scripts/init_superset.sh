#!/usr/bin/env bash
set -euo pipefail

wait_for_superset() {
  local retries=60
  until curl -fsS http://localhost:8088/health >/dev/null 2>&1; do
    retries=$((retries - 1))
    if (( retries == 0 )); then
      echo "Superset health endpoint did not become ready in time." >&2
      return 1
    fi
    sleep 2
  done
}

echo "Waiting for Superset to become ready..."
wait_for_superset

echo "Upgrading Superset metadata database..."
docker compose exec -T superset superset db upgrade

echo "Creating admin user..."
docker compose exec -T superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@local.dev \
  --password admin || true

echo "Initializing Superset..."
docker compose exec -T superset superset init

echo "Registering Doris database connection..."
docker compose exec -T superset python3 -c "
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database

    existing = db.session.query(Database).filter_by(
        database_name='AI Observability (Doris)'
    ).first()
    if not existing:
        doris_db = Database(
            database_name='AI Observability (Doris)',
            sqlalchemy_uri='mysql+pymysql://root:@doris-fe:9030/ai_observability',
            expose_in_sqllab=True,
            allow_run_async=True,
        )
        db.session.add(doris_db)
        db.session.commit()
        print('Doris database registered.')
    else:
        if existing.sqlalchemy_uri != 'mysql+pymysql://root:@doris-fe:9030/ai_observability':
            existing.sqlalchemy_uri = 'mysql+pymysql://root:@doris-fe:9030/ai_observability'
            db.session.commit()
            print('Doris database driver updated to PyMySQL.')
        else:
            print('Doris database already uses PyMySQL.')
        print('Doris database already registered.')
"

echo "Provisioning Superset dashboards from repository specs..."
docker compose exec -T superset python3 /app/bootstrap-scripts/provision_superset_dashboards.py --provision

echo "Superset initialization complete."
echo "Open http://localhost:8088 (admin / admin)"
