# Dashboard Implementation Plan

This plan adds a visualization layer to the AI Observability Lakehouse using
Apache Superset for BI analytics and Grafana for operational monitoring. Both
tools connect to Doris via the MySQL protocol on port 9030.

## 1. Design Rationale

### Why two tools?

The 12 existing dashboard queries in `sql/doris_dashboard_queries.sql` fall into
two distinct categories:

| Category | Queries | Best tool |
|---|---|---|
| BI analytics (cost, latency, reliability, leaderboards) | Q1-Q3, Q8-Q12 | Superset |
| Operational monitoring (compliance, handoffs, health breaches) | Q4-Q7 | Grafana |

Superset is optimized for interactive data exploration with rich chart types,
SQL Lab, and shareable dashboards. Grafana is optimized for time-series
monitoring with alerting, auto-refresh, and status panels. Using both avoids
forcing one tool into the other's role.

### Connection method

Both tools connect to Doris through the MySQL wire protocol:

```
Superset ──(mysql://root:@doris-fe:9030/ai_observability)──→ Doris FE
Grafana  ──(MySQL data source: doris-fe:9030)──────────────→ Doris FE
```

No additional drivers or connectors are needed.

---

## 2. Tech Stack

| Component | Image | Port | Role |
|---|---|---|---|
| Superset | `apache/superset:4.1.1` | 8088 | BI dashboards and SQL Lab |
| Superset metadata DB | `postgres:16` | internal | Superset internal state |
| Superset cache | `redis:7-alpine` | internal | Query cache and Celery broker |
| Grafana | `grafana/grafana-oss:11.6.0` | 3001 | Operational monitoring |

Total additional containers: 4 (Superset + its Postgres + Redis + Grafana).

---

## 3. Infrastructure

### 3.1 docker-compose.yml additions

Add the following services after the existing `doris-init` service:

```yaml
  # ---- Superset (BI Analytics) ----
  superset-metadata:
    image: postgres:16
    container_name: ai-observability-superset-db
    environment:
      POSTGRES_DB: superset
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
    volumes:
      - superset_metadata:/var/lib/postgresql/data

  superset-redis:
    image: redis:7-alpine
    container_name: ai-observability-superset-redis

  superset:
    image: apache/superset:4.1.1
    container_name: ai-observability-superset
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: "ai-observability-local-dev-secret-key"
      SQLALCHEMY_DATABASE_URI: "postgresql://superset:superset@superset-metadata:5432/superset"
      REDIS_URL: "redis://superset-redis:6379/0"
    depends_on:
      - superset-metadata
      - superset-redis
    volumes:
      - superset_home:/app/superset_home

  # ---- Grafana (Operational Monitoring) ----
  grafana:
    image: grafana/grafana-oss:11.6.0
    container_name: ai-observability-grafana
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_INSTALL_PLUGINS: ""
    volumes:
      - grafana_data:/var/lib/grafana
      - ./config/grafana/provisioning:/etc/grafana/provisioning:ro
```

Add to the `volumes` section:

```yaml
  superset_metadata:
  superset_home:
  grafana_data:
```

### 3.2 Grafana provisioning files

Create `config/grafana/provisioning/datasources/doris.yaml`:

```yaml
apiVersion: 1
datasources:
  - name: Doris
    type: mysql
    url: doris-fe:9030
    database: ai_observability
    user: root
    password: ""
    isDefault: true
    editable: true
```

Create `config/grafana/provisioning/dashboards/default.yaml`:

```yaml
apiVersion: 1
providers:
  - name: default
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards/json
      foldersFromFilesStructure: true
```

Create the directory for dashboard JSON exports:

```
config/grafana/provisioning/dashboards/json/
```

### 3.3 Superset initialization script

Create `scripts/init_superset.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

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
from superset.extensions import db
from superset.models.core import Database

app = create_app()
with app.app_context():
    existing = db.session.query(Database).filter_by(
        database_name='AI Observability (Doris)'
    ).first()
    if not existing:
        doris_db = Database(
            database_name='AI Observability (Doris)',
            sqlalchemy_uri='mysql://root:@doris-fe:9030/ai_observability',
            expose_in_sqllab=True,
            allow_run_async=True,
        )
        db.session.add(doris_db)
        db.session.commit()
        print('Doris database registered.')
    else:
        print('Doris database already registered.')
"

echo "Superset initialization complete."
echo "Open http://localhost:8088 (admin / admin)"
```

### 3.4 Makefile targets

```makefile
infra-dashboard:
	docker compose up -d superset-metadata superset-redis superset grafana

init-superset:
	scripts/init_superset.sh

dashboard-stop:
	docker compose stop superset superset-metadata superset-redis grafana
```

---

## 4. Superset Dashboard Design

### 4.1 Dashboard: AI Observability Overview

**Purpose**: Executive-level view of LLM cost, reliability, and usage trends.

**Filters**: Date range (default: last 7 days), App name, Feature name.

#### Layout

```
Row 1: KPI cards (4 columns)
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ Total Req    │ Success Rate │ Total Cost   │ Avg Latency  │
│   12,450     │   96.2%      │   $3.82      │   245ms      │
└──────────────┴──────────────┴──────────────┴──────────────┘

Row 2: Trend chart (full width)
┌─────────────────────────────────────────────────────────────┐
│  📈 Daily Request Count, Cost & Error Rate                  │
│  (Q1 — dual-axis line chart: left=count, right=cost+rate)   │
└─────────────────────────────────────────────────────────────┘

Row 3: Feature breakdown (2 columns)
┌─────────────────────────────┬───────────────────────────────┐
│  📊 Request Volume by       │  🥧 Cost Distribution by      │
│     Feature                 │     Feature                   │
│  (Q2 — vertical bar)        │  (Q3 — pie / donut)           │
└─────────────────────────────┴───────────────────────────────┘

Row 4: Reliability + Latency (2 columns)
┌─────────────────────────────┬───────────────────────────────┐
│  📊 Error Rate by Feature   │  📊 Weighted Avg Latency by   │
│  (Q8 — bar, sorted desc)    │     Feature                   │
│                             │  (Q9 — horizontal bar)        │
└─────────────────────────────┴───────────────────────────────┘

Row 5: Model analysis (2 columns)
┌─────────────────────────────┬───────────────────────────────┐
│  🥧 Cost by Model           │  📋 Model Cost with Pricing   │
│  (Q10 — treemap or pie)     │  (Q12 — table with data bars) │
└─────────────────────────────┴───────────────────────────────┘

Row 6: Leaderboard (full width)
┌─────────────────────────────────────────────────────────────┐
│  🏆 App + Feature Leaderboard                               │
│  (Q11 — sortable table with conditional formatting)         │
│  Columns: app, feature, requests, cost, latency, error_rate │
└─────────────────────────────────────────────────────────────┘
```

#### Chart specifications

| Chart | Query | Chart type | X axis | Y axis | Notes |
|---|---|---|---|---|---|
| KPI: Total Requests | Q1 (latest date) | Big Number | — | `SUM(request_count)` | Show delta vs previous day |
| KPI: Success Rate | Q1 (latest date) | Big Number | — | `success_rate` | Green if > 0.95, red otherwise |
| KPI: Total Cost | Q1 (latest date) | Big Number | — | `estimated_cost_usd` | Format as USD |
| KPI: Avg Latency | Q9 (all features) | Big Number | — | `AVG(weighted_avg_latency_ms)` | Format as ms |
| Daily Trend | Q1 | Dual-axis line | `date` | Left: `request_count`, Right: `estimated_cost_usd` | Add `error_rate` as secondary line |
| Request by Feature | Q2 | Bar chart | `feature_name` | `request_count` | Sort descending |
| Cost by Feature | Q3 | Pie / Donut | `feature_name` | `estimated_cost_usd` | Show percentage labels |
| Error Rate by Feature | Q8 | Bar chart | `feature_name` | `error_rate` | Color: red gradient by error_rate |
| Latency by Feature | Q9 | Horizontal bar | `feature_name` | `weighted_avg_latency_ms` | Sort descending |
| Cost by Model | Q10 | Treemap | `model_name` | `estimated_cost_usd` | Size = cost, color = avg_cost_per_request |
| Model Pricing Table | Q12 | Table | — | All columns | Highlight highest cost model |
| Leaderboard | Q11 | Table | — | All columns | Conditional formatting on error_rate |

### 4.2 Dashboard: Compliance & Governance

**Purpose**: Compliance officer view of access control and data retention.

**Filters**: Date range, Data classification, Policy name.

#### Layout

```
Row 1: Summary KPIs
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ Denied       │ Distinct     │ Retention    │ Total Rows   │
│ Attempts     │ Denied Users │ Actions      │ Affected     │
└──────────────┴──────────────┴──────────────┴──────────────┘

Row 2: Access audit (full width)
┌─────────────────────────────────────────────────────────────┐
│  📋 Denied Access Attempts by Classification & Action       │
│  (Q4 — table, grouped by date)                              │
└─────────────────────────────────────────────────────────────┘

Row 3: Retention enforcement (full width)
┌─────────────────────────────────────────────────────────────┐
│  📋 Retention Policy Enforcement Evidence                    │
│  (Q5 — table with sparkline on rows_affected)               │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 Dashboard: Agent Orchestration

**Purpose**: Agent team lead view of multi-agent handoff performance.

**Filters**: Date range, Parent agent, Child agent.

#### Layout

```
Row 1: KPIs
┌──────────────┬──────────────┬──────────────┬──────────────┐
│ Total        │ Error        │ Timeout      │ P95 Handoff  │
│ Handoffs     │ Handoffs     │ Handoffs     │ Latency      │
└──────────────┴──────────────┴──────────────┴──────────────┘

Row 2: Handoff details (full width)
┌─────────────────────────────────────────────────────────────┐
│  📋 Inter-Agent Handoff Bottlenecks                          │
│  (Q6 — table sorted by p95 latency desc)                    │
│  Color: red rows where timeout_cnt_1d > 0                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Grafana Dashboard Design

### 5.1 Dashboard: Platform Health

**Purpose**: SRE/ops view of pipeline health and threshold breaches.

**Auto-refresh**: 30 seconds.

#### Layout

```
Row 1: Status overview
┌──────────────────────────────────────────────────────────────┐
│  🚦 Pipeline Service Status                                  │
│  (Stat panels: Postgres, Kafka, Flink JM, Flink TM,        │
│   Doris FE, Doris BE — green/red based on health check)     │
└──────────────────────────────────────────────────────────────┘

Row 2: Breach table (full width)
┌──────────────────────────────────────────────────────────────┐
│  🔴 Platform Health Threshold Breaches                       │
│  (Q7 — table with threshold_utilization color coding)       │
│  >1.0 = red, 0.8-1.0 = yellow, <0.8 = green                │
└──────────────────────────────────────────────────────────────┘

Row 3: Flink job monitoring (2 columns)
┌──────────────────────────────┬───────────────────────────────┐
│  📈 Flink Checkpoint         │  📈 Flink Records             │
│     Duration Trend           │     In/Out per Job            │
│  (Flink REST API: /jobs/     │  (Flink REST API: /jobs/      │
│   {id}/checkpoints)          │   {id}/vertices/{id})         │
└──────────────────────────────┴───────────────────────────────┘
```

#### Grafana-specific data sources

For Flink metrics, add a second data source using the Infinity plugin or a JSON
API data source pointing to the Flink REST API:

| Panel | Data source | Endpoint |
|---|---|---|
| Flink job status | Infinity (JSON) | `http://flink-jobmanager:8081/jobs/overview` |
| Checkpoint duration | Infinity (JSON) | `http://flink-jobmanager:8081/jobs/{id}/checkpoints` |
| Records in/out | Infinity (JSON) | `http://flink-jobmanager:8081/jobs/{id}/vertices` |
| Service status | Shell script | `scripts/check_pipeline_health.sh` output |

The Doris-based Q7 query connects through the MySQL data source already
provisioned.

### 5.2 Dashboard: Data Quality Monitor

**Purpose**: Track DQ rule pass rates and quarantine trends.

**Auto-refresh**: 5 minutes.

#### Panels

| Panel | Query source | Chart type |
|---|---|---|
| DQ pass rate (today) | Doris: `SELECT COUNT(*) FROM dwd WHERE rules pass / total` | Gauge |
| Quarantine row count trend | Doris: daily quarantine counts | Time series |
| DQ rule breakdown | Doris: pass/fail per rule category | Stacked bar |
| Latest pipeline run metadata | Doris or file: `pipeline_runs.jsonl` | Table |

Note: DQ monitoring queries are not yet in `doris_dashboard_queries.sql`. They
should be added as Q13-Q16 when the DQ results table is materialized in Doris.

---

## 6. Query-to-Chart Mapping Summary

| Query | Dashboard | Chart | Tool |
|---|---|---|---|
| Q1: Daily traffic + cost | AI Overview | Trend line + KPIs | Superset |
| Q2: Request by feature | AI Overview | Bar chart | Superset |
| Q3: Cost by feature | AI Overview | Pie chart | Superset |
| Q4: Denied access | Compliance | Table | Superset |
| Q5: Retention enforcement | Compliance | Table | Superset |
| Q6: Agent handoff bottlenecks | Agent Orchestration | Table | Superset |
| Q7: Platform health breaches | Platform Health | Table + color | Grafana |
| Q8: Reliability by feature | AI Overview | Bar chart | Superset |
| Q9: Latency by feature | AI Overview | Horizontal bar | Superset |
| Q10: Cost by model | AI Overview | Treemap | Superset |
| Q11: App + feature leaderboard | AI Overview | Table | Superset |
| Q12: Cost by model with pricing | AI Overview | Table | Superset |
| New: Flink job status | Platform Health | Stat panels | Grafana |
| New: Checkpoint duration | Platform Health | Time series | Grafana |
| New: DQ pass rate | Data Quality | Gauge + bar | Grafana |

---

## 7. Implementation Phases

### Phase 1: Infrastructure (Day 1)

Files to create or modify:

| File | Action |
|---|---|
| `docker-compose.yml` | Add superset, superset-metadata, superset-redis, grafana services and volumes |
| `config/grafana/provisioning/datasources/doris.yaml` | Create Grafana Doris data source |
| `config/grafana/provisioning/dashboards/default.yaml` | Create Grafana dashboard provider config |
| `config/grafana/provisioning/dashboards/json/` | Create empty directory for dashboard JSON |
| `scripts/init_superset.sh` | Create Superset initialization script |
| `Makefile` | Add `infra-dashboard`, `init-superset`, `dashboard-stop` targets |

Verification:

```bash
make infra-serving           # Doris must be running
make infra-dashboard         # Start Superset + Grafana
make init-superset           # Initialize Superset + register Doris

# Verify
curl -s http://localhost:8088/health       # Superset health
curl -s http://localhost:3001/api/health   # Grafana health
```

### Phase 2: Superset AI Overview Dashboard (Day 2)

Build the main dashboard manually in the Superset UI:

1. Open `http://localhost:8088`, login as admin/admin.
2. Go to SQL Lab, verify Doris connection by running Q1.
3. Create each chart from Q1-Q3, Q8-Q12 using the chart builder.
4. Arrange charts into the AI Observability Overview dashboard.
5. Add date range, app_name, and feature_name filters.
6. Export the dashboard as JSON for version control.

Save the export to `config/superset/dashboards/ai_overview.json`.

### Phase 3: Superset Compliance + Agent Dashboards (Day 3)

1. Create charts from Q4-Q6.
2. Build the Compliance & Governance dashboard (Q4, Q5).
3. Build the Agent Orchestration dashboard (Q6).
4. Export both as JSON.

Save exports to:
- `config/superset/dashboards/compliance.json`
- `config/superset/dashboards/agent_orchestration.json`

### Phase 4: Grafana Platform Health Dashboard (Day 4)

1. Open `http://localhost:3001`, login as admin/admin.
2. Create a new dashboard named "Platform Health".
3. Add Q7 as a table panel using the Doris MySQL data source.
4. Add Flink REST API panels (requires Infinity or JSON API plugin).
5. Set auto-refresh to 30 seconds.
6. Export the dashboard JSON.

Save to `config/grafana/provisioning/dashboards/json/platform_health.json`.

### Phase 5: Demo integration (Day 5)

Update `scripts/run_serving_demo.sh` to include dashboard startup:

```bash
# After existing Doris sync steps:
docker compose up -d superset-metadata superset-redis superset grafana
scripts/init_superset.sh

printf '\nDashboards available at:\n'
printf '  Superset: http://localhost:8088 (admin/admin)\n'
printf '  Grafana:  http://localhost:3001 (admin/admin)\n'
```

Update health check to verify dashboard services:

```bash
# Add to check_pipeline_health.sh (inside skip_serving=false block)
if [[ "${skip_serving}" == false ]]; then
  require_running_service superset
  require_running_service grafana
fi
```

---

## 8. Dashboard JSON Version Control

Exported dashboard JSON files should be committed to the repository so that
dashboards are reproducible without manual setup.

```
config/
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── doris.yaml
│       └── dashboards/
│           ├── default.yaml
│           └── json/
│               └── platform_health.json
└── superset/
    └── dashboards/
        ├── ai_overview.json
        ├── compliance.json
        └── agent_orchestration.json
```

Grafana dashboards are auto-loaded from the `json/` directory on container
startup via the provisioning system.

Superset dashboards require an import step. Add to `scripts/init_superset.sh`:

```bash
for dashboard_file in config/superset/dashboards/*.json; do
  if [[ -f "${dashboard_file}" ]]; then
    echo "Importing dashboard: ${dashboard_file}"
    docker compose exec -T superset superset import-dashboards \
      -p "/app/${dashboard_file}" || true
  fi
done
```

---

## 9. Resource Impact

| Service | Memory estimate | CPU impact |
|---|---|---|
| Superset | ~512 MB | Low (idle until query) |
| Superset Postgres | ~64 MB | Minimal |
| Superset Redis | ~32 MB | Minimal |
| Grafana | ~128 MB | Low (idle until refresh) |
| **Total** | **~736 MB** | **Low** |

These services should only be started when needed. The `infra-light` workflow
is unaffected. Dashboard services are part of the serving layer:

```bash
# Day-to-day streaming development (no dashboard)
make infra-light
make seed-data
make flink-submit

# Full demo with dashboards
make demo-serving             # includes Doris + dashboards
```

---

## 10. Access URLs

| Service | URL | Credentials |
|---|---|---|
| Superset | `http://localhost:8088` | admin / admin |
| Grafana | `http://localhost:3001` | admin / admin |
| Doris Web UI | `http://localhost:8030` | — |
| Flink Web UI | `http://localhost:8081` | — |

---

## 11. ADR

Create `docs/adr/009-superset-grafana-dashboard-stack.md` to record the decision:

- **Status**: Accepted
- **Context**: The lakehouse has 12 dashboard queries and a Doris serving layer but no visualization. Need a dashboard stack that connects to Doris via MySQL protocol.
- **Decision**: Use Apache Superset for BI analytics dashboards and Grafana for operational monitoring dashboards. Both connect to Doris via MySQL protocol on port 9030.
- **Alternatives considered**: Streamlit (too code-heavy, no persistent dashboards), Metabase (less customizable, weaker portfolio signal), Grafana-only (not suited for analytics exploration).
- **Consequences**: Four additional Docker containers. Dashboard JSON is version-controlled for reproducibility. Dashboards are part of the serving layer, not required for streaming development.
