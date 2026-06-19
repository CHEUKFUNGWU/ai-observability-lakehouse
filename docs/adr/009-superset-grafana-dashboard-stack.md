# 009 Superset And Grafana Dashboard Stack

- Status: Accepted

## Context

The lakehouse exposes 12 dashboard queries in `sql/doris_dashboard_queries.sql`
and already materializes a Doris serving layer. It did not yet include a
dashboard stack that operators or stakeholders could open directly.

## Decision

Use Apache Superset for BI analytics dashboards and Grafana for operational
monitoring dashboards. Both tools connect to Doris over the MySQL protocol on
port `9030`.

## Alternatives Considered

- Streamlit: flexible, but too code-heavy for persistent dashboard authoring.
- Metabase: simpler, but less customizable for a mixed observability showcase.
- Grafana only: strong for monitoring, weak for analytics exploration.

## Consequences

- The local serving stack adds four containers: Superset, Superset Postgres,
  Superset Redis, and Grafana.
- Dashboard assets live in version control for reproducible local demos.
- Dashboard services remain part of the serving/demo layer, not the default
  streaming development workflow.
