Store generated Superset dashboard ZIP bundles in this directory.

Current source of truth:

- `scripts/provision_superset_dashboards.py --provision` upserts the Doris database,
  virtual datasets, charts, and dashboards directly inside Superset.
- `scripts/provision_superset_dashboards.py --write-bundles config/superset/dashboards`
  emits official Superset 4.1 dashboard export bundles (`*.zip`) from the same
  deterministic spec for version control and auditability.

These ZIP bundles follow Superset 4.1's native export format:

- `metadata.yaml`
- `databases/*.yaml`
- `datasets/**/*.yaml`
- `charts/*.yaml`
- `dashboards/*.yaml`
