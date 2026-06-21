# ADR 010: Use Gravitino as the Metadata Control Plane

- Status: Accepted

## Context

Flink and Spark share a Paimon warehouse, while Doris provides a separate
serving/query layer. File paths and engine-local catalog declarations alone do
not provide a single operational place to inspect and manage lakehouse catalog
metadata.

The metadata service must not become a second data store or change the existing
ODS/DWD/DWS/DIM/ADS naming and grain contracts.

## Decision

Use Apache Gravitino 1.2.0 as the metadata control plane for the shared Paimon
warehouse.

- Create the metalake `ai_observability`.
- Register the relational catalog `paimon_lake` with provider
  `lakehouse-paimon`.
- Use the filesystem backend at `file:///workspace/data/paimon` for the local
  development stack.
- Persist Gravitino service state in the `gravitino_data` Docker volume.
- Expose the API and Web V2 UI on port `8090`.
- Initialize the metalake and catalog through an idempotent script so container
  restarts do not create duplicate resources or hide initialization failures.

Flink and Spark continue to use the Paimon runtime for table data and snapshot
I/O. Gravitino manages and exposes the catalog metadata; Paimon remains the
storage system. Doris local tables and the Doris-side Paimon Catalog remain
serving-layer assets and are not implicitly registered as a Gravitino catalog.

## Consequences

- Operators have a consistent API/UI for the Paimon metalake and catalog.
- Catalog naming becomes an explicit contract shared by documentation and jobs.
- The light local stack gains one long-running service and a persistent volume.
- Gravitino availability and catalog initialization must be checked separately
  from data-path health.
- The local single-instance, filesystem-backed setup is not production HA.
- Column-level lineage, data quality, ownership, retention enforcement, and
  authorization are not automatically provided by catalog registration; they
  require separate metadata producers and governance controls.
