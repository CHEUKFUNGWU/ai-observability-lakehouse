# ADR 001: Why Paimon Instead Of Iceberg

## Context

The streaming path needs primary-key upserts from Flink CDC, local filesystem support for demos, and straightforward Spark batch reads.

## Decision

Use Apache Paimon as the lakehouse table format for the streaming path.

## Consequences

- Flink CDC upsert semantics are simpler than a local Iceberg setup for this project.
- Spark remains able to read batch outputs for validation and backfill.
- The project trades broader ecosystem familiarity for a better fit with local Flink-centric demos.
