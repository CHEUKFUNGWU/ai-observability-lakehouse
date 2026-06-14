# ADR 008: Use Paimon as the Shared Warehouse for Flink and Spark

## Status

Accepted

## Decision

Spark backfill and validation jobs read and write the same `paimon_lake` catalog that Flink uses, eliminating the parallel Parquet warehouse as the primary analytics path.

## Consequences

Spark becomes a supplementary backfill and validation engine, while Paimon remains the single source of truth for DWD and DWS data.
