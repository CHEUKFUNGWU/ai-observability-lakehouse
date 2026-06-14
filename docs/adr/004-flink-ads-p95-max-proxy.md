# ADR 004: Why Flink ADS Uses MAX As The Latency Upper Bound

## Context

The local Flink SQL path does not use a production percentile aggregate implementation for continuous ADS updates.

## Decision

Store `max_latency_ms` in Flink ADS and keep percentile reporting in Spark and Doris layers.

## Consequences

- Streaming ADS remains simple and stable.
- The metric name avoids pretending that `MAX` is p95.
- Users needing p95 should query Spark-produced ADS or Doris DWD facts.
