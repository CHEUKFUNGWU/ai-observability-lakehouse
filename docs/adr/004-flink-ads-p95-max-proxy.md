# ADR 004: Why Flink DWS Uses MAX As The Latency Upper Bound

## Context

The local Flink SQL path does not use a production percentile aggregate implementation for continuous DWS updates.

## Decision

Store `max_latency_ms` in Flink DWS, write `p95_latency_ms = 0`, and keep percentile reporting in Spark and Doris layers.

## Consequences

- Streaming DWS remains simple and stable.
- The metric name avoids pretending that `MAX` is p95.
- Users needing p95 should query Spark-produced DWS data or Doris DWD facts.
