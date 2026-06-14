# ADR 002: Why Kafka As Real-Time ODS

## Context

Direct CDC to Paimon tightly couples ingestion with downstream storage. Restarting DWD or DWS jobs risks ingestion backpressure and limits replay.

## Decision

Insert Kafka between Postgres CDC and Flink SQL transforms, and treat Kafka as the real-time ODS buffer.

## Consequences

- CDC capture is decoupled from analytical table availability.
- Topic retention provides replay and failover headroom.
- The local stack gains an extra service to operate and test.
