# ADR 007: Use the Same DQ Rules in Flink and Spark

## Status

Accepted

## Decision

Mirror the Spark validation rules in the Flink DWD filter so both paths enforce the same completeness, validity, and consistency checks.

## Consequences

The streaming path no longer accepts rows that the Spark validation path would quarantine.
