# ADR 003: Why ADS Does Not Store Pre-Computed Rates

## Context

Success rates, error rates, and span failure rates are derived metrics. Storing them at write time can create inconsistencies after late data or recomputation.

## Decision

Store counts in ADS and derive rates in queries.

## Consequences

- ADS stays additive and recomputation-safe.
- Query logic is slightly more verbose.
- Dashboard consumers must divide by guarded denominators at read time.
