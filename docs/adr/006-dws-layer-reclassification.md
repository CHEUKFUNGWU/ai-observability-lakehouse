# ADR 006: Reclassify Daily Summary Metrics from ADS to DWS

## Status

Accepted

## Decision

Rename the reusable daily summary tables from `ads_*` to `dws_*` because downstream queries re-aggregate them by feature, model, and app.

## Consequences

Flink, Spark, Doris, and dashboard queries now treat these tables as warehouse summaries rather than application-specific marts.
