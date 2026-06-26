# PRD: Langfuse External Observability Source and Analytics Extensions

> Status: Draft PRD for agent-ready implementation planning (2026-06-26). This PRD follows ADR 011 and the project domain glossary. It is not an implementation record.

## Problem Statement

AI Agent and AI Application teams often already use Langfuse for runtime observability, tracing, prompt workflows, and evaluation feedback. This project needs to ingest those applications without forcing teams to replace Langfuse, while still preserving the lakehouse's own ODS/DWD/DWS/ADS contracts, privacy rules, long-term retention, and BI/governance value.

The current repository has strong AI observability modeling across LLM Request, Agent Run, Agent Span, Agent Tool Call, Retrieval Request, Feedback Action, and Evaluation Judgment, but its default live connector only covers LLM request events through Postgres CDC. Langfuse should become an External Observability Source that lowers onboarding cost and also inspires higher-value ADS products such as trace health, prompt version comparison, evaluation regression, dataset/experiment comparison, and annotation operations.

## Solution

Adopt Langfuse as an External Observability Source. Build an adapter path that reads Langfuse traces, observations, generations, and Score Events through stable public integration surfaces such as API/export or OpenTelemetry-compatible ingestion, then normalizes them into existing ODS/DWD contracts.

Treat a Langfuse trace as a Trace Envelope by default. It supplies `trace_id` correlation, but it is not automatically an Agent Run. It only becomes an Agent Run when metadata or source conventions provide task/run semantics.

Use existing DWD facts first. Map generation observations to LLM Request, span/chain observations to Agent Span, tool observations to Agent Tool Call, retriever observations to Retrieval Request, and Score Events to either Feedback Action or Evaluation Judgment based on source/configuration.

Use Langfuse product concepts to expand ADS value without duplicating the warehouse model. Enhance existing prompt/evaluation/session assets first, and only introduce new DWS tables when a grain is stable and reused by more than one ADS product.

## User Stories

1. As an AI application developer, I want to keep using Langfuse instrumentation, so that I do not need to rewrite observability hooks to adopt this lakehouse.
2. As an AI application developer, I want Langfuse traces to appear in lakehouse facts, so that I can analyze them with warehouse metrics and BI tools.
3. As an AI application developer, I want a Langfuse trace to remain a Trace Envelope unless run metadata is explicit, so that Agent Run metrics are not inflated by ambiguous trace boundaries.
4. As an AI application developer, I want generation observations mapped to LLM Request facts, so that model usage, latency, cost, and failure metrics are consistent with existing dashboards.
5. As an AI application developer, I want span and chain observations mapped to Agent Span facts, so that runtime steps can be analyzed alongside existing Agent runs.
6. As an AI application developer, I want tool observations mapped to Agent Tool Call facts, so that tool reliability and latency are visible in warehouse metrics.
7. As an AI application developer, I want retriever observations mapped to Retrieval Request facts when metadata is sufficient, so that retrieval quality and latency can be analyzed.
8. As an AI product manager, I want Langfuse Score Events split into Feedback Action or Evaluation Judgment, so that user feedback is not mixed with automated evaluation.
9. As an AI product manager, I want prompt version performance compared across cost, latency, reliability, and quality, so that prompt releases can be judged with evidence.
10. As an AI product manager, I want trace health detail ADS output, so that high-cost, slow, or failed traces can be reviewed quickly.
11. As an AI product manager, I want evaluation regression reporting, so that prompt/model/release changes that reduce quality are visible before rollout.
12. As an AI product manager, I want dataset and experiment comparison at ADS level, so that baseline and candidate variants can be compared without prematurely creating a full experiment domain model.
13. As a data engineer, I want the Langfuse adapter to emit contract-compatible ODS or Raw events, so that existing Spark/Flink DWD paths can validate and process them.
14. As a data engineer, I want prompt and response bodies hashed or measured instead of stored in DWD/DWS/ADS, so that sensitive text does not spread through serving layers.
15. As a data engineer, I want adapter checkpoints and replay windows, so that late or repeated Langfuse events can be handled idempotently.
16. As a data engineer, I want malformed or underspecified Langfuse records sent to quarantine, so that one bad trace does not stop an ingestion batch.
17. As a data engineer, I want provider usage fields normalized, so that prompt tokens, completion tokens, and total tokens keep the same warehouse meaning.
18. As a data engineer, I want estimated cost recalculated or clearly sourced, so that cost metrics do not drift across Langfuse and the lakehouse.
19. As a data engineer, I want Langfuse metadata mapped into app, feature, agent, task, prompt, model, environment, and region dimensions, so that DWS and ADS grains remain comparable.
20. As a data engineer, I want existing prompt-version DWS/ADS assets extended before new prompt tables are added, so that the model avoids duplicate assets.
21. As a data engineer, I want session or trace DWS tables deferred until their grain is stable, so that early ADS experiments do not harden the wrong model.
22. As a data engineer, I want dataset and experiment metadata initially treated as controlled ADS inputs, so that the project can show experiment value without over-expanding DWD.
23. As a BI user, I want Langfuse-backed traces visible in Doris-backed dashboards, so that application observability can be explored without entering Langfuse.
24. As a BI user, I want derived rates calculated from stored numerator and denominator counts, so that longer-period rates are not computed by averaging daily rates.
25. As a platform operator, I want Langfuse integration health measured separately from lakehouse health, so that source outage and warehouse outage are distinguishable.
26. As a platform operator, I want the adapter to avoid Langfuse internal ClickHouse schema, so that Langfuse version upgrades do not break the lakehouse contract.
27. As a reviewer of this student project, I want to see realistic Langfuse-inspired data products, so that the project demonstrates knowledge of real AI observability workflows.
28. As a reviewer of this student project, I want the design to preserve clean warehouse layers, so that product ambition does not create an unmaintainable schema.
29. As a future agent contributor, I want the PRD split into implementation issues, so that connector, DWD mapping, ADS products, tests, and documentation can be built independently.
30. As a future agent contributor, I want each issue to reference ADR 011 and the domain glossary, so that implementation remains consistent with the decided model.

## Implementation Decisions

- Langfuse is adopted as an External Observability Source, not as the warehouse source of truth, BI serving layer, or replacement for Kafka, Spark, Flink, Paimon, Doris, or Gravitino.
- The integration should prefer Langfuse API/export or OpenTelemetry-compatible ingestion surfaces over direct reads from Langfuse internal ClickHouse tables.
- A Langfuse trace is a Trace Envelope by default. It provides correlation through `trace_id`; it only maps to Agent Run when metadata or source conventions explicitly define run/task semantics.
- The adapter should normalize Langfuse records into existing ODS/Raw event contracts before DWD. It should not create a parallel Langfuse-specific DWD model for the first implementation.
- Generation observations map to LLM Request facts.
- Span and chain observations map to Agent Span facts unless they are more specifically identified as tool, retriever, or generation events.
- Tool observations map to Agent Tool Call facts when tool name/type and execution outcome can be derived.
- Retriever observations map to Retrieval Request facts when query, strategy, top-k, returned count, score, and latency metadata are sufficient.
- Score Events must be classified before DWD. User/manual feedback becomes Feedback Action. Evaluator, judge, test, dataset-run, or automated scoring becomes Evaluation Judgment.
- Prompt and response bodies must not enter DWD/DWS/ADS. The adapter should emit privacy-safe hashes, sizes, character counts, and token counts.
- Missing required fields should produce quarantine rows rather than failing an entire batch.
- Prompt version analytics should extend existing prompt-version DWS and ADS assets before any new prompt-version aggregate is introduced.
- Session and trace analytics should be proven first through ADS trace health outputs. A new session/trace DWS table is only justified when the grain stabilizes and multiple ADS products reuse it.
- Dataset and experiment functionality should be delivered in the current stage as ADS-level comparison using controlled metadata. A full dataset/experiment DWD/DWS/DIM domain is deferred until lifecycle and reuse requirements are clear.
- ADS products should include trace health detail, prompt version comparison, evaluation regression, dataset/experiment comparison, and annotation operations.
- DWS additions must store counts, sums, durations, amounts, and score numerators/denominators. Rates remain derived from stored components.
- Langfuse integration must preserve the project naming conventions, layer rules, grain documentation, privacy constraints, and current catalog naming.
- The first implementation should target a POC that proves ingestion and normalization with a small set of Langfuse traces, observations, generations, and scores.

## Testing Decisions

- The highest preferred test seam is the Langfuse normalization boundary: given representative Langfuse trace, observation, generation, and score payloads, the system should emit contract-compatible events for existing DWD facts or quarantine records.
- A good test verifies externally visible behavior: normalized event fields, target fact classification, privacy-safe payload handling, quarantine behavior, and idempotency. It should not assert internal helper call order.
- Existing warehouse contract and Spark transform tests are prior art for validating field projection, required defaults, data quality rules, and quarantine behavior.
- Existing SQL asset tests are prior art for validating that any new or extended DWS/ADS assets match naming, grain, and generated DDL expectations.
- The connector tests should cover generation-to-LLM Request mapping, span-to-Agent Span mapping, score-to-Feedback Action mapping, score-to-Evaluation Judgment mapping, and unknown/ambiguous records entering quarantine.
- Tests should include a case where a Langfuse trace has no run metadata and therefore remains only a Trace Envelope rather than creating an Agent Run.
- Tests should include a case where a Langfuse trace has explicit run/task metadata and can produce an Agent Run.
- Tests should verify prompt/response bodies are not emitted into DWD/DWS/ADS fields.
- Tests should verify token totals, latency, status, environment, model, prompt version, and cost source behavior.
- ADS tests should validate derived rates from numerators and denominators rather than averaged daily rates.
- Documentation tests should keep links, table names, and commands resolvable if new docs or assets are introduced.

## Out of Scope

- Replacing Langfuse or rebuilding its online trace UI.
- Replacing the lakehouse stack with Langfuse, or using Langfuse as the BI serving layer.
- Depending on Langfuse internal ClickHouse tables as a stable production contract.
- Storing raw prompt or response bodies in DWD/DWS/ADS.
- Building a full dataset/experiment domain model with independent lifecycle, permissions, and dimensions in the first implementation.
- Building an OpenTelemetry collector pipeline unless the API/export POC shows that collector-based fanout is required.
- Production high availability, enterprise identity, secret management, and cross-tenant RBAC beyond the repository's existing local-demo scope.
- Real prompt/response data, API keys, direct personal contact data, or undeidentified IP addresses in committed fixtures.

## Further Notes

The main implementation path should be split into independently agent-ready issues:

- Langfuse adapter POC and fixture capture
- Normalization and classification rules
- DWD mapping and quarantine behavior
- Score Event split into Feedback Action and Evaluation Judgment
- Prompt version ADS enhancement
- Trace health ADS product
- Evaluation regression and dataset/experiment comparison ADS product
- Documentation, runbook, and metric definition updates

The issue tracker publication step could not be completed in this session because GitHub CLI authentication for the configured remote is invalid. The PRD is therefore published as a repository document and should be copied into the issue tracker or used as input to `/to-issues` once authentication is restored.
