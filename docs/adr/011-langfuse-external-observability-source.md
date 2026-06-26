# ADR 011: Treat Langfuse as an External Observability Source

- Status: Accepted

## Context

Many AI Agent and AI Application teams already use Langfuse for runtime observability, tracing, prompt workflows, and evaluation feedback. This project should lower the cost of onboarding those applications while preserving its own ODS/DWD/DWS/ADS contracts, long-term lakehouse storage, and BI/governance layers.

The main alternatives were to ignore Langfuse, treat Langfuse as a replacement observability product, read Langfuse internal ClickHouse tables directly, or adapt Langfuse data into the existing lakehouse contracts.

## Decision

Adopt Langfuse as an external AI runtime observability source, not as the warehouse source of truth or serving layer.

The preferred integration path is Langfuse API/export or OpenTelemetry-compatible ingestion through an adapter that writes contract-compatible ODS/Raw events. The adapter maps Langfuse traces, observations, generations, and scores into existing DWD facts such as LLM requests, Agent spans, tool calls, retrieval requests, feedback actions, and evaluation judgments.

Langfuse product concepts are valid inspiration for DWS/ADS data products, especially trace health, prompt version comparison, evaluation regression, dataset/experiment analysis, and annotation operations. These features must still obey this repository's naming, grain, privacy, and derived-rate rules.

## Consequences

- Langfuse integrations should not depend on Langfuse internal ClickHouse schema as the stable contract.
- A Langfuse trace is treated as a trace envelope by default, not automatically as an Agent Run.
- Langfuse scores must be classified before DWD: user/manual feedback becomes Feedback Action, while evaluator/judge/test results become Evaluation Judgment.
- Existing prompt-version DWS/ADS assets should be extended before introducing duplicate prompt-version tables.
- Raw prompt/response content remains outside DWD/DWS/ADS except for privacy-safe hashes, sizes, and derived statistics.
