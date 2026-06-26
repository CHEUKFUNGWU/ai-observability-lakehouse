# Domain Glossary

## Runtime Entities

### LLM Request

One concrete provider request attempt result. A retry that reaches the provider is a separate request fact when it has its own `request_id`.

### Agent Run

One end-to-end agent task. It can contain spans, LLM requests, tool calls, retrievals, and inter-agent handoffs.

### Agent Span

One trace-like runtime step inside an agent run, such as planning, retrieval, LLM call, tool call, or final response.

### Agent Tool Call

One concrete tool invocation, including its arguments metadata, result size, duration, retry count, and outcome.

### Agent Handoff

The transfer of work or control from a parent agent run to a distinct child agent run. A handoff is not an internal span performed by the same run.

### External Observability Source

A system outside this lakehouse that already captures AI runtime traces, spans, generations, scores, or sessions and can be adapted into the ODS/DWD contracts. It is not the warehouse system of record.
_Avoid_: Warehouse source of truth, serving layer

### Trace Envelope

An external trace-level correlation boundary that groups runtime observations under one `trace_id`. A trace envelope is not automatically an Agent Run; it only becomes a run when the source provides task/run semantics.
_Avoid_: Agent Run, Session

## Quality and Governance

### Retrieval Request

One search or retrieval operation against a knowledge base, including strategy, top-k, returned-hit, similarity, and latency observations.

### Feedback Action

One explicit user feedback event, such as thumbs up/down, rating, regeneration, or report. It may arrive after the related request or run.

### Evaluation Judgment

One quality judgment for one dimension of a request or run, produced by a human, LLM judge, ground truth comparison, classifier, or rule.

### Score Event

A generic quality or feedback signal emitted by an external observability source. It must be classified into a Feedback Action or Evaluation Judgment before entering DWD.
_Avoid_: Evaluation Judgment, Feedback Action

### Guardrail Check

One evaluation of one safety or policy rule at a specific stage. A check can trigger an action such as block, redact, or warn.

### Access Audit Event

A record of one user's attempt to act on a classified AI data resource, whether the attempt was granted or denied.

### Data Retention Event

A record that a retention policy archived, anonymized, or deleted one table partition. It describes policy enforcement evidence, not the policy definition itself.

## Platform and Modeling

### Metalake

A top-level Gravitino metadata namespace that groups related catalogs. This project uses `ai_observability`.

### Catalog

A metadata collection inside a metalake. Gravitino registers the shared Paimon warehouse as the relational catalog `paimon_lake`; the catalog stores and exposes metadata, while Paimon stores table data and snapshots.

### Platform Health Metric

A threshold-bearing observation about the operational state of a lakehouse component. It is distinct from business workload and AI runtime metrics.

### Quarantine

The storage path for rows that fail completeness, validity, or consistency rules. Quarantine preserves evidence and prevents one bad row from failing an entire batch.

### Grain

The exact business meaning of one table row, defined by the table name, key columns, contract, and documentation.

### Derived Rate

A ratio calculated from stored numerator and denominator counts in a query, view, or report layer. Daily rates must not be averaged to produce a longer-period rate.
