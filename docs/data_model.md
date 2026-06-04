# Data Model

## 1. Overview

This project models AI application observability data around two implemented runtime domains:

1. LLM request events
2. Agent runtime events

The model intentionally avoids Dify-specific names such as `workflow_run` or `node_execution`. Instead, it uses generic observability concepts that fit internal customer-support agents, research agents, coding agents and OpenAI/Claude-style agent runtimes:

- `agent_run`: one end-to-end agent task
- `agent_span`: one step inside an agent run, such as planning, retrieval, LLM call, tool call or final response
- `agent_tool_call`: one concrete tool invocation with arguments and result payload
- `llm_request`: one concrete model API call, optionally linked back to an agent run and span

The current warehouse layers are:

```text
Data sources / collectors
  -> Raw JSONL files
  -> ODS source event tables
  -> DWD typed business event tables
  -> ADS llm_feature_daily_metrics
  -> ADS agent_daily_metrics
  -> ADS agent_tool_daily_metrics
```

---

## 2. Source vs ODS vs DWD vs ADS

The mock generators and live API collectors are not warehouse layers. They are data sources.

| Layer | Current Implementation | Responsibility |
|---|---|---|
| Source | mock generator, DeepSeek live collector, Hermes trajectory parser | Produce application/runtime events |
| Raw | JSONL files under `data/raw/` | Local landing files for generated or collected events |
| ODS | Parquet under `data/warehouse/ods/` | Preserve source event fields and add technical metadata |
| DWD | Parquet under `data/warehouse/llm_request`, `agent_run`, `agent_span` | Cast types, normalize fields and apply row-level validation |
| ADS | Parquet under `data/warehouse/ads/` | Aggregate dashboard-ready metrics |

ODS deliberately does not calculate business metrics. It keeps source-aligned data stable so DWD logic can evolve without coupling directly to collectors.

---

## 3. ODS Tables

### Business Meaning

ODS tables are source event landing tables. They preserve the raw event fields from JSONL and append technical metadata.

Current ODS outputs:

| Table | Path | Source Event Type |
|---|---|---|
| ods_llm_request_events | `data/warehouse/ods/llm_request/events.parquet` | `llm_request` |
| ods_agent_run_events | `data/warehouse/ods/agent_run/events.parquet` | `agent_run` |
| ods_agent_span_events | `data/warehouse/ods/agent_span/events.parquet` | `agent_span` |
| ods_agent_tool_call_events | `data/warehouse/ods/agent_tool_call/events.parquet` | `agent_tool_call` |

### Added ODS Metadata

| Field | Type | Description |
|---|---|---|
| source_name | string | Source system or collector name |
| source_event_type | string | Source event type |
| ingested_at | timestamp | ODS ingestion timestamp |
| raw_event_json | string | Original event serialized as JSON |

### Grain

One row per source event.

---

## 4. DWD Table: llm_request_events

### Business Meaning

Each row represents one LLM API request. This table is the source of truth for model usage, latency, token cost, API reliability and prompt/response metadata. Raw prompt and response text stay in ODS/source data; DWD keeps hashes and length fields for safer analytics.

When the request is produced by an Agent, it can be linked to `agent_run_events` and `agent_span_events` through `run_id` and `span_id`.

### Fields

| Field | Type | Description |
|---|---|---|
| request_id | string | Unique LLM request ID |
| trace_id | string | Cross-system trace ID |
| run_id | string | Related Agent run ID |
| span_id | string | Related Agent span ID |
| agent_id | string | Agent identifier |
| agent_name | string | Agent display name |
| channel | string | Request channel, e.g. api, web, slack |
| user_id | string | User ID |
| session_id | string | Session ID |
| conversation_id | string | Conversation ID |
| app_name | string | AI application name |
| feature_name | string | Feature module |
| prompt_category | string | Prompt category |
| prompt_id | string | Prompt ID |
| prompt_version | string | Prompt version |
| model_name | string | LLM model name |
| provider | string | API provider, e.g. deepseek |
| prompt_hash | string | Hash of prompt text for safer deduplication |
| response_hash | string | Hash of response text |
| input_chars | int | Prompt character count |
| output_chars | int | Response character count |
| prompt_tokens | int | Input tokens |
| completion_tokens | int | Output tokens |
| total_tokens | int | Total tokens |
| request_type | string | chat, completion, embedding or other request type |
| is_streaming | boolean | Whether the API request used streaming |
| temperature | double | Sampling temperature |
| max_tokens | int | Requested max output tokens |
| finish_reason | string | Provider finish reason |
| retry_count | int | Number of retries before final result |
| latency_ms | int | Request latency in milliseconds |
| status | string | success / error |
| error_type | string | Error type |
| http_status | int | HTTP status code |
| estimated_cost_usd | double | Estimated API cost |
| mode | string | mock / live / replay |
| region | string | User or runtime region |
| environment | string | dev / staging / prod |
| created_at | timestamp | Request timestamp |
| date | date | Partition date |

### Grain

One row per provider request attempt result.

### Partition

Partitioned by `date`.

---

## 5. DWD Table: agent_run_events

### Business Meaning

Each row represents one end-to-end Agent task. Examples:

- A customer-support agent handling one user question
- A coding agent completing one edit request
- A research agent answering one investigation task
- An operations agent running one monitoring or remediation flow

This table is the main fact table for Agent-level success rate, cost, latency and workload volume.

### Fields

| Field | Type | Description |
|---|---|---|
| run_id | string | Unique Agent run ID |
| trace_id | string | Cross-system trace ID |
| agent_id | string | Agent identifier |
| agent_name | string | Agent display name |
| agent_version | string | Agent version |
| app_name | string | AI application name |
| user_id | string | User ID |
| session_id | string | Session ID |
| conversation_id | string | Conversation ID |
| task_type | string | Task category, e.g. support_answer, doc_summary |
| channel | string | Runtime channel, e.g. api, web, slack |
| toolsets_used | string | JSON string of toolsets or skills enabled for the run |
| input_text_hash | string | Hash of run input text |
| output_text_hash | string | Hash of final output text |
| start_time | timestamp | Run start time |
| end_time | timestamp | Run end time |
| duration_ms | int | End-to-end run duration |
| status | string | success / error |
| error_type | string | Run-level error type |
| turn_count | int | Conversation turns handled in the run |
| llm_call_count | int | Number of LLM calls in the run |
| tool_call_count | int | Number of tool calls in the run |
| retrieval_count | int | Number of retrieval steps in the run |
| total_tokens | int | Total tokens consumed by the run |
| estimated_cost_usd | double | Estimated total run cost |
| mode | string | mock / live / replay |
| region | string | User or runtime region |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event creation timestamp |
| date | date | Partition date |

### Grain

One row per Agent task/run.

### Partition

Partitioned by `date`.

---

## 6. DWD Table: agent_span_events

### Business Meaning

Each row represents one step inside an Agent run. This is similar to a tracing span and is more general than a low-code workflow node.

Common `span_type` values:

- `planning`
- `retrieval`
- `llm_call`
- `tool_call`
- `final_response`

This table is useful for diagnosing where Agent latency, cost and failures happen.

### Fields

| Field | Type | Description |
|---|---|---|
| span_id | string | Unique span ID |
| parent_span_id | string | Parent span ID, if nested |
| run_id | string | Related Agent run ID |
| trace_id | string | Cross-system trace ID |
| agent_id | string | Agent identifier |
| span_name | string | Human-readable span name |
| span_type | string | planning / retrieval / llm_call / tool_call / final_response |
| span_order | int | Step order inside the run |
| start_time | timestamp | Span start time |
| end_time | timestamp | Span end time |
| duration_ms | int | Span duration |
| status | string | success / error |
| error_type | string | Span-level error type |
| retry_count | int | Number of retries for this span |
| input_size | int | Input size for this step |
| output_size | int | Output size for this step |
| model_name | string | Model name if this is an LLM span |
| tool_name | string | Tool name if this is a tool span |
| mode | string | mock / live / replay |
| region | string | User or runtime region |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event creation timestamp |
| date | date | Partition date |

### Grain

One row per Agent runtime step/span.

### Partition

Partitioned by `date`.

---

## 7. DWD Table: agent_tool_call_events

### Business Meaning

Each row represents one concrete Agent tool invocation. This table keeps detail that is too verbose for `agent_span_events`, especially tool arguments and returned payloads.

Hermes trajectories are a natural source for this table because assistant messages can include `tool_calls`, and subsequent tool messages contain tool results.

### Fields

| Field | Type | Description |
|---|---|---|
| tool_call_id | string | Unique tool call ID |
| span_id | string | Related Agent tool-call span ID |
| run_id | string | Related Agent run ID |
| trace_id | string | Cross-system trace ID |
| agent_id | string | Agent identifier |
| tool_name | string | Tool name |
| tool_type | string | function / api / terminal / file / other tool category |
| arguments_json | string | Tool arguments serialized as JSON or raw argument string |
| result_text | string | Tool result text |
| result_size | int | Tool result size in characters |
| duration_ms | int | Tool call duration if available |
| status | string | success / error |
| error_type | string | Tool-level error type |
| retry_count | int | Number of retries |
| mode | string | mock / live / replay / hermes |
| region | string | User or runtime region |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event creation timestamp |
| date | date | Partition date |

### Grain

One row per Agent tool call.

### Partition

Partitioned by `date`.

---

## 8. ADS Table: llm_feature_daily_metrics

### Business Meaning

Daily feature-level LLM metrics for dashboard queries and ClickHouse loading.

### Grouping Keys

| Field | Type | Description |
|---|---|---|
| date | date | Metric date |
| app_name | string | AI application name |
| feature_name | string | Feature module |
| model_name | string | LLM model name |

### Metrics

| Field | Type | Description |
|---|---|---|
| request_count | long | Total request count |
| success_count | long | Successful request count |
| error_count | long | Failed request count |
| prompt_tokens | long | Total prompt tokens |
| completion_tokens | long | Total completion tokens |
| total_tokens | long | Total tokens |
| estimated_cost_usd | double | Total estimated cost |
| avg_latency_ms | double | Average latency |
| p95_latency_ms | long | Approximate p95 latency |

---

## 9. ADS Table: agent_daily_metrics

### Business Meaning

Daily Agent-level metrics for operational dashboards. This table answers:

- Which agents are most active?
- Which task types are slow or expensive?
- Which agents have high failure rates?
- Are failures concentrated in spans/tools instead of the whole run?

### Grouping Keys

| Field | Type | Description |
|---|---|---|
| date | date | Metric date |
| app_name | string | AI application name |
| agent_id | string | Agent identifier |
| agent_name | string | Agent display name |
| task_type | string | Task category |

### Metrics

| Field | Type | Description |
|---|---|---|
| run_count | long | Total Agent runs |
| success_count | long | Successful runs |
| error_count | long | Failed runs |
| turn_count | long | Total turns |
| llm_call_count | long | Total LLM calls |
| tool_call_count | long | Total tool calls |
| retrieval_count | long | Total retrieval steps |
| total_tokens | long | Total Agent tokens |
| estimated_cost_usd | double | Total estimated Agent cost |
| avg_duration_ms | double | Average Agent run duration |
| p95_duration_ms | long | Approximate p95 Agent run duration |
| span_count | long | Total spans under the agent/date |
| failed_span_count | long | Failed spans |
| tool_span_count | long | Tool-call spans |
| llm_span_count | long | LLM-call spans |
| span_failure_rate | double | failed_span_count / span_count |

---

## 10. ADS Table: agent_tool_daily_metrics

### Business Meaning

Daily tool-level Agent metrics for dashboard queries. This table answers:

- Which tools are called most often?
- Which tools fail most often?
- Which tools return the largest payloads?
- Which agents depend on which tools?

### Grouping Keys

| Field | Type | Description |
|---|---|---|
| date | date | Metric date |
| agent_id | string | Agent identifier |
| tool_name | string | Tool name |
| tool_type | string | Tool type |

### Metrics

| Field | Type | Description |
|---|---|---|
| tool_call_count | long | Total tool calls |
| success_count | long | Successful tool calls |
| error_count | long | Failed tool calls |
| retry_count | long | Total retries |
| avg_duration_ms | double | Average tool call duration |
| p95_duration_ms | long | Approximate p95 tool call duration |
| avg_result_size | double | Average result payload size |
| max_result_size | int | Max result payload size |

---

## 11. Entity Relationship

```text
agent_run_events.run_id
        |
        +---- agent_span_events.run_id
        |
        +---- llm_request_events.run_id

agent_span_events.span_id
        |
        +---- llm_request_events.span_id
        |
        +---- agent_tool_call_events.span_id
```

The `trace_id` field connects events across system boundaries. The `run_id` connects all events belonging to one Agent task. The `span_id` connects a specific LLM request to a specific Agent step.

---

## 12. Hermes Agent Sources

Hermes Agent can feed this model through multiple source paths:

| Source | Location / Interface | Best Use |
|---|---|---|
| Batch trajectories | `data/<run_name>/trajectories.jsonl` | Agent runs, spans, toolsets, tool stats and tool calls |
| Session SQLite | `~/.hermes/state.db` | Historical sessions, messages, token counts and billing |
| Gateway transcripts | `~/.hermes/sessions/` | Raw conversation transcripts including tool calls |
| API server events | `/v1/runs/{id}/events` SSE | Near-real-time run lifecycle and tool events |
| Hooks | `pre_tool_call`, `post_tool_call`, `pre_llm_call`, `post_llm_call` | High-fidelity runtime capture with arguments, results and duration |

The current implementation supports the batch trajectory path first:

```text
Hermes trajectories.jsonl
  -> parse_hermes_trajectories.py
  -> raw agent_run / agent_span / agent_tool_call JSONL
  -> ODS
  -> DWD
```

---

## 13. Planned Extensions

The following are natural next steps, but they are not part of the current implemented schema:

| Extension | Purpose |
|---|---|
| Retrieval observability fact | Query, embedding model, vector store, top_k, retrieved docs and retrieval latency |
| Configurable model pricing | Externalized model pricing for cost calculation |
| Prompt/version dimension | Prompt metadata, owner, version and release status |
| Agent dimension | Agent metadata, owner, version and deployment status |
