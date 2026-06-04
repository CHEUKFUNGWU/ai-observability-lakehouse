# AI Observability Lakehouse Product Document

## 1. Product Positioning

AI Observability Lakehouse is a data infrastructure project for production LLM and Agent applications.

It does not train models. It collects, models and analyzes runtime data from AI applications so product, engineering and data teams can answer:

- How many LLM requests and Agent runs happened today?
- Which app, feature, model, agent or tool consumes the most tokens and cost?
- Which features or agents fail most often?
- Where does latency occur: model call, tool call, retrieval, planning or final response?
- Which tools are used by agents, with what failure rate and payload size?
- Can we replay or inspect Hermes-style Agent trajectories as warehouse facts?

## 2. Current Product Scope

The implemented scope covers two runtime domains:

| Domain | Implemented Event Model | Purpose |
|---|---|---|
| LLM requests | `LLMRequestEvent` | Model usage, latency, token, cost and reliability analysis |
| Agent runtime | `AgentRunEvent`, `AgentSpanEvent`, `AgentToolCallEvent` | Agent workflow, span, tool-call, cost and failure analysis |

The project intentionally avoids low-code-platform-specific names such as Dify `workflow_run` or `node_execution`. It uses generic observability concepts that fit internal support agents, coding agents, research agents and Hermes-style agent runtimes.

## 3. Source Paths

Current source adapters:

| Source | Script | Output |
|---|---|---|
| Mock LLM requests | `scripts/generate_mock_llm_logs.py` | Raw LLM JSONL |
| DeepSeek live calls | `scripts/run_deepseek_live_calls.py` | Raw live LLM JSONL |
| Mock Agent events | `scripts/generate_mock_agent_logs.py` | Raw Agent run/span JSONL |
| Hermes trajectories | `scripts/parse_hermes_trajectories.py` | Raw Agent run/span/tool-call JSONL |

Collectors and parsers are source adapters, not warehouse layers. They feed raw files or source tables, then ODS/DWD/ADS handle modeling.

## 4. Warehouse Layers

```text
Source adapters
  -> Raw JSONL or Postgres source table
  -> ODS source event tables
  -> DWD typed fact tables
  -> ADS dashboard metric tables
  -> ClickHouse serving tables
```

| Layer | Responsibility |
|---|---|
| Raw | Local landing files for generated, collected or parsed source events |
| ODS | Preserve source fields and add ingestion metadata |
| DWD | Normalize data types, validate facts and remove oversized raw text where appropriate |
| ADS | Store additive daily metrics for dashboards |
| ClickHouse | Serve OLAP queries and support percentile calculation from DWD detail tables |

## 5. Dashboard Questions

LLM dashboard metrics:

- Daily request count, success count and error count
- Token usage by app, feature and model
- Estimated cost by feature and model
- Average latency and p95 latency
- Derived success/error rates in query layer

Agent dashboard metrics:

- Daily run count by app, agent and task type
- Agent tokens, cost, duration and p95 duration
- Span count and failed span count
- LLM span vs tool span volume
- Span failure rate

Tool dashboard metrics:

- Tool call count by agent, tool name and tool type
- Tool success/error counts
- Retry count
- Average and p95 tool duration
- Average and max result payload size

## 6. Current Data Products

Implemented DWD facts:

- `llm_request_events`
- `agent_run_events`
- `agent_span_events`
- `agent_tool_call_events`

Implemented ADS metrics:

- `llm_feature_daily_metrics`
- `agent_daily_metrics`
- `agent_tool_daily_metrics`

Implemented ClickHouse serving tables:

- `dwd_llm_request_events`
- `dwd_agent_run_events`
- `dwd_agent_span_events`
- `dwd_agent_tool_call_events`
- `ads_llm_feature_daily_metrics`
- `ads_agent_daily_metrics`
- `ads_agent_tool_daily_metrics`

## 7. Product Value

For AI engineering teams:

- Debug model latency, token usage, tool-call failures and Agent step failures.
- Compare Spark batch and Flink/Paimon streaming-batch outputs through consistent schemas.
- Keep raw prompt/response text in ODS/source while using hashes and size fields in DWD.

For data teams:

- Maintain a generic AI observability subject domain instead of a Dify-specific workflow model.
- Separate source adapters from warehouse layers.
- Keep ADS tables additive and derive rates in query/dashboard layers.

For product and operations teams:

- Track AI feature usage, cost, reliability and Agent tool behavior.
- Use ClickHouse for fast operational dashboards.

## 8. Explicit Non-Scope For Current MVP

The following are planned extensions, not implemented core tables:

- Dedicated RAG retrieval fact table
- Prompt/version dimension table
- Agent dimension table
- Config-driven pricing table
- Production Superset/Grafana dashboard bundle
