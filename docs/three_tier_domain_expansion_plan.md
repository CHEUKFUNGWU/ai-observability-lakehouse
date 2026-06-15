# AI Observability Lakehouse — Three-Tier Domain Expansion Plan

## 1. Current State Diagnosis

The lakehouse MVP covers two runtime domains with a compact table set:

| Layer | Tables | Count |
|---|---|---|
| DWD | `llm_request_events`, `agent_run_events`, `agent_span_events`, `agent_tool_call_events` | 4 |
| DWS | `llm_feature_daily_metrics`, `agent_daily_metrics`, `agent_tool_daily_metrics` | 3 |
| ADS | `cost_anomaly_daily`, `sla_daily_report`, `prompt_version_daily_metrics` | 3 |
| DIM | `dim_model_df` | 1 |

Table count is low for three reasons:

1. **Business domains are narrow.** Only LLM request and Agent runtime are modeled. User, organization, tenant, Prompt, experiment, retrieval, knowledge base, billing, guardrail and feedback domains are absent.
2. **Aggregation granularities are limited.** DWS tables are daily-only. Hourly, user-level, tenant-level, session-level, conversation-level, trace-level, region-level and environment-level summaries do not exist, despite the DWD fact tables already carrying the fields needed to produce them.
3. **ADS covers few application scenarios.** Only cost anomaly, SLA report and prompt-version metrics are implemented. Dashboards for retrieval quality, user satisfaction, content safety, cost governance, model comparison, agent diagnostics and platform health have no supporting tables.

---

## 2. Architecture Context

The expansion plan builds on top of the existing unified architecture:

```text
Sources
  -> Postgres -> Flink CDC -> Kafka ODS -> Flink SQL -> Paimon DWD/DWS
  -> JSONL -> Spark backfill/validation ──────────────────┘
                                                           |
                                                           v
                                                   Doris serving -> Dashboard
```

New domains follow the same layering:
- Source adapters produce raw events
- DWD stores validated, typed fact rows
- DWS stores additive daily (or hourly) aggregates
- ADS stores application-specific derived tables
- DIM stores slowly-changing descriptive attributes
- All tables land in Paimon and sync to Doris

Implementation note: conceptual table names in this plan use short domain labels for readability. Physical assets in this repository use the canonical naming standard from `AGENTS.md`, for example `dwd_ai_retrieval_request_di`, `dws_ai_retrieval_knowledge_base_request_1d`, and `ads_observability_retrieval_daily_quality`.

---

## 3. Tier 1 — Fill Missing Faces of the AI Runtime (Month 1-2)

### 3.1 Why Tier 1 First

These three domains are embedded in the existing LLM/Agent call chain. Their events are naturally emitted alongside current event types, so no new external system integrations are needed. They connect to existing fact tables via `trace_id`, `run_id`, `request_id` and `session_id`.

```text
agent_run_events
    |-- agent_span_events (span_type = retrieval)
    |       └── retrieval_events          <- Tier 1
    |-- llm_request_events
    |       |-- guardrail_events          <- Tier 1 (pre/post)
    |       └── feedback_events           <- Tier 1 (user side)
    └── agent_tool_call_events
```

### 3.2 RAG / Retrieval Observability

**Business questions answered:**
- Which knowledge bases have low hit rates?
- Is embedding latency dragging down response time?
- Is `top_k` set appropriately for each use case?
- Which retrieval strategies (vector, hybrid, keyword) perform best?

#### DWD: `dwd_ai_retrieval_request_di`

| Field | Type | Description |
|---|---|---|
| retrieval_id | string | Unique retrieval event ID |
| trace_id | string | Cross-system trace ID |
| run_id | string | Related Agent run ID |
| span_id | string | Related retrieval span ID |
| request_id | string | Related LLM request ID (if retrieval feeds a specific call) |
| agent_id | string | Agent identifier |
| app_name | string | Application name |
| feature_name | string | Feature module |
| user_id | string | User ID |
| knowledge_base_id | string | Knowledge base / collection identifier |
| knowledge_base_name | string | Knowledge base display name |
| embedding_model | string | Embedding model used |
| retrieval_strategy | string | vector / hybrid / keyword / graph |
| query_text_hash | string | Hash of query text |
| query_length | int | Query character count |
| top_k | int | Requested number of results |
| returned_count | int | Actual number of results returned |
| hit_count | int | Number of results deemed relevant (if relevance scoring exists) |
| max_similarity_score | double | Highest similarity score among returned results |
| min_similarity_score | double | Lowest similarity score among returned results |
| avg_similarity_score | double | Average similarity score |
| embedding_latency_ms | int | Time to generate query embedding |
| search_latency_ms | int | Time to search the vector store |
| total_latency_ms | int | End-to-end retrieval latency |
| status | string | success / error |
| error_type | string | Error type if failed |
| mode | string | mock / live / replay |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

**Grain:** one row per retrieval request.

#### DWS: `dws_ai_retrieval_knowledge_base_request_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| app_name | string |
| knowledge_base_id | string |
| embedding_model | string |
| retrieval_strategy | string |

| Metrics | Type | Description |
|---|---|---|
| retrieval_count | long | Total retrieval requests |
| success_count | long | Successful retrievals |
| error_count | long | Failed retrievals |
| total_returned | long | Total documents returned |
| total_hit | long | Total relevant documents |
| avg_similarity_score | double | Average similarity score |
| avg_total_latency_ms | double | Average end-to-end retrieval latency |
| p95_total_latency_ms | long | p95 retrieval latency |
| avg_embedding_latency_ms | double | Average embedding latency |
| avg_search_latency_ms | double | Average vector search latency |

#### ADS: `ads_observability_retrieval_daily_quality`

Derived from DWS. Adds hit rate (`total_hit / total_returned`), zero-result rate, latency breach flag against configured thresholds.

#### Source Adapter

A retrieval event collector that hooks into the RAG pipeline. For the MVP, a mock generator (`scripts/generate_mock_retrieval_logs.py`) produces events consistent with the schema.

---

### 3.3 User Feedback

**Business questions answered:**
- What is the user satisfaction rate per feature/agent?
- Which features trigger the most regenerations?
- Are negative feedback events correlated with specific models or prompt versions?
- How does satisfaction trend after a prompt version change?

#### DWD: `dwd_ai_feedback_action_di`

| Field | Type | Description |
|---|---|---|
| feedback_id | string | Unique feedback event ID |
| trace_id | string | Cross-system trace ID |
| request_id | string | Related LLM request ID |
| run_id | string | Related Agent run ID |
| session_id | string | Session ID |
| conversation_id | string | Conversation ID |
| user_id | string | User ID |
| app_name | string | Application name |
| feature_name | string | Feature module |
| agent_id | string | Agent identifier |
| feedback_type | string | thumbs_up / thumbs_down / rating / regenerate / edit / report |
| rating_value | int | Numeric rating (1-5) if applicable |
| feedback_text_hash | string | Hash of optional free-text feedback |
| feedback_text_length | int | Length of free-text feedback |
| response_latency_ms | int | Latency of the response being rated |
| model_name | string | Model that produced the rated response |
| prompt_version | string | Prompt version that produced the rated response |
| mode | string | mock / live |
| environment | string | dev / staging / prod |
| created_at | timestamp | Feedback timestamp |
| date | date | Partition date |

**Grain:** one row per feedback action.

#### DWS: `dws_ai_feedback_feature_action_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| app_name | string |
| feature_name | string |
| agent_id | string |

| Metrics | Type | Description |
|---|---|---|
| feedback_count | long | Total feedback events |
| thumbs_up_count | long | Positive feedback |
| thumbs_down_count | long | Negative feedback |
| regenerate_count | long | User-triggered regenerations |
| report_count | long | Content reports |
| avg_rating | double | Average numeric rating |
| rated_request_count | long | Distinct requests that received feedback |

#### ADS: `ads_observability_feedback_daily_satisfaction`

Derived from DWS joined with `dws_ai_llm_feature_request_1d`. Adds satisfaction rate (`thumbs_up / (thumbs_up + thumbs_down)`), regeneration rate (`regenerate_count / request_count`), and trend indicators (day-over-day change).

---

### 3.4 Content Safety / Guardrails

**Business questions answered:**
- How many requests trigger content filters?
- Which guardrail rules fire most often?
- Which prompts or features repeatedly violate policies?
- What is the PII detection hit rate?
- How much latency does the guardrail layer add?

#### DWD: `dwd_ai_guardrail_check_di`

| Field | Type | Description |
|---|---|---|
| guardrail_event_id | string | Unique guardrail event ID |
| trace_id | string | Cross-system trace ID |
| request_id | string | Related LLM request ID |
| run_id | string | Related Agent run ID |
| user_id | string | User ID |
| app_name | string | Application name |
| feature_name | string | Feature module |
| guardrail_stage | string | pre_request / post_response |
| rule_name | string | Guardrail rule name |
| rule_category | string | content_filter / pii_detection / toxicity / topic_block / length_limit |
| triggered | boolean | Whether the rule was triggered |
| action_taken | string | pass / warn / block / redact / override |
| severity | string | low / medium / high / critical |
| matched_pattern_hash | string | Hash of the matched pattern (for PII, sensitive content) |
| input_text_length | int | Length of the text that was checked |
| guardrail_latency_ms | int | Time to run this guardrail check |
| model_name | string | Model associated with the request |
| prompt_version | string | Prompt version |
| mode | string | mock / live |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

**Grain:** one row per guardrail rule evaluation. A single LLM request may produce multiple rows (one per rule checked).

#### DWS: `dws_ai_guardrail_rule_check_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| app_name | string |
| rule_category | string |
| action_taken | string |

| Metrics | Type | Description |
|---|---|---|
| check_count | long | Total guardrail checks |
| triggered_count | long | Checks where rule was triggered |
| block_count | long | Requests blocked |
| redact_count | long | Responses redacted |
| warn_count | long | Warnings issued |
| avg_guardrail_latency_ms | double | Average guardrail latency |
| distinct_user_count | long | Distinct users involved |

#### ADS: `ads_observability_guardrail_daily_violation`

Derived from DWS. Flags features with high trigger rates, identifies repeat violators (by `user_id` aggregation at ADS level), tracks guardrail latency impact on overall response time.

---

### 3.5 Tier 1 Dimension Tables

| Dimension | Key | Purpose |
|---|---|---|
| `dim_knowledge_base_df` | knowledge_base_id | Knowledge base metadata: name, type, document count, last updated |
| `dim_guardrail_rule_df` | rule_name | Rule description, category, severity default, owner team |

---

### 3.6 Tier 1 SLA Rule Expansion

```yaml
rules:
  # Existing
  - scope: feature
    feature_name: chat
    p95_latency_ms_max: 3000
    error_rate_max: 0.05
  - scope: feature
    feature_name: rag_answer
    p95_latency_ms_max: 5000
    error_rate_max: 0.03

  # Tier 1 additions
  - scope: retrieval
    knowledge_base_name: product_docs
    p95_total_latency_ms_max: 2000
    zero_result_rate_max: 0.10
  - scope: feedback
    app_name: customer_support
    satisfaction_rate_min: 0.80
    regeneration_rate_max: 0.15
  - scope: guardrail
    rule_category: pii_detection
    miss_rate_max: 0.01
```

---

### 3.7 Tier 1 Implementation Files

| Category | New Files |
|---|---|
| Source adapters | `scripts/generate_mock_retrieval_logs.py`, `scripts/generate_mock_feedback_logs.py`, `scripts/generate_mock_guardrail_logs.py` |
| Domain models | `app/retrieval_event.py`, `app/feedback_event.py`, `app/guardrail_event.py` |
| Spark transforms | `scripts/spark_transform_retrieval_events.py`, `scripts/spark_transform_feedback_events.py`, `scripts/spark_transform_guardrail_events.py` |
| DWS builders | `scripts/spark_build_dws_retrieval_daily_metrics.py`, `scripts/spark_build_dws_feedback_daily_metrics.py`, `scripts/spark_build_dws_guardrail_daily_metrics.py` |
| ADS builders | `scripts/spark_build_ads_retrieval_quality.py`, `scripts/spark_build_ads_satisfaction.py`, `scripts/spark_build_ads_guardrail_violation.py` |
| Flink SQL | `flink/sql/03b_dwd_retrieval_paimon.sql`, `flink/sql/03c_dwd_feedback_paimon.sql`, `flink/sql/03d_dwd_guardrail_paimon.sql` |
| Doris DDL | Update `sql/create_doris_tables.sql` |
| Tests | `tests/test_retrieval_events.py`, `tests/test_feedback_events.py`, `tests/test_guardrail_events.py` |

---

## 4. Tier 2 — Enterprise Operations and Governance (Month 3-4)

### 4.1 Why Tier 2 Second

These domains require data from outside the AI runtime call chain: HR/org structures for team attribution, CI/CD systems for deployment events, evaluation frameworks for quality scores, and budget systems for cost governance. They need new source adapters connecting to enterprise systems.

---

### 4.2 Cost Governance

**Business questions answered:**
- How much does each team/department spend on AI per day/week/month?
- Which teams exceed their AI budget?
- How should internal chargeback be calculated?
- What is the cost trend and forecast?

#### New Dimensions

**`dim_team`**

| Field | Type | Description |
|---|---|---|
| team_id | string | Team identifier |
| team_name | string | Team display name |
| department | string | Department name |
| cost_center | string | Finance cost center code |
| budget_monthly_usd | double | Monthly AI budget |
| manager | string | Team manager |

**`dim_user`**

| Field | Type | Description |
|---|---|---|
| user_id | string | User identifier |
| user_name | string | Display name |
| team_id | string | FK to dim_team |
| role | string | User role |
| ai_access_tier | string | Access tier (basic / power / admin) |

#### DWS: `dws_ai_cost_team_request_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| team_id | string |
| app_name | string |
| model_name | string |

| Metrics | Type | Description |
|---|---|---|
| request_count | long | Total LLM requests |
| total_tokens | long | Total tokens consumed |
| estimated_cost_usd | double | Total estimated cost |
| agent_run_count | long | Total Agent runs |
| agent_cost_usd | double | Total Agent cost |

Built by joining `dwd_ai_llm_request_di.user_id` -> `dim_user_df.user_id` -> `dim_user_df.team_id`.

#### ADS: `ads_observability_cost_daily_budget`

Joins `dws_ai_cost_team_request_1d` with `dim_team_df.budget_monthly_usd`. Computes MTD spend, budget utilization rate, projected month-end spend, and budget breach flag.

#### ADS: `ads_observability_cost_monthly_chargeback`

Monthly grain. Final chargeback amount per team/cost center for finance systems.

Implementation status: `dim_team_df`, `dim_user_df`, `dws_ai_cost_team_request_1d`, and `ads_observability_cost_daily_budget` are implemented as the first Tier 2 slice. Monthly chargeback remains planned.

---

### 4.3 Evaluation / Quality

**Business questions answered:**
- What quality scores do LLM-as-judge evaluations produce?
- How does accuracy compare against ground truth?
- Which features or prompt versions produce the most hallucinations?
- How does quality change after a model or prompt version switch?

#### DWD: `dwd_ai_evaluation_judgment_di`

| Field | Type | Description |
|---|---|---|
| evaluation_id | string | Unique evaluation event ID |
| trace_id | string | Cross-system trace ID |
| request_id | string | Related LLM request ID being evaluated |
| run_id | string | Related Agent run ID being evaluated |
| app_name | string | Application name |
| feature_name | string | Feature module |
| evaluator_type | string | llm_judge / human / ground_truth / regex / classifier |
| evaluator_model | string | Model used for LLM-as-judge (if applicable) |
| evaluation_dimension | string | relevance / faithfulness / coherence / toxicity / hallucination |
| score | double | Evaluation score (0.0 - 1.0 normalized) |
| raw_score | string | Raw score before normalization |
| pass_threshold | double | Threshold for pass/fail |
| passed | boolean | Whether the evaluation passed |
| evaluated_model_name | string | Model that produced the evaluated response |
| evaluated_prompt_version | string | Prompt version of the evaluated response |
| evaluation_latency_ms | int | Evaluation execution time |
| mode | string | mock / live / offline |
| environment | string | dev / staging / prod |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

**Grain:** one row per evaluation judgment. A single request may be evaluated on multiple dimensions.

#### DWS: `dws_ai_evaluation_feature_judgment_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| app_name | string |
| feature_name | string |
| evaluation_dimension | string |
| evaluated_model_name | string |

| Metrics | Type | Description |
|---|---|---|
| evaluation_count | long | Total evaluations |
| pass_count | long | Evaluations that passed |
| fail_count | long | Evaluations that failed |
| avg_score | double | Average score |
| p10_score | double | 10th percentile score (identifies worst-case) |
| avg_evaluation_latency_ms | double | Average evaluation latency |

Implementation status: `dwd_ai_evaluation_judgment_di` and `dws_ai_evaluation_feature_judgment_1d` are implemented as the second Tier 2 slice, including mock events, Spark batch transforms, Paimon/Flink table definitions, Doris DDL and tests.

---

### 4.4 Prompt Engineering

**Business questions answered:**
- Which prompt version performs best in terms of quality, latency and cost?
- Did the latest prompt version rollout improve or regress?
- Which prompts are in A/B testing and what are the results?

#### DIM: `dim_prompt_version_df`

| Field | Type | Description |
|---|---|---|
| prompt_id | string | Prompt identifier |
| prompt_version | string | Version string |
| prompt_name | string | Display name |
| owner_team_id | string | FK to dim_team |
| status | string | draft / active / deprecated / rolled_back |
| release_date | date | When this version went active |
| ab_test_group | string | A/B test group if applicable |
| description | string | Change description |

#### DWS: `dws_ai_prompt_version_request_1d`

Upgrade of the existing ADS table. Grouping keys: `date`, `prompt_id`, `prompt_version`, `model_name`. Metrics: request_count, success_count, error_count, avg_latency_ms, p95_latency_ms, total_tokens, estimated_cost_usd, avg_evaluation_score (joined from evaluation DWS).

This table enables prompt A/B test analysis and version comparison directly.

---

### 4.5 Model Deployment

**Business questions answered:**
- Did the new model version degrade latency or error rates?
- What percentage of traffic is on the canary?
- When was the last rollback and why?

#### DWD: `dwd_ai_model_deployment_di`

| Field | Type | Description |
|---|---|---|
| deployment_id | string | Unique deployment event ID |
| model_name | string | Model being deployed |
| model_version | string | Model version |
| provider | string | Provider name |
| deployment_action | string | deploy / rollback / scale / canary_start / canary_promote / canary_abort |
| traffic_percentage | double | Percentage of traffic routed to this version |
| target_environment | string | dev / staging / prod |
| deployer_user_id | string | Who triggered the deployment |
| deploy_reason | string | Reason for deployment |
| status | string | success / failed / in_progress |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

**Grain:** one row per deployment action.

#### DIM: `dim_model_version_df`

Extends existing `dim_model_df`. Adds `model_version`, `deployment_status`, `first_deployed_at`, `last_deployed_at`, `is_current_prod`.

---

### 4.6 Tier 2 Granularity Expansions

With team and user dimensions in place, existing DWS tables gain new grouping keys:

| Existing DWS Table | New Grouping Key | New Table |
|---|---|---|
| `dws_ai_llm_feature_request_1d` | + `team_id` | `dws_ai_cost_team_request_1d` (see 4.2) |
| `dws_ai_llm_feature_request_1d` | + `environment` | `dws_ai_llm_feature_env_request_1d` |
| `dws_ai_agent_agent_run_1d` | + `team_id` | `dws_ai_agent_team_run_1d` |

---

### 4.7 Tier 2 Implementation Approach

| Domain | Source Adapter | Integration Complexity |
|---|---|---|
| Cost governance | Join existing DWD with `dim_team_df`/`dim_user_df` loaded from HR API or CSV | Low (dimension load only) |
| Evaluation | Hook into evaluation framework (LangSmith, RAGAS, custom) or batch import | Medium |
| Prompt engineering | Parse prompt registry (Git repo, config DB, or API) | Medium |
| Model deployment | Parse CI/CD events (GitHub Actions, ArgoCD, internal deploy API) | Medium-High |

---

## 5. Tier 3 — Compliance, Multi-Agent, and Platform Health (Month 5+)

### 5.1 Why Tier 3 Last

These domains become urgent only at enterprise scale: when there are regulatory audits, multi-agent orchestration workflows, or the lakehouse platform itself needs monitoring. They are the highest complexity and lowest immediate ROI domains.

---

### 5.2 Audit and Compliance

**Business questions answered:**
- Who accessed what sensitive AI data and when?
- Are prompt/response retention policies being enforced?
- Can we produce a compliance report for auditors?

#### DWD: `dwd_ai_compliance_access_audit_di`

| Field | Type | Description |
|---|---|---|
| audit_event_id | string | Unique audit event ID |
| user_id | string | User who performed the action |
| action_type | string | query / export / view_prompt / view_response / delete / admin_override |
| resource_type | string | dashboard / dwd_table / raw_log / prompt_text / response_text |
| resource_id | string | Specific resource accessed |
| ip_address | string | Source IP (hashed or masked) |
| access_granted | boolean | Whether access was allowed |
| denial_reason | string | Reason for denial if applicable |
| data_classification | string | public / internal / confidential / restricted |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

#### DWD: `dwd_ai_compliance_data_retention_di`

Tracks when data is archived, anonymized or deleted per retention policy. Fields: retention_event_id, table_name, partition_date, action (archive / anonymize / delete), rows_affected, policy_name, created_at.

---

### 5.3 Multi-Agent Orchestration

**Business questions answered:**
- What is the call topology between agents in a multi-agent workflow?
- Which inter-agent handoff is the bottleneck?
- What is the end-to-end SLA for a multi-agent task?

#### DWD: `dwd_ai_agent_orchestration_di`

| Field | Type | Description |
|---|---|---|
| orchestration_id | string | Unique orchestration event ID |
| trace_id | string | Cross-system trace ID |
| parent_run_id | string | Calling agent's run ID |
| child_run_id | string | Called agent's run ID |
| parent_agent_id | string | Calling agent ID |
| child_agent_id | string | Called agent ID |
| handoff_type | string | delegate / callback / broadcast / sequential |
| payload_size | int | Inter-agent message size |
| handoff_latency_ms | int | Time from parent dispatch to child start |
| status | string | success / error / timeout |
| created_at | timestamp | Event timestamp |
| date | date | Partition date |

**Grain:** one row per inter-agent handoff.

#### DWS: `dws_ai_agent_orchestration_handoff_1d`

Grouping: `date`, `parent_agent_id`, `child_agent_id`, `handoff_type`. Metrics: handoff_count, success_count, error_count, timeout_count, avg_handoff_latency_ms, p95_handoff_latency_ms.

---

### 5.4 Platform Health

**Business questions answered:**
- Is Kafka lagging? Is Flink checkpointing normally?
- Are Paimon tables growing too large? How many snapshots need compaction?
- Is Doris query latency degrading?

#### DWS: `dws_ai_platform_component_health_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| component | string |
| metric_name | string |

| Metrics | Type | Description |
|---|---|---|
| metric_value | double | Metric value |
| threshold | double | Alert threshold |
| is_breach | boolean | Whether threshold was breached |

Components: `kafka` (consumer lag, topic size), `flink` (checkpoint duration, restart count, backpressure), `paimon` (snapshot count, file count, table size), `doris` (query p95, compaction lag, tablet count).

Source: Kafka JMX, Flink REST API, Paimon catalog metadata, Doris `information_schema`.

---

## 6. Granularity Expansion — Cross-Tier

### 6.1 Time Granularity

| Layer | Current | Tier 1 Addition | Tier 2 Addition |
|---|---|---|---|
| DWS | Daily only | **Hourly** `dws_*_hourly_metrics` for real-time dashboards | No change |
| ADS | Daily only | No change | **Weekly/Monthly** `ads_*_weekly_trend`, `ads_*_monthly_trend` for management reports |

Hourly DWS tables use the same schema as daily DWS but with `hour` (int, 0-23) as an additional grouping key. In Flink, hourly aggregation uses a 1-hour tumble window. Daily DWS remains as a rollup from hourly.

### 6.2 Organization Granularity

```text
Company -> Business Unit -> Department -> Team -> Application -> Feature / Agent
                                                      |
                                                 User -> Session -> Conversation Turn
```

Implementation: `dim_team` and `dim_user` (Tier 2) enable team-level and user-level DWS tables. No schema change to DWD — `user_id` is already present. DWS tables add `team_id` as a grouping key by joining through `dim_user`.

### 6.3 Session / Conversation Granularity

#### DWS: `dws_ai_llm_session_request_1d`

| Grouping Keys | Type |
|---|---|
| date | date |
| app_name | string |
| feature_name | string |

| Metrics | Type | Description |
|---|---|---|
| session_count | long | Distinct sessions |
| avg_turns_per_session | double | Average conversation turns |
| avg_tokens_per_session | double | Average tokens per session |
| avg_duration_per_session_ms | double | Average session duration |
| resolved_session_count | long | Sessions marked as resolved (if feedback exists) |

Built from: `dwd_ai_llm_request_di` grouped by `session_id`, then aggregated to daily.

### 6.4 Region / Environment Granularity

Both `region` and `environment` fields exist in all DWD tables but are not DWS grouping keys.

Addition: a `dws_ai_llm_region_request_1d` table with `region` and `environment` as grouping keys, enabling multi-region comparison and prod-vs-staging analysis.

---

## 7. Complete Table Inventory After Three Tiers

### DWD Fact Tables (11 total, +7 new)

| Table | Tier | Domain |
|---|---|---|
| `dwd_ai_llm_request_di` | Existing | LLM |
| `dwd_ai_agent_run_di` | Existing | Agent |
| `dwd_ai_agent_span_di` | Existing | Agent |
| `dwd_ai_agent_tool_call_di` | Existing | Agent |
| `dwd_ai_retrieval_request_di` | Tier 1 | Retrieval |
| `dwd_ai_feedback_action_di` | Tier 1 | Feedback |
| `dwd_ai_guardrail_check_di` | Tier 1 | Guardrail |
| `dwd_ai_evaluation_judgment_di` | Tier 2 | Evaluation |
| `dwd_ai_model_deployment_di` | Tier 2 | Deployment |
| `dwd_ai_compliance_access_audit_di` | Tier 3 | Compliance |
| `dwd_ai_agent_orchestration_di` | Tier 3 | Multi-Agent |

### DWS Summary Tables (16 total, +13 new)

| Table | Tier | Domain |
|---|---|---|
| `dws_ai_llm_feature_request_1d` | Existing | LLM |
| `dws_ai_agent_agent_run_1d` | Existing | Agent |
| `dws_ai_agent_tool_tool_call_1d` | Existing | Agent |
| `dws_ai_retrieval_knowledge_base_request_1d` | Tier 1 | Retrieval |
| `dws_ai_feedback_feature_action_1d` | Tier 1 | Feedback |
| `dws_ai_guardrail_rule_check_1d` | Tier 1 | Guardrail |
| `dws_ai_llm_feature_request_1h` | Tier 1 | LLM (hourly) |
| `dws_ai_llm_session_request_1d` | Tier 1 | Session |
| `dws_ai_cost_team_request_1d` | Tier 2 | Cost |
| `dws_ai_evaluation_feature_judgment_1d` | Tier 2 | Evaluation |
| `dws_ai_prompt_version_request_1d` | Tier 2 | Prompt |
| `dws_ai_llm_region_request_1d` | Tier 2 | LLM (region) |
| `dws_ai_agent_team_run_1d` | Tier 2 | Agent (team) |
| `dws_ai_llm_feature_env_request_1d` | Tier 2 | LLM (environment) |
| `dws_ai_agent_orchestration_handoff_1d` | Tier 3 | Multi-Agent |
| `dws_ai_platform_component_health_1d` | Tier 3 | Platform |

### ADS Application Tables (9 total, +6 new)

| Table | Tier | Domain |
|---|---|---|
| `ads_observability_cost_feature_anomaly` | Existing | Cost |
| `ads_observability_sla_feature_report` | Existing | SLA |
| `ads_observability_prompt_prompt_version_metrics` | Existing | Prompt |
| `ads_observability_retrieval_daily_quality` | Tier 1 | Retrieval |
| `ads_observability_feedback_daily_satisfaction` | Tier 1 | Feedback |
| `ads_observability_guardrail_daily_violation` | Tier 1 | Guardrail |
| `ads_observability_cost_daily_budget` | Tier 2 | Cost |
| `ads_observability_cost_monthly_chargeback` | Tier 2 | Cost |
| `ads_observability_executive_weekly_summary` | Tier 2 | Management |

### DIM Dimension Tables (7 total, +6 new)

| Table | Tier | Domain |
|---|---|---|
| `dim_model_df` | Existing | Model |
| `dim_knowledge_base_df` | Tier 1 | Retrieval |
| `dim_guardrail_rule_df` | Tier 1 | Guardrail |
| `dim_team_df` | Tier 2 | Organization |
| `dim_user_df` | Tier 2 | Organization |
| `dim_prompt_version_df` | Tier 2 | Prompt |
| `dim_model_version_df` | Tier 2 | Model |

### Total: 43 tables (from 11)

---

## 8. Dependency and Execution Order

```text
Tier 1 (Month 1-2)
  |
  |-- 1a. Retrieval domain (DWD + DWS + ADS + dim)
  |-- 1b. Feedback domain (DWD + DWS + ADS)         <- can parallel with 1a
  |-- 1c. Guardrail domain (DWD + DWS + ADS + dim)  <- can parallel with 1a
  |-- 1d. Hourly DWS + Session DWS                  <- after 1a/1b/1c
  |
  v
Tier 2 (Month 3-4)
  |
  |-- 2a. dim_team_df + dim_user_df (prerequisite for all Tier 2)
  |-- 2b. Cost governance (DWS + ADS)               <- after 2a
  |-- 2c. Evaluation domain (DWD + DWS)             <- can parallel with 2b
  |-- 2d. Prompt engineering (dim + DWS upgrade)     <- can parallel with 2b
  |-- 2e. Model deployment (DWD + dim)               <- can parallel with 2b
  |-- 2f. Region/environment/team DWS variants       <- after 2a
  |
  v
Tier 3 (Month 5+)
  |
  |-- 3a. Audit and compliance (DWD)
  |-- 3b. Multi-agent orchestration (DWD + DWS)     <- can parallel with 3a
  |-- 3c. Platform health (DWS)                      <- can parallel with 3a
```

Within each tier, DWD is always built first, then DWS, then ADS. Dimensions can be built at any time since they are loaded independently.

---

## 9. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Source adapters for Tier 2 domains depend on enterprise systems (HR API, CI/CD, eval framework) that may not expose clean events | Delays Tier 2 rollout | Build mock generators first; define the DWD schema contract before integrating real sources |
| Table proliferation creates maintenance overhead | Increases pipeline failure surface | Use a shared DQ framework (already exists in `app/data_quality.py`) for all new DWD tables; automate DWS builds with a registry pattern |
| Hourly DWS tables multiply Paimon snapshot count | Storage growth and compaction pressure | Configure Paimon snapshot expiration (`snapshot.time-retained: 24h` for hourly tables) and enable async compaction |
| Team/user dimensions require PII governance | Compliance risk | Store only `user_id` and `team_id` in DWD/DWS; keep PII (names, emails) only in `dim_user_df` with access controls |
| Multi-agent orchestration events are hard to capture from existing runtimes | Tier 3 scope creep | Defer until the enterprise actually runs multi-agent workflows; treat as optional |
