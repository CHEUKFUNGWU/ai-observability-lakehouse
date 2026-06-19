# Metric Definitions

## 1. Request Count

### Definition

Total number of LLM requests.

### SQL

```sql
SELECT count(*) AS total_requests
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 2. Success Rate

### Definition

Percentage of requests with `status = 'success'`.

### SQL

```sql
SELECT
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / count(*) AS success_rate
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 3. Error Rate

### Definition

Percentage of requests with `status = 'error'`.

### SQL

```sql
SELECT
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / count(*) AS error_rate
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 4. Total Tokens

### Definition

Total input and output tokens consumed by LLM requests.

### SQL

```sql
SELECT
    sum(total_tokens) AS total_tokens
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 5. Prompt Tokens

### Definition

Total input tokens.

### SQL

```sql
SELECT
    sum(prompt_tokens) AS prompt_tokens
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 6. Completion Tokens

### Definition

Total output tokens.

### SQL

```sql
SELECT
    sum(completion_tokens) AS completion_tokens
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 7. Estimated Cost

### Definition

Estimated API cost calculated using model pricing.

### Formula

```text
estimated_cost_usd =
prompt_tokens / 1,000,000 * input_price_per_1m_tokens
+
completion_tokens / 1,000,000 * output_price_per_1m_tokens
```

### SQL

```sql
SELECT
    sum(estimated_cost_usd) AS total_estimated_cost_usd
FROM ai_observability.dwd_ai_llm_request_di;
```

---

## 8. Average Latency

### Definition

Average request latency in milliseconds.

### SQL

```sql
SELECT
    avg(latency_ms) AS avg_latency_ms
FROM ai_observability.dwd_ai_llm_request_di
WHERE status = 'success';
```

---

## 9. P95 Latency

### Definition

95th percentile request latency.

This query is for the Doris DWD fact table, where percentile computation is supported directly.
The local Flink DWS layer stores `max_latency_ms` plus a placeholder `p95_latency_ms = 0` because the current Flink streaming SQL path uses `MAX(latency_ms)` instead of a percentile aggregate.

### SQL

```sql
SELECT
    PERCENTILE_APPROX(latency_ms, 0.95) AS p95_latency_ms
FROM ai_observability.dwd_ai_llm_request_di
WHERE status = 'success';
```

---

## 10. Cost by Model

### Definition

Estimated cost grouped by model.

### SQL

```sql
SELECT
    model_name,
    sum(estimated_cost_usd) AS total_cost_usd
FROM ai_observability.dwd_ai_llm_request_di
GROUP BY model_name
ORDER BY total_cost_usd DESC;
```

---

## 11. Error Rate by Model

### Definition

Error rate grouped by model.

### SQL

```sql
SELECT
    model_name,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_count,
    count(*) AS total_count,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / count(*) AS error_rate
FROM ai_observability.dwd_ai_llm_request_di
GROUP BY model_name
ORDER BY error_rate DESC;
```

---

## 12. Token Usage by Feature

### Definition

Token usage grouped by feature.

### SQL

```sql
SELECT
    feature_name,
    sum(prompt_tokens) AS input_tokens,
    sum(completion_tokens) AS output_tokens,
    sum(total_tokens) AS total_tokens
FROM ai_observability.dwd_ai_llm_request_di
GROUP BY feature_name
ORDER BY total_tokens DESC;
```

---

## 13. Tool Failure Rate

### Definition

Failure rate of Agent tool calls.

### SQL

```sql
SELECT
    tool_name,
    sum(error_count) AS failed_calls,
    sum(tool_call_count) AS total_calls,
    sum(error_count) / sum(tool_call_count) AS failure_rate
FROM ai_observability.dws_ai_agent_tool_tool_call_1d
GROUP BY tool_name
ORDER BY failure_rate DESC;
```

---

## 14. Resolved Session Count

### Definition

Distinct daily sessions with positive feedback. A session is resolved when it has a `thumbs_up` event or a rating value of at least 4.

Session duration is measured from the first request start to the last request completion within the same date, app, feature, and session.

---

## 15. Executive Weekly Weighted Metrics

### Definition

`ads_observability_executive_weekly_summary` is grouped by ISO week start date and application. Average request latency and evaluation score are weighted by their request/evaluation counts rather than averaging daily averages.

```text
avg_latency_ms = sum(daily_avg_latency_ms * daily_request_count) / sum(daily_request_count)
avg_evaluation_score = sum(daily_avg_score * daily_evaluation_count) / sum(daily_evaluation_count)
retrieval_hit_rate_1w = retrieval_hit_cnt_1w / retrieval_returned_cnt_1w
satisfaction_rate_1w = thumbs_up_cnt_1w / (thumbs_up_cnt_1w + thumbs_down_cnt_1w)
evaluation_pass_rate_1w = evaluation_pass_cnt_1w / evaluation_cnt_1w
total_ai_cost_amt_1w = llm_cost_amt_1w + agent_cost_amt_1w
```

---

## 16. Agent Handoff Metrics

### Definition

`dws_ai_agent_orchestration_handoff_1d` groups inter-agent handoffs by date, parent agent, child agent and handoff type.

```text
handoff_cnt_1d = count(orchestration events)
success_cnt_1d = count(status = 'success')
error_cnt_1d = count(status = 'error')
timeout_cnt_1d = count(status = 'timeout')
avg_handoff_latency_ms = avg(handoff_latency_ms)
```

The Spark batch builder calculates `p95_handoff_latency_ms` with `percentile_approx`. The current Flink streaming path uses the maximum handoff latency as a conservative proxy, consistent with the existing Flink percentile limitation.

---

## 17. Retention Enforcement Metrics

### Definition

Retention enforcement volume is derived from `dwd_ai_compliance_data_retention_di`. `rows_affected` counts rows archived, anonymized or deleted by one policy action. A zero value is valid evidence that the policy ran against an empty partition.

---

## 18. Platform Health Breach

### Definition

`dws_ai_platform_component_health_1d` keeps the maximum observed value per date, component and metric. All configured thresholds are upper bounds.

```text
metric_value = max(observed metric value during the day)
is_breach = metric_value > threshold
```

Thresholds are defined in `config/platform_health_thresholds.yaml`. A value equal to the threshold is not a breach.
