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
