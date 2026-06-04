# Metric Definitions

## 1. Request Count

### Definition

Total number of LLM requests.

### SQL

```sql
SELECT count(*) AS total_requests
FROM llm_request_events_ch;
```

---

## 2. Success Rate

### Definition

Percentage of requests with `status = 'success'`.

### SQL

```sql
SELECT
    countIf(status = 'success') / count(*) AS success_rate
FROM llm_request_events_ch;
```

---

## 3. Error Rate

### Definition

Percentage of requests with `status = 'error'`.

### SQL

```sql
SELECT
    countIf(status = 'error') / count(*) AS error_rate
FROM llm_request_events_ch;
```

---

## 4. Total Tokens

### Definition

Total input and output tokens consumed by LLM requests.

### SQL

```sql
SELECT
    sum(total_tokens) AS total_tokens
FROM llm_request_events_ch;
```

---

## 5. Prompt Tokens

### Definition

Total input tokens.

### SQL

```sql
SELECT
    sum(prompt_tokens) AS prompt_tokens
FROM llm_request_events_ch;
```

---

## 6. Completion Tokens

### Definition

Total output tokens.

### SQL

```sql
SELECT
    sum(completion_tokens) AS completion_tokens
FROM llm_request_events_ch;
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
FROM llm_request_events_ch;
```

---

## 8. Average Latency

### Definition

Average request latency in milliseconds.

### SQL

```sql
SELECT
    avg(latency_ms) AS avg_latency_ms
FROM llm_request_events_ch
WHERE status = 'success';
```

---

## 9. P95 Latency

### Definition

95th percentile request latency.

### SQL

```sql
SELECT
    quantile(0.95)(latency_ms) AS p95_latency_ms
FROM llm_request_events_ch
WHERE status = 'success';
```

---

## 10. P99 Latency

### Definition

99th percentile request latency.

### SQL

```sql
SELECT
    quantile(0.99)(latency_ms) AS p99_latency_ms
FROM llm_request_events_ch
WHERE status = 'success';
```

---

## 11. Cost by Model

### Definition

Estimated cost grouped by model.

### SQL

```sql
SELECT
    model_name,
    sum(estimated_cost_usd) AS total_cost_usd
FROM llm_request_events_ch
GROUP BY model_name
ORDER BY total_cost_usd DESC;
```

---

## 12. Error Rate by Model

### Definition

Error rate grouped by model.

### SQL

```sql
SELECT
    model_name,
    countIf(status = 'error') AS error_count,
    count(*) AS total_count,
    countIf(status = 'error') / count(*) AS error_rate
FROM llm_request_events_ch
GROUP BY model_name
ORDER BY error_rate DESC;
```

---

## 13. Token Usage by Feature

### Definition

Token usage grouped by feature.

### SQL

```sql
SELECT
    feature_name,
    sum(prompt_tokens) AS input_tokens,
    sum(completion_tokens) AS output_tokens,
    sum(total_tokens) AS total_tokens
FROM llm_request_events_ch
GROUP BY feature_name
ORDER BY total_tokens DESC;
```

---

## 14. Tool Failure Rate

### Definition

Failure rate of Agent tool calls.

### SQL

```sql
SELECT
    tool_name,
    countIf(status = 'error') AS failed_calls,
    count(*) AS total_calls,
    countIf(status = 'error') / count(*) AS failure_rate
FROM agent_tool_events_ch
GROUP BY tool_name
ORDER BY failure_rate DESC;
```

---

## 15. RAG Retrieval Latency

### Definition

Average and p95 latency of RAG retrieval.

### SQL

```sql
SELECT
    vector_db,
    avg(retrieval_latency_ms) AS avg_retrieval_latency,
    quantile(0.95)(retrieval_latency_ms) AS p95_retrieval_latency
FROM rag_retrieval_events_ch
GROUP BY vector_db
ORDER BY p95_retrieval_latency DESC;
```
