# AI Observability Lakehouse 技术文档

## 1. 技术目标

本项目的技术目标是构建一个面向 AI 应用运行日志的流批一体 lakehouse。

当前主线已经从单纯 Spark 批处理升级为：

```text
Flink CDC
→ Flink SQL
→ Apache Paimon ODS / DWD / ADS
→ Spark batch backfill / validation
→ ClickHouse serving
```

早期章节中提到的 Iceberg / Spark Structured Streaming 可作为候选方案或历史设计背景；当前优先落地的是 Paimon + Flink SQL 的流批一体路径。

系统需要支持：

- 真实 DeepSeek API 调用采集
- 模拟 AI 应用事件生成
- Flink CDC 实时采集 operational source table 变更
- Flink SQL 实时构建 ODS / DWD / ADS
- Apache Paimon 流批一体表存储
- Spark 批处理回补、历史重算与校验
- ClickHouse OLAP 查询
- Dashboard 可视化
- AI observability 指标分析

核心技术栈：

```text
Python
DeepSeek API
Kafka
Apache Flink CDC
Apache Flink SQL
Apache Paimon
Apache Spark
MinIO / Local Storage
ClickHouse
Superset
Docker Compose
```

---

## 2. 总体架构

### 2.1 MVP 架构

```text
+----------------------------+
| DeepSeek API / Mock Events |
+-------------+--------------+
              |
              v
+----------------------------+
| Raw JSONL Events           |
+-------------+--------------+
              |
              v
+----------------------------+
| Apache Spark Batch         |
+-------------+--------------+
              |
              v
+----------------------------+
| Apache Iceberg Tables      |
+-------------+--------------+
              |
              v
+----------------------------+
| ClickHouse Query Layer     |
+-------------+--------------+
              |
              v
+----------------------------+
| Superset Dashboard         |
+----------------------------+
```

### 2.2 进阶架构

```text
+----------------------------+
| DeepSeek API / Mock Events |
+-------------+--------------+
              |
              v
+----------------------------+
| Kafka Topics               |
+-------------+--------------+
              |
              v
+----------------------------+
| Spark Structured Streaming |
+-------------+--------------+
              |
              v
+----------------------------+
| Apache Iceberg Tables      |
+-------------+--------------+
              |
              v
+----------------------------+
| ClickHouse Query Layer     |
+-------------+--------------+
              |
              v
+----------------------------+
| Superset / Grafana         |
+----------------------------+
```

---

## 3. 技术选型

### 3.1 Apache Iceberg

Iceberg 用作 lakehouse table format。

选择原因：

- 支持大规模分析表
- 支持 schema evolution
- 支持 partition evolution
- 支持 snapshot
- 支持 time travel
- 支持 Spark、ClickHouse 等多引擎访问
- 适合长期保存 AI 应用事件数据

在本项目中，Iceberg 是长期事实表存储层。

### 3.2 Apache Spark

Spark 用于数据清洗、转换和写入 Iceberg。

选择原因：

- Spark 对 Iceberg 支持成熟
- 适合批处理和流式处理
- 适合处理大规模 JSON / Parquet 数据
- 可以实现数据质量检查、字段转换和指标预聚合

在本项目中，Spark 负责：

- 读取 raw logs
- 清洗字段
- 衍生日期分区字段
- 计算 estimated_cost_usd
- 写入 Iceberg table

### 3.3 ClickHouse

ClickHouse 用于高速 OLAP 查询。

选择原因：

- 适合高性能聚合查询
- 支持实时分析场景
- SQL 表达能力强
- 适合 dashboard 后端查询
- 可以读取 Iceberg 数据或将数据物化到 MergeTree

在本项目中，ClickHouse 负责：

- 请求量分析
- 延迟分析
- 成本分析
- 错误率分析
- 模型表现分析
- dashboard 查询加速

### 3.4 Kafka

Kafka 用于进阶阶段的实时事件流。

选择原因：

- 适合收集实时事件
- 支持高吞吐消息传输
- 解耦 AI application 和 analytics system
- 适合模拟生产环境 event-driven architecture

MVP 阶段可以先不引入 Kafka，降低复杂度。

### 3.5 Superset

Superset 用于 dashboard 可视化。

选择原因：

- SQL-friendly
- 适合 BI dashboard
- 支持 ClickHouse 连接
- 适合展示项目成果

---

## 4. Workload Modes

系统支持三种模式：

### 4.1 Mock Mode

用途：

- 生成大规模模拟数据
- 测试 Iceberg 写入能力
- 测试 ClickHouse 查询性能
- 制作 dashboard demo

### 4.2 Live Mode

用途：

- 调用真实 DeepSeek API
- 记录真实 latency
- 记录真实 token usage
- 记录真实 error behavior
- 验证真实 AI workload observability

### 4.3 Replay Mode

用途：

- 读取历史保存的真实 event
- 重复写入 pipeline
- 保证实验可复现
- 避免重复消耗 API 成本

---

## 5. 数据流设计

### 5.1 MVP 数据流

```text
Step 1: run_deepseek_live_calls.py / generate_mock_llm_logs.py
生成真实或模拟 LLM request events

Step 2: raw JSONL files
保存到 data/raw/live_llm_requests/ 或 data/raw/mock_llm_requests/

Step 3: spark_write_iceberg.py
Spark 读取 raw files，清洗和转换数据

Step 4: Iceberg table
写入 ai_analytics.llm_request_events

Step 5: ClickHouse
查询 Iceberg table 或物化到 MergeTree table

Step 6: Superset
展示 dashboard
```

### 5.2 进阶数据流

```text
Step 1: run_deepseek_live_calls.py / generate_mock_llm_logs.py
持续生成事件

Step 2: Kafka producer
写入 Kafka topic

Step 3: Spark Structured Streaming
消费 Kafka topic

Step 4: Iceberg sink
流式写入 Iceberg table

Step 5: ClickHouse query layer
分析 Iceberg 数据

Step 6: Dashboard
准实时展示指标
```

---

## 6. DeepSeek API 调用设计

### 6.1 环境变量

```bash
export DEEPSEEK_API_KEY="your_api_key"
export DEEPSEEK_MODEL="deepseek-chat"
export DEEPSEEK_BASE_URL="https://api.deepseek.com"
export LLM_MODE="live"
```

### 6.2 调用逻辑

每次调用需要记录：

- request start time
- request end time
- latency_ms
- model_name
- prompt_text
- response_text
- usage.prompt_tokens
- usage.completion_tokens
- usage.total_tokens
- status
- error_type

### 6.3 Python 示例

```python
import os
import time
import uuid
import json
from datetime import datetime, timezone
from openai import OpenAI

client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
)

MODEL_NAME = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

def call_deepseek(prompt: str, feature_name: str = "general_chat") -> dict:
    request_id = str(uuid.uuid4())
    start_time = time.time()

    status = "success"
    error_type = None
    response_text = None
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a helpful data engineering assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2
        )

        response_text = response.choices[0].message.content

        if response.usage:
            prompt_tokens = response.usage.prompt_tokens or 0
            completion_tokens = response.usage.completion_tokens or 0
            total_tokens = response.usage.total_tokens or 0

    except Exception as e:
        status = "error"
        error_type = type(e).__name__

    latency_ms = int((time.time() - start_time) * 1000)

    event = {
        "request_id": request_id,
        "user_id": "test_user_001",
        "session_id": "session_live_test",
        "app_name": "ai-observability-demo",
        "feature_name": feature_name,
        "model_name": MODEL_NAME,
        "provider": "deepseek",
        "prompt_text": prompt,
        "response_text": response_text,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "latency_ms": latency_ms,
        "status": status,
        "error_type": error_type,
        "mode": "live",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    return event
```

---

## 7. 数据模型设计

### 7.1 Database

```text
Catalog: local / rest
Namespace: ai_analytics
```

### 7.2 Table: llm_request_events

每一行代表一次 LLM request。

```sql
CREATE TABLE local.ai_analytics.llm_request_events (
    request_id STRING,
    user_id STRING,
    session_id STRING,
    app_name STRING,
    feature_name STRING,
    prompt_category STRING,
    prompt_id STRING,
    prompt_version STRING,
    model_name STRING,
    provider STRING,

    prompt_text STRING,
    response_text STRING,

    prompt_tokens INT,
    completion_tokens INT,
    total_tokens INT,

    latency_ms INT,
    status STRING,
    error_type STRING,
    http_status INT,

    estimated_cost_usd DOUBLE,

    mode STRING,
    region STRING,
    environment STRING,
    created_at TIMESTAMP,
    date DATE
)
USING iceberg
PARTITIONED BY (date);
```

### 7.3 Table: agent_tool_events

每一行代表一次 Agent tool call。

```sql
CREATE TABLE local.ai_analytics.agent_tool_events (
    tool_call_id STRING,
    request_id STRING,
    user_id STRING,
    tool_name STRING,
    tool_type STRING,
    input_size INT,
    output_size INT,
    latency_ms INT,
    status STRING,
    error_type STRING,
    retry_count INT,
    created_at TIMESTAMP,
    date DATE
)
USING iceberg
PARTITIONED BY (date);
```

### 7.4 Table: rag_retrieval_events

每一行代表一次 RAG retrieval。

```sql
CREATE TABLE local.ai_analytics.rag_retrieval_events (
    retrieval_id STRING,
    request_id STRING,
    user_id STRING,
    query_text STRING,
    embedding_model STRING,
    vector_db STRING,
    top_k INT,
    retrieved_docs INT,
    retrieval_latency_ms INT,
    fallback_used BOOLEAN,
    created_at TIMESTAMP,
    date DATE
)
USING iceberg
PARTITIONED BY (date);
```

---

## 8. Pricing Table 设计

为了避免把模型价格硬编码在代码里，建议建立模型价格配置表。

```sql
CREATE TABLE local.ai_analytics.model_pricing (
    provider STRING,
    model_name STRING,
    input_price_per_1m_tokens DOUBLE,
    output_price_per_1m_tokens DOUBLE,
    effective_from DATE,
    effective_to DATE
)
USING iceberg;
```

成本计算公式：

```python
estimated_cost_usd = (
    prompt_tokens / 1_000_000 * input_price_per_1m_tokens
    + completion_tokens / 1_000_000 * output_price_per_1m_tokens
)
```

注意：

> 实际价格应以 DeepSeek 官方价格页面为准。项目中价格表仅用于成本估算和工程演示。

---

## 9. Spark 处理逻辑

### 9.1 Batch Job

文件：

```text
scripts/spark_write_iceberg.py
```

主要步骤：

```text
1. 初始化 SparkSession
2. 配置 Iceberg catalog
3. 读取 raw JSONL files
4. 校验字段类型
5. 衍生 total_tokens
6. 衍生 date 字段
7. 根据 model_pricing 计算 estimated_cost_usd
8. 写入 Iceberg table
```

### 9.2 SparkSession 配置示例

```python
spark = (
    SparkSession.builder
    .appName("AI Observability Lakehouse")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "data/warehouse")
    .getOrCreate()
)
```

### 9.3 写入 Iceberg

```python
df.writeTo("local.ai_analytics.llm_request_events").append()
```

或者首次创建：

```python
df.writeTo("local.ai_analytics.llm_request_events").createOrReplace()
```

---

## 10. ClickHouse 查询设计

### 10.1 查询方式一：ClickHouse 直接读取 Iceberg

适合展示 ClickHouse 与 Iceberg 集成。

```sql
SELECT *
FROM icebergLocal('/path/to/warehouse/ai_analytics/llm_request_events')
LIMIT 10;
```

### 10.2 查询方式二：将 Iceberg 数据同步到 ClickHouse MergeTree

适合性能更好的 dashboard 查询。

```sql
CREATE TABLE llm_request_events_ch
(
    request_id String,
    user_id String,
    session_id String,
    app_name String,
    feature_name String,
    prompt_category String,
    prompt_id String,
    prompt_version String,
    model_name String,
    provider String,
    prompt_tokens UInt32,
    completion_tokens UInt32,
    total_tokens UInt32,
    latency_ms UInt32,
    status String,
    error_type Nullable(String),
    estimated_cost_usd Float64,
    mode String,
    region String,
    environment String,
    created_at DateTime,
    date Date
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (date, app_name, model_name, created_at);
```

实际项目中可以同时展示两种方案：

```text
Query-in-place: ClickHouse directly queries Iceberg
Materialized analytics: data loaded into ClickHouse MergeTree
```

---

## 11. 核心 SQL

### Total Requests

```sql
SELECT count(*) AS total_requests
FROM llm_request_events_ch;
```

### Requests Over Time

```sql
SELECT
    toStartOfHour(created_at) AS hour,
    count(*) AS request_count
FROM llm_request_events_ch
GROUP BY hour
ORDER BY hour;
```

### Success Rate

```sql
SELECT
    countIf(status = 'success') / count(*) AS success_rate
FROM llm_request_events_ch;
```

### Error Rate by Model

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

### P95 Latency by Model

```sql
SELECT
    model_name,
    quantile(0.95)(latency_ms) AS p95_latency_ms
FROM llm_request_events_ch
GROUP BY model_name
ORDER BY p95_latency_ms DESC;
```

### Cost by Model

```sql
SELECT
    model_name,
    sum(estimated_cost_usd) AS total_cost_usd
FROM llm_request_events_ch
GROUP BY model_name
ORDER BY total_cost_usd DESC;
```

### Token Usage by Feature

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

### Latency Distribution

```sql
SELECT
    model_name,
    quantile(0.50)(latency_ms) AS p50_latency,
    quantile(0.95)(latency_ms) AS p95_latency,
    quantile(0.99)(latency_ms) AS p99_latency
FROM llm_request_events_ch
GROUP BY model_name
ORDER BY p95_latency DESC;
```

---

## 12. Dashboard 设计

### 12.1 Overview Dashboard

指标卡片：

```text
Total Requests
Success Rate
Total Tokens
Estimated Cost
P95 Latency
```

图表：

```text
Requests Over Time
Error Rate Over Time
Cost Over Time
Requests by Model
Cost by Model
```

### 12.2 Model Performance Dashboard

```text
P95 Latency by Model
Average Latency by Model
Error Rate by Model
Token Usage by Model
Cost by Model
```

### 12.3 Prompt Category Dashboard

```text
Latency by Prompt Category
Cost by Prompt Category
Token Usage by Prompt Category
Output Length by Prompt Category
```

### 12.4 Agent Monitoring Dashboard

进阶阶段：

```text
Tool Calls by Tool
Tool Failure Rate
Tool P95 Latency
Average Retry Count
Slowest Tool Calls
```

### 12.5 RAG Monitoring Dashboard

进阶阶段：

```text
Retrieval Latency Over Time
Fallback Rate
Retrieved Docs Distribution
top_k Distribution
Retrieval Latency by Vector DB
```

---

## 13. 数据质量设计

### 13.1 基础校验

Spark 写入前需要检查：

```text
request_id 不为空
created_at 不为空
prompt_tokens >= 0
completion_tokens >= 0
total_tokens = prompt_tokens + completion_tokens
latency_ms > 0
status in ('success', 'error')
estimated_cost_usd >= 0
mode in ('mock', 'live', 'replay')
```

### 13.2 异常数据处理

| 异常情况 | 处理方式 |
|---|---|
| request_id 为空 | 丢弃或写入 quarantine table |
| token 为负数 | 标记为 invalid |
| latency 为空 | 使用 null，不参与 latency 指标 |
| status 非法 | 映射为 unknown |
| created_at 为空 | 丢弃 |
| cost 计算失败 | 设置为 0 并记录 error |

### 13.3 Quarantine Table

进阶可增加：

```text
ai_analytics.invalid_llm_request_events
```

用于保存异常数据，方便排查数据质量问题。

---

## 14. 分区设计

### 14.1 Iceberg Partition

MVP：

```text
PARTITIONED BY (date)
```

原因：

- 大多数 dashboard 按日期过滤
- 实现简单
- 方便 time-series analytics

进阶可考虑：

```text
PARTITIONED BY (days(created_at), app_name)
```

但不建议过早引入复杂分区。

### 14.2 ClickHouse Partition

```sql
PARTITION BY date
ORDER BY (date, app_name, model_name, created_at)
```

原因：

- 按日期查询频繁
- app_name 和 model_name 是常见过滤字段
- created_at 支持时间范围扫描

---

## 15. Benchmark 设计

### 15.1 数据量

```text
100,000 rows
1,000,000 rows
10,000,000 rows
```

### 15.2 测试 SQL

```text
Total request count
Requests by hour
P95 latency by model
Cost by model
Error rate by feature
```

### 15.3 对比对象

```text
Spark SQL on Iceberg
ClickHouse direct query on Iceberg
ClickHouse MergeTree table
```

### 15.4 记录内容

```text
query name
engine
row count
execution time
notes
```

示例：

| Query | Engine | Rows | Time | Notes |
|---|---|---:|---:|---|
| p95 latency by model | Spark SQL | 1M | 3.2s | baseline |
| p95 latency by model | ClickHouse Iceberg | 1M | 1.1s | query-in-place |
| p95 latency by model | ClickHouse MergeTree | 1M | 0.2s | materialized |

---

## 16. Iceberg 功能演示

### 16.1 Schema Evolution

```sql
ALTER TABLE local.ai_analytics.llm_request_events
ADD COLUMN user_plan STRING;
```

说明：

```text
AI 产品上线后，可能新增用户订阅等级字段，例如 free、pro、enterprise。
Iceberg 支持在不重写全表的情况下演进 schema。
```

### 16.2 Snapshot

```sql
SELECT *
FROM local.ai_analytics.llm_request_events.snapshots;
```

说明：

```text
每次写入 Iceberg 表都会产生新的 snapshot。
这可以用于审计、回溯和历史分析。
```

### 16.3 Time Travel

```sql
SELECT *
FROM local.ai_analytics.llm_request_events
VERSION AS OF <snapshot_id>;
```

说明：

```text
可以查询历史版本数据，适合排查错误写入或指标异常。
```

---

## 17. 开发路线图

### Phase 1: Real API Minimum Loop

```text
1. 创建项目结构
2. 编写 deepseek_client.py
3. 编写 run_deepseek_live_calls.py
4. 调用 DeepSeek API 并保存 JSONL
5. 编写 Spark job
6. 创建 Iceberg table
7. 写入 Iceberg
8. 用 ClickHouse 查询
9. 编写核心 SQL
10. 完成 README
```

### Phase 2: Large-Scale Mock Data

```text
1. 编写 generate_mock_llm_logs.py
2. 生成 1M / 10M mock events
3. 写入 Iceberg
4. 建立 ClickHouse MergeTree table
5. 做 query benchmark
6. 建立 dashboard
```

### Phase 3: Multi-Event Observability

```text
1. 增加 agent_tool_events
2. 增加 rag_retrieval_events
3. 建立 request_id 关联关系
4. 增加 tool call 分析 SQL
5. 增加 RAG retrieval 分析 SQL
6. 扩展 dashboard
```

### Phase 4: Kafka Streaming

```text
1. 部署 Kafka
2. 编写 Kafka producer
3. 创建 Kafka topics
4. 编写 Spark Structured Streaming job
5. 流式写入 Iceberg
6. 验证准实时 dashboard
```
