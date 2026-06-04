# AI Observability Lakehouse 产品文档

## 1. 项目背景

随着企业将 LLM、RAG、Agent 和 AI Copilot 应用投入生产环境，AI 应用会持续产生大量运行日志，包括用户请求、模型调用、token 使用量、延迟、错误、工具调用、RAG 检索结果和成本数据。

如果这些数据只保存在普通 application logs 中，团队很难回答以下问题：

- 今天 AI 应用一共处理了多少请求？
- 哪些模型成本最高？
- 哪些模型延迟最高？
- 哪些功能模块错误率最高？
- Agent tool call 是否经常失败？
- RAG 检索是否变慢？
- 用户使用量是否出现异常波动？
- token 成本是否超出预算？

因此，本项目设计并实现一个面向 AI 应用的实时分析平台，用于收集、存储、分析和可视化 AI 应用运行数据。

项目核心目标是构建一个基于 DeepSeek API、Kafka、Spark、Apache Iceberg、ClickHouse 和 Superset 的 AI Observability Lakehouse，用于支撑 AI 应用的实时监控、成本分析和性能分析。

---

## 2. 项目定位

本项目不是一个模型训练项目，而是一个 AI 应用数据基础设施项目。

它关注的是：

- AI 应用上线后的运行数据如何采集
- 真实 LLM API 调用如何被记录和分析
- LLM request logs 如何进入实时数据链路
- Agent tool call 如何被监控
- RAG 检索事件如何被分析
- 如何用 lakehouse 保存长期可分析数据
- 如何用 ClickHouse 支撑低延迟 OLAP 查询
- 如何用 dashboard 展示 AI 应用的业务和技术指标

项目可以理解为：

> 一个面向 LLM / RAG / Agent 应用的实时数据分析与可观测性平台。

---

## 3. 目标用户

### 3.1 AI 产品团队

关注：

- 用户使用量
- 功能模块使用情况
- 用户活跃度
- 产品增长趋势
- 不同 AI 功能的使用占比

### 3.2 AI 工程团队

关注：

- LLM 请求延迟
- 模型错误率
- Agent tool call 成功率
- RAG 检索延迟
- 系统异常波动
- 不同模型的稳定性

### 3.3 数据团队

关注：

- 数据链路是否完整
- 数据是否可追溯
- 数据是否支持长期存储
- 数据是否支持 schema evolution
- OLAP 查询是否高效
- 指标口径是否清晰

### 3.4 财务 / 管理团队

关注：

- LLM API 调用成本
- token 成本趋势
- 不同模型的成本贡献
- 不同用户或业务线的成本分布
- AI 应用投入产出情况

---

## 4. 核心业务问题

### 4.1 使用量分析

- 每分钟 / 每小时 / 每天有多少 LLM 请求？
- 不同 app、feature、model 的请求量是多少？
- 用户活跃度是否在增长？
- 哪些功能模块使用最多？

### 4.2 性能分析

- LLM 平均延迟是多少？
- p50、p95、p99 latency 是多少？
- 哪些模型响应最慢？
- 哪些功能模块延迟最高？
- RAG retrieval 是否成为性能瓶颈？

### 4.3 成本分析

- 每天消耗多少 tokens？
- input tokens 和 output tokens 分别是多少？
- 每天估算 LLM API 成本是多少？
- 哪个模型成本最高？
- 哪些用户或业务线成本最高？

### 4.4 稳定性分析

- 请求成功率是多少？
- 错误率是否异常上升？
- 常见 error type 有哪些？
- timeout、rate limit、server error 分别占多少？
- 哪些模型或功能模块错误率最高？

### 4.5 Agent 行为分析

- 每个 request 平均触发多少次 tool call？
- 哪些 tool 使用最多？
- 哪些 tool 最容易失败？
- 哪些 tool latency 最高？
- Agent 是否存在过多 retry？

### 4.6 RAG 检索分析

- 每次 retrieval 平均耗时多少？
- top_k 设置是否合理？
- 平均 retrieved_docs 数量是多少？
- fallback rate 是否过高？
- retrieval latency 是否影响整体响应速度？

---

## 5. 项目范围

### 5.1 MVP 范围

第一版项目需要完成以下功能：

1. 支持真实 DeepSeek API 调用
2. 记录每次调用的 request、response、token usage、latency 和 error
3. 生成模拟 LLM request log 数据，用于大数据量测试
4. 使用 Spark 清洗和写入 Apache Iceberg 表
5. 使用 ClickHouse 查询 Iceberg 表
6. 实现核心分析 SQL
7. 构建基础 dashboard
8. 编写项目 README、架构文档和数据模型文档

MVP 主要分析对象是：

```text
llm_request_events
```

核心指标包括：

- request count
- success rate
- error rate
- total tokens
- estimated cost
- average latency
- p95 latency
- model-level cost
- feature-level usage

### 5.2 进阶范围

第二阶段加入：

1. Kafka 实时事件流
2. Spark Structured Streaming
3. Agent tool call events
4. RAG retrieval events
5. 更完整的 dashboard
6. ClickHouse query benchmark
7. Iceberg schema evolution demo
8. Iceberg snapshot / time travel demo

### 5.3 暂不包含范围

本项目暂不包含：

- 模型训练
- 模型微调
- 真实用户隐私数据
- 复杂权限系统
- 生产级 Kubernetes 部署
- 企业级监控告警系统

---

## 6. 功能需求

### 6.1 真实 API 调用模块

系统需要支持调用 DeepSeek API，并记录真实调用行为。

记录字段包括：

- request_id
- prompt_text
- response_text
- model_name
- provider
- prompt_tokens
- completion_tokens
- total_tokens
- latency_ms
- status
- error_type
- created_at

### 6.2 模拟数据生成模块

系统需要支持生成大规模 mock events，用于 dashboard 和 benchmark。

mock events 用于：

- 生成 1M / 10M 规模数据
- 测试 ClickHouse 查询性能
- 测试 dashboard 展示效果
- 模拟错误率、延迟、成本等分布

### 6.3 数据湖存储模块

系统使用 Apache Iceberg 作为 lakehouse table format。

Iceberg 表需要支持：

- partitioned storage
- schema evolution
- snapshot tracking
- historical query
- multi-engine access

### 6.4 OLAP 查询模块

系统使用 ClickHouse 读取 Iceberg 表并进行低延迟分析查询。

需要支持的查询包括：

- 请求量趋势
- token 使用量趋势
- 成本趋势
- 模型延迟排名
- 模型错误率排名
- feature 使用量排名
- 用户使用量排名

### 6.5 Dashboard 模块

Dashboard 用于展示 AI 应用运行情况。

建议使用 Superset。

Dashboard 页面包括：

#### Page 1: AI App Overview

核心卡片：

- Total Requests
- Success Rate
- Total Tokens
- Estimated Cost
- P95 Latency

趋势图：

- Requests Over Time
- Tokens Over Time
- Cost Over Time
- Error Rate Over Time

#### Page 2: Model Performance

图表：

- Requests by Model
- Average Latency by Model
- P95 Latency by Model
- Error Rate by Model
- Cost by Model

#### Page 3: Prompt Category Analysis

图表：

- Latency by Prompt Category
- Cost by Prompt Category
- Token Usage by Prompt Category
- Average Output Length by Category

#### Page 4: Agent Monitoring

进阶阶段实现。

图表：

- Tool Calls by Tool
- Tool Success Rate
- Tool Failure Rate
- Tool Latency
- Retry Count

#### Page 5: RAG Monitoring

进阶阶段实现。

图表：

- Retrieval Latency
- Average Retrieved Docs
- Fallback Rate
- top_k Distribution
- Retrieval Latency vs LLM Latency

---

## 7. 用户故事

### User Story 1: AI 产品经理查看使用量

作为 AI 产品经理，  
我希望查看不同功能模块的请求量，  
以便判断哪些 AI 功能最受用户欢迎。

验收标准：

- 可以按 feature_name 查看 request count
- 可以按日期查看趋势
- 可以筛选 app_name 和 model_name

### User Story 2: AI 工程师排查延迟问题

作为 AI 工程师，  
我希望查看不同模型和功能模块的 p95 latency，  
以便定位性能瓶颈。

验收标准：

- 可以查看整体 p95 latency
- 可以按 model_name 分组
- 可以按 feature_name 分组
- 可以查看 latency over time

### User Story 3: 数据工程师验证数据链路

作为数据工程师，  
我希望检查从 raw event 到 Iceberg table 再到 ClickHouse query 的完整链路，  
以便保证数据可追溯和指标可信。

验收标准：

- raw event 有 request_id
- Iceberg table 保留 request_id
- ClickHouse 查询结果可回溯到原始数据
- README 中说明数据流向

### User Story 4: 管理者查看成本

作为管理者，  
我希望查看每天的 LLM API 估算成本，  
以便控制 AI 应用运营成本。

验收标准：

- 可以查看 total cost
- 可以按 model_name 查看成本
- 可以按 feature_name 查看成本
- 可以查看 cost over time

---

## 8. 非功能需求

### 8.1 可扩展性

系统应该可以从单表扩展到多事件表，包括：

- LLM request events
- Agent tool events
- RAG retrieval events
- user feedback events
- model evaluation events

### 8.2 可维护性

项目代码应保持清晰目录结构：

```text
scripts/
sql/
docs/
notebooks/
docker/
```

### 8.3 可演示性

项目需要可以通过 README 快速启动。

最低要求：

```bash
docker compose up -d
python scripts/run_deepseek_live_calls.py
python scripts/generate_mock_llm_logs.py
spark-submit scripts/spark_write_iceberg.py
clickhouse-client < sql/clickhouse_queries.sql
```

### 8.4 可解释性

项目需要在文档中解释：

- 为什么使用 Iceberg
- 为什么使用 ClickHouse
- 为什么使用 Spark
- 为什么 Kafka 是进阶模块
- 为什么同时支持 mock / live / replay
- 为什么这个项目适合 AI 应用 observability

---

## 9. 成功标准

MVP 成功标准：

- 能成功调用 DeepSeek API 并记录真实 events
- 能生成至少 100 万条模拟 LLM request logs
- 能用 Spark 写入 Iceberg 表
- 能用 ClickHouse 查询 Iceberg 表
- 能计算核心指标
- 能展示 dashboard
- GitHub README 完整
- 有架构图、数据模型和 SQL 示例

进阶成功标准：

- Kafka 实时写入链路可运行
- Spark Structured Streaming 可写入 Iceberg
- Agent tool events 可分析
- RAG retrieval events 可分析
- 有 ClickHouse 查询性能测试
- 有 Iceberg schema evolution 示例
