# 数据模型

> 状态：当前实现（2026-06-21）。字段与粒度事实来源依次为 `app/warehouse_contract.py`、Flink/Paimon SQL、Doris DDL 和 Spark 作业。

## 1. 模型原则

- 使用通用 AI observability 语义，不绑定 Dify、LangChain 或单一 Agent runtime。
- 事实表一行代表一个不可再分的业务事件；聚合表名称和主键直接表达 grain 与 period。
- ODS 保留源语义，DWD 负责类型与质量，DWS 负责复用聚合，ADS 负责具体消费，DIM 提供参考上下文。
- ID 是字符串；事件明细按 `date` 分区；日快照维度使用 `df`；日聚合使用 `1d`。
- 原始 prompt/response 等敏感大文本留在受控源/ODS，DWD 优先保存 hash、size 和统计字段。

## 2. 分层与物理实现

| 层 | 主要实现 | 语义 |
|---|---|---|
| Source/Raw | 应用事件、DeepSeek、Hermes、mock、JSONL、Postgres | 未进入仓库前的源数据 |
| ODS | Kafka topics、本地 Parquet landing | 源对齐事件 + 技术元数据，无业务聚合 |
| Metadata | Gravitino `ai_observability.paimon_lake` | 统一管理 Paimon namespace、table 和 catalog 元数据 |
| DWD | Paimon、Doris、本地 Parquet | typed、validated 行级事实 |
| DWS | Paimon、Doris、本地 Parquet | 可复用的小时/日/会话指标 |
| DIM | Doris/Paimon/Parquet snapshot | 模型、组织、Prompt、知识库和规则上下文 |
| ADS | Doris/Parquet | SLA、预算、质量、异常和管理报告 |

Gravitino 不新增仓库业务层，也不改变 44 张物理表的命名与粒度。它管理 `paimon_lake` 的元数据入口；Paimon 仍保存表数据、快照和文件。

## 3. ODS 事件入口

| ODS 表/topic | 源事件 |
|---|---|
| `ods_ai_observability_llm_request_events_di` | LLM request |
| `ods_ai_observability_agent_run_events_di` | Agent run |
| `ods_ai_observability_agent_span_events_di` | Agent span |
| `ods_ai_observability_agent_tool_call_events_di` | Tool call |
| `ods_ai_observability_retrieval_events_di` | Retrieval request |
| `ods_ai_observability_feedback_events_di` | Feedback action |
| `ods_ai_observability_guardrail_events_di` | Guardrail check |
| `ods_ai_observability_evaluation_events_di` | Evaluation judgment |
| `ods_ai_observability_model_deployment_events_di` | Model deployment action |
| `ods_ai_observability_compliance_access_audit_events_di` | Access audit event |
| `ods_ai_observability_compliance_data_retention_events_di` | Retention enforcement event |
| `ods_ai_observability_agent_orchestration_events_di` | Inter-agent handoff |
| `ods_ai_observability_platform_health_metrics_di` | Platform health observation |

默认 Postgres CDC 只自动生产第一个 LLM topic；其他 topic 需要应用 producer、采集器或显式加载流程。

Langfuse Score Event 是外部观测源的通用质量/反馈信号，不新增独立 ODS/DWD 事实表。采集适配器必须先按 `source`、`name` 和 `config` 分类：user/manual feedback 进入 Feedback Action 事件入口；evaluator/judge/test/dataset-run/automated score 进入 Evaluation Judgment 事件入口；目标缺失、分值越界、分类不明或分类冲突进入 quarantine。

## 4. DWD 事实表（12）

| 表 | 粒度 | 主 ID/关联 |
|---|---|---|
| `dwd_ai_llm_request_di` | 每个 provider request attempt result 一行 | `request_id`; `trace_id`, `run_id`, `span_id` |
| `dwd_ai_agent_run_di` | 每个端到端 Agent task/run 一行 | `run_id`, `trace_id` |
| `dwd_ai_agent_span_di` | 每个 Agent runtime span 一行 | `span_id`; `run_id`, `parent_span_id` |
| `dwd_ai_agent_tool_call_di` | 每次具体 tool invocation 一行 | `tool_call_id`; `run_id`, `span_id` |
| `dwd_ai_retrieval_request_di` | 每次 retrieval request 一行 | `retrieval_id`; `request_id`, `run_id` |
| `dwd_ai_feedback_action_di` | 每次 feedback action 一行 | `feedback_id`; `request_id`, `run_id` |
| `dwd_ai_guardrail_check_di` | 每次 guardrail rule evaluation 一行 | `guardrail_check_id`; `request_id`, `run_id` |
| `dwd_ai_evaluation_judgment_di` | 每次 evaluation judgment 一行 | `evaluation_id`; `request_id`, `run_id` |
| `dwd_ai_model_deployment_di` | 每次 model deployment action 一行 | `deployment_id`, model/version |
| `dwd_ai_compliance_access_audit_di` | 每次访问尝试一行 | `audit_event_id`, `user_id` |
| `dwd_ai_compliance_data_retention_di` | 每次分区留存动作一行 | `retention_event_id`, table/partition |
| `dwd_ai_agent_orchestration_di` | 每次 inter-agent handoff 一行 | `orchestration_id`; parent/child run |

### 4.1 核心运行时关系

```mermaid
erDiagram
    DWD_AI_AGENT_RUN_DI ||--o{ DWD_AI_AGENT_SPAN_DI : run_id
    DWD_AI_AGENT_RUN_DI ||--o{ DWD_AI_LLM_REQUEST_DI : run_id
    DWD_AI_AGENT_SPAN_DI ||--o{ DWD_AI_LLM_REQUEST_DI : span_id
    DWD_AI_AGENT_SPAN_DI ||--o{ DWD_AI_AGENT_TOOL_CALL_DI : span_id
    DWD_AI_LLM_REQUEST_DI ||--o{ DWD_AI_RETRIEVAL_REQUEST_DI : request_id
    DWD_AI_LLM_REQUEST_DI ||--o{ DWD_AI_FEEDBACK_ACTION_DI : request_id
    DWD_AI_LLM_REQUEST_DI ||--o{ DWD_AI_GUARDRAIL_CHECK_DI : request_id
    DWD_AI_LLM_REQUEST_DI ||--o{ DWD_AI_EVALUATION_JUDGMENT_DI : request_id
```

这些是逻辑关联，不是数据库外键。迟到事件、异步 evaluation/feedback 和跨系统 ID 映射必须由接入方处理。

## 5. DWS 汇总表（16）

| 表 | 粒度 |
|---|---|
| `dws_ai_llm_feature_request_1d` | 每日 app × feature × model |
| `dws_ai_llm_feature_request_1h` | 每小时 app × feature × model |
| `dws_ai_llm_session_request_1d` | 每日 app × feature 的 session 汇总 |
| `dws_ai_llm_feature_env_request_1d` | 每日 app × feature × model × environment |
| `dws_ai_llm_region_request_1d` | 每日 region × environment × app × model |
| `dws_ai_agent_agent_run_1d` | 每日 app × agent × task type |
| `dws_ai_agent_tool_tool_call_1d` | 每日 agent × tool × tool type |
| `dws_ai_agent_team_run_1d` | 每日 team × app × agent × task type |
| `dws_ai_agent_orchestration_handoff_1d` | 每日 parent agent × child agent × handoff type |
| `dws_ai_retrieval_knowledge_base_request_1d` | 每日 app × knowledge base × embedding model × strategy |
| `dws_ai_feedback_feature_action_1d` | 每日 app × feature × agent |
| `dws_ai_guardrail_rule_check_1d` | 每日 app × rule category × action |
| `dws_ai_cost_team_request_1d` | 每日 team × app × model |
| `dws_ai_evaluation_feature_judgment_1d` | 每日 app × feature × evaluation dimension × evaluated model |
| `dws_ai_prompt_version_request_1d` | 每日 prompt × version × model |
| `dws_ai_platform_component_health_1d` | 每日 component × metric |

DWS 原则上保存可直接聚合的 count、token、amount、duration、score 和 distinct count。成功率、错误率、满意度等 rate 默认由查询/ADS 使用分子和分母计算。

## 6. DIM 维度表（7）

| 表 | 粒度/用途 |
|---|---|
| `dim_model_df` | 每个模型定义快照；provider、能力、价格、deprecated 状态 |
| `dim_model_version_df` | 每个 model version 快照；部署状态和 current-prod 标志 |
| `dim_prompt_version_df` | 每个 prompt/version 快照；owner、状态、A/B group |
| `dim_team_df` | 每个团队快照；department、cost center、预算 |
| `dim_user_df` | 每个用户快照；team 归属和访问层级 |
| `dim_knowledge_base_df` | 每个知识库快照；类型、文档数和更新时间 |
| `dim_guardrail_rule_df` | 每个 guardrail rule 快照；类别、默认严重度和 owner |

## 7. ADS 应用表（9）

| 表 | 粒度/用途 |
|---|---|
| `ads_observability_cost_feature_anomaly` | feature 成本异常 |
| `ads_observability_sla_feature_report` | feature 日 SLA 结果 |
| `ads_observability_prompt_prompt_version_metrics` | Prompt 版本效果消费表 |
| `ads_observability_retrieval_daily_quality` | 检索命中、零结果和时延质量 |
| `ads_observability_feedback_daily_satisfaction` | 满意度与再生成风险 |
| `ads_observability_guardrail_daily_violation` | 规则触发、阻断和策略时延 |
| `ads_observability_cost_daily_budget` | 团队/app MTD、预测和预算 breach |
| `ads_observability_cost_monthly_chargeback` | 团队/cost center 月度分摊 |
| `ads_observability_executive_weekly_summary` | app 周度跨域管理摘要 |

## 8. 核心字段族

不同事实按适用性复用以下字段族：

| 字段族 | 典型字段 | 说明 |
|---|---|---|
| Identity | `request_id`, `run_id`, `span_id`, `trace_id` | 唯一标识和跨域关联 |
| Subject | `app_name`, `feature_name`, `agent_id`, `model_name` | 业务归属 |
| Context | `user_id`, `session_id`, `region`, `environment` | 用户和运行环境 |
| Timing | `created_at`, `start_time`, `end_time`, `date` | 事件时间与分区 |
| Outcome | `status`, `error_type`, `http_status`, `is_*` | 结果和标志 |
| Usage | `prompt_tokens`, `completion_tokens`, `total_tokens` | 模型使用量 |
| Cost | `estimated_cost_usd`, budget/chargeback amounts | 估算或财务金额 |
| Performance | `latency_ms`, `duration_ms`, `retry_count` | 性能和重试 |
| Privacy-safe payload | `*_hash`, `*_size`, `*_chars` | 避免传播原始正文 |

精确类型、nullable/default 和列顺序不要从本文复制；实现时使用共享 contract 和对应 DDL。

## 9. 时间、迟到与快照

- DWD 使用事件对应的 `date` 分区，而不是处理机器当天日期。
- Flink 聚合按 event time 和 watermark 工作；当前小时窗口使用 5 秒 watermark，真实环境应按迟到分布调整。
- 异步 feedback/evaluation 可能晚于 request 到达；session 与 Prompt 质量重算要允许回填。
- DIM 是日全量语义。历史事实重算必须固定所需快照语义，避免用当前组织/价格覆盖历史口径。

## 10. 指标约束

- `request_count = success_count + error_count` 仅在状态全集严格为 success/error 时成立。
- rate 使用安全除法：分母为 0 时返回 NULL 或明确约定值，不默认返回 0。
- 成本注明估算/实际、币种、价格版本和排除项。
- percentile 注明算法与计算层；不得把 Flink 的 `MAX` 上界标为 p95。
- 周/月汇总必须由可加和分子分母加权，不能平均日 rate。

详细公式见[指标定义](metric_definitions.md)。

## 11. 生命周期

- ODS/DWD 支持追踪与回放，应比可重建的 DWS/ADS 保留更久。
- Doris 当前动态月分区配置保留至少过去 12 个月并创建未来 3 个月。
- Kafka 本地 retention 为 48 小时，仅用于开发；生产回放窗口应与恢复目标一致。
- 删除、匿名化和归档应产生 `dwd_ai_compliance_data_retention_di` 证据事件。
