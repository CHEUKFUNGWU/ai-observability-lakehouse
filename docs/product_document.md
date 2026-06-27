# AI Observability Lakehouse 产品文档

> 状态：当前实现（2026-06-21）。本文描述已交付的数据能力和明确边界，不把历史规划当作现状。

## 1. 产品定位

AI Observability Lakehouse 是面向 LLM 与 Agent 应用的数据基础设施和分析产品。它不训练模型，也不替代应用侧 tracing SDK；它把不同运行时和企业系统产生的事件统一为可审计、可聚合、可查询的数据产品，让工程、产品、数据、FinOps、安全和管理团队使用相同口径判断 AI 系统的表现。

## 2. 目标用户与决策

| 用户 | 需要做的决策 | 主要数据产品 |
|---|---|---|
| AI/平台工程师 | 定位请求、span、工具和 Agent 交接的失败与延迟 | DWD 明细、Agent/Tool DWS、Grafana |
| 产品负责人 | 判断功能使用、满意度、Prompt/模型版本影响 | Feature、Feedback、Evaluation、Prompt DWS/ADS |
| 数据工程师 | 维护跨引擎契约、catalog、回填、质量和血缘 | Gravitino、ODS/DWD/DWS、quarantine、pipeline metadata |
| FinOps/团队负责人 | 归因团队成本、预算风险和内部结算 | Cost team DWS、budget/chargeback ADS |
| 安全/合规人员 | 检查 Guardrail、访问拒绝和留存执行 | Guardrail ADS、Compliance DWD、Superset |
| 管理层 | 按周观察规模、成本、质量和风险 | Executive weekly ADS |

## 3. 核心用户场景

### 3.1 成本与容量

- 按 app、feature、model、team、region 和 environment 查看请求、token 与估算成本。
- 计算团队月累计、月末预测、预算利用率和超支标志。
- 生成按团队和 cost center 的月度 chargeback 数据。

### 3.2 可靠性与性能

- 从 Agent run 下钻到 span、LLM request 和 tool call。
- 比较成功/错误计数、重试、平均时延、p95 或明确标识的时延上界。
- 定位多 Agent handoff 的 error、timeout 和延迟瓶颈。

### 3.3 质量与体验

- 关联检索命中/零结果、用户反馈和 evaluation judgment。
- 对比 Prompt 版本在质量、时延、错误、token 和成本上的变化。
- 使用受控 dataset/experiment/variant metadata 对比 baseline 与 candidate 的质量、时延和估算成本，不提前建立完整实验域模型。
- 观察 session 的 turn、token、duration 和 positive resolution。

### 3.4 安全与治理

- 按规则类别、动作和严重度观察 Guardrail trigger/block/redact/warn。
- 记录谁对何种数据资源执行了何种操作，以及访问是否获准。
- 记录分区 archive/anonymize/delete 的留存执行证据。

### 3.5 平台运行

- 聚合 Kafka、Flink、Paimon 和 Doris 的阈值型健康指标。
- 通过 Gravitino `ai_observability` metalake 统一查看和管理 `paimon_lake` catalog 元数据。
- 用 Grafana 展示运行状态，用 Superset 展示业务、成本和治理分析。

## 4. 已交付能力

| 能力域 | 事件/事实 | 聚合或应用数据产品 | 状态 |
|---|---|---|---|
| LLM 使用 | request | feature 日/小时、session、region、env、cost/SLA | 已实现 |
| Agent runtime | run、span、tool call | agent、tool、team 日聚合 | 已实现 |
| RAG | retrieval request | knowledge-base DWS、quality ADS | 已实现 |
| 用户体验 | feedback action | feedback DWS、satisfaction ADS | 已实现 |
| 安全 | guardrail check | rule DWS、violation ADS | 已实现 |
| 质量评测 | evaluation judgment | feature judgment DWS、Prompt 关联、dataset/experiment regression ADS | 已实现 |
| 模型运营 | deployment action | model/version DIM | 已实现 |
| 成本治理 | LLM/Agent + team/user DIM | team DWS、budget/chargeback ADS | 已实现 |
| 合规 | access audit、retention action | 合规查询与仪表盘 | 已实现 |
| 多 Agent | orchestration handoff | handoff DWS | 已实现 |
| 平台健康 | component metric | component health DWS | 已实现 |
| 元数据管理 | `ai_observability` metalake + `paimon_lake` catalog | Gravitino API / Web V2 | 已实现 |
| 可视化 | Doris query products | Superset + Grafana repo-managed assets | 已实现 |

“已实现”表示仓库中存在模型/契约、转换或聚合、服务 DDL 和测试。它不表示所有外部生产系统 connector 已交付。默认 Postgres CDC 自动入口只覆盖 LLM request；其他域由 Kafka producer、应用 hook 或批量文件接入。

## 5. 产品输出

当前服务层共有 46 张物理表：12 DWD、16 DWS、7 DIM、11 ADS。重点面向用户的输出包括：

- `ads_observability_cost_daily_budget`
- `ads_observability_cost_monthly_chargeback`
- `ads_observability_cost_feature_anomaly`
- `ads_observability_sla_feature_report`
- `ads_observability_prompt_prompt_version_metrics`
- `ads_observability_retrieval_daily_quality`
- `ads_observability_feedback_daily_satisfaction`
- `ads_observability_guardrail_daily_violation`
- `ads_observability_executive_weekly_summary`
- `ads_observability_trace_health_detail`
- `ads_observability_evaluation_dataset_experiment_regression`

完整清单和粒度见[数据模型](data_model.md)，指标口径见[指标定义](metric_definitions.md)。

## 6. 体验与交付方式

| 界面 | 用途 | 交付方式 |
|---|---|---|
| Superset | AI overview、compliance、agent orchestration 分析 | 确定性 provisioning 脚本 + 版本化 ZIP bundle |
| Grafana | platform health 和运行监控 | provisioning datasource + dashboard JSON |
| Doris SQL | 下钻、验证和二次分析 | DDL、Paimon Catalog、dashboard query SQL |
| Parquet/Paimon | 数据工程开发、回填和交换 | Spark/Flink 作业 |
| Gravitino | metalake、catalog 和表元数据查看 | 幂等初始化脚本 + Web V2/API |

## 7. 成功标准

本仓库不声明虚构的生产 KPI 基线。落地环境应至少跟踪：

- 数据新鲜度：事件时间到 DWS/Doris 可查询时间。
- 完整性：source/ODS/DWD 行数差异和 quarantine 比例。
- 一致性：Spark 与 Flink 对同一窗口的计数/金额差异。
- 可用性：Gravitino catalog、Flink job/checkpoint、Doris 查询与 dashboard 健康状态。
- 可解释性：每个指标能追溯到粒度、分子、分母、窗口和源事实。
- 安全性：未授权明文、密钥和直接标识符不进入分析输出。

具体目标值应由部署环境的 SLA、数据量和合规要求确定，并进入配置或 ADR，而不是硬编码在产品描述中。

## 8. 明确非目标

- 模型训练、fine-tuning 或在线推理网关。
- 生产级 OpenTelemetry collector/trace backend 的完整替代品。
- 每个 SaaS、Agent framework、HR、CI/CD、评测平台的现成 connector。
- 多租户身份治理、企业 SSO、密钥管理、TLS 和行列级权限的生产实现。
- 云上多节点高可用、对象存储和灾备交付。
- 原始 prompt/response 的长期分析存储；敏感正文应停留在受控源/ODS。

## 9. 后续产品化优先级

下一阶段应优先解决接入和运营成熟度，而不是继续扩张表数量：

1. 为关键业务源提供稳定 producer/connector 与 schema compatibility 策略。
2. 建立数据新鲜度、quarantine 和跨引擎对账的可视化 SLO。
3. 将 demo 密钥、RBAC、审计和持久化配置升级为可部署环境模板。
4. 用真实用户旅程验证仪表盘信息架构和告警噪声。
5. 在 Gravitino catalog 基础上补齐 46 表的 owner、description、retention、lineage 和变更审计元数据。
