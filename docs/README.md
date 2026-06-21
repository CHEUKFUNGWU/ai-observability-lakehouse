# 文档索引

## 当前态文档

这些文档描述仓库当前实现，功能或架构变更时必须同步更新：

| 文档 | 维护内容 |
|---|---|
| [产品文档](product_document.md) | 用户、场景、能力、边界和产品化优先级 |
| [项目架构](architecture.md) | 组件职责、数据流、部署拓扑和设计边界 |
| [技术文档](technical_document.md) | 模块、处理语义、契约、质量、测试与扩展流程 |
| [数据模型](data_model.md) | 分层、44 表清单、粒度、关联和指标约束 |
| [数据血缘](data_lineage.md) | 域、作业、表和消费端血缘 |
| [指标定义](metric_definitions.md) | 指标公式、窗口、分子分母和查询口径 |
| [运行手册](runtime_runbook.md) | 启停、健康检查、仪表盘和故障恢复 |
| [Flink 作业说明](../flink/README.md) | SQL 执行顺序和 session cluster 说明 |

## 架构决策

`adr/` 记录已接受的跨组件决策。变更 Paimon/Kafka/DWS/DQ/Agent 模型或可视化技术栈时，应新增 ADR 或更新现有 ADR 的状态与后续决策。

## 参考报告

- [Benchmark results](benchmark_results.md)
- [Flink failover test report](failover_test_report.md)

报告描述特定时间和环境的结果，不应直接外推为生产 SLA。

## 历史计划与迁移记录

以下文件保留演进上下文，可能包含已完成的未来时描述、旧表名或旧架构。不要把它们作为当前状态来源：

- `upgrade_plan.md`
- `architecture_unification_plan.md`
- `three_tier_domain_expansion_plan.md`
- `dashboard_implementation_plan.md`
- `migration_clickhouse_to_doris.md`

当前状态以根目录 `README.md`、本索引的当前态文档、代码、配置、DDL 和测试为准。

## 文档维护检查

- 表名、字段、粒度与 `app/warehouse_contract.py`、SQL 和测试一致。
- 命令对应现有 Makefile target 或脚本。
- 新域同步更新数据模型、血缘和指标定义。
- 新服务同步更新架构、运行手册和 health check。
- 相对链接可解析，不引用已删除资产。
- 未接入的 connector、生产安全和 HA 能力明确标注为边界。
