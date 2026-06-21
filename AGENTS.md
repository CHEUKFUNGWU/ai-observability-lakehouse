# Repository Working Agreement

本文件是本仓库代码、SQL、测试和文档变更的执行约束。事实优先级为：运行代码与配置 → `app/warehouse_contract.py` → SQL DDL/DML → 测试 → 当前态文档 → 历史计划。

## Repository Map

| 路径 | 责任 |
|---|---|
| `app/` | 事件模型、跨引擎字段/粒度契约、数据质量和共享逻辑 |
| `scripts/` | 数据生成/采集、Spark 转换聚合、Doris 加载和运维工具 |
| `flink/sql/` | Catalog、Kafka ODS、Paimon DWD/DWS、流式 DML 和验证 SQL |
| `sql/` | Postgres 源表、Doris DDL、Paimon Catalog、同步和仪表盘查询 |
| `config/` | SLA、平台健康阈值、Superset/Grafana 可复现资产 |
| `docs/` | 当前态文档、运行手册、指标、血缘、ADR 和历史计划 |
| `tests/` | 业务逻辑、跨引擎契约、SQL/仪表盘资产与集成测试 |

## Change Workflow

1. 先确认目标表的层、粒度、主键、时间字段和保留策略。
2. 优先在 `app/warehouse_contract.py` 维护可共享的字段和粒度契约，避免 Spark、Flink、Doris 各自手写漂移。
3. 同一变更必须同步更新事件模型、DDL/DML、Spark/Flink 作业、Doris loader、测试和当前态文档。
4. 新增业务域时补充 `docs/data_model.md` 与 `docs/data_lineage.md`；新增跨组件决策时补 ADR。
5. 新增/改名 Paimon database 或 table 时，同步验证 Gravitino `ai_observability.paimon_lake` catalog 可发现该对象；不要维护平行的 catalog 命名。
6. 运行最小相关测试，跨层或改名变更运行全量 `uv run pytest -v`。
7. 不把 `docs/*_plan.md` 当作当前实现证据；计划完成后同步当前态文档。

常用验证命令：

```bash
uv run pytest -v
uv run pytest tests/test_warehouse_contract.py tests/test_doris_schema.py tests/test_flink_sql_assets.py -v
uv run pytest tests/test_gravitino_assets.py -v
make health
```

需要 Paimon 运行时的测试带有 `paimon` marker；本地基础设施操作见 `docs/runtime_runbook.md`。

## Warehouse Layers

| Prefix | Layer | Purpose |
|---|---|---|
| `ods` | Operational data store / raw landing | 保留源对齐事件和技术元数据，不计算业务指标 |
| `dwd` | Detail warehouse data | 类型化、校验后的行级业务事实 |
| `dwm` | Detail wide model | 仅在确有需要时提供轻量整合宽明细 |
| `dws` | Summary warehouse data | 可复用的主题与指标聚合 |
| `ads` | Application data service | 报表、仪表盘、告警和应用专用数据集市 |
| `dim` | Dimension | 参考维度和以全量快照为主的维表 |
| `tmp` | Temporary | 短期开发/调试表 |
| `view` | View | 已有表的逻辑封装 |
| `_bak` | Backup suffix | 临时或历史备份表后缀 |

## Project Naming Conventions

- 一级数据域：`ai`。
- 二级域使用稳定业务域，如 `llm`、`agent`、`retrieval`、`feedback`、`guardrail`、`evaluation`、`compliance`、`platform`。
- 本地 Postgres CDC 源库：`ai_observability`。
- 日增量明细使用 `di`，日全量维度使用 `df`，日聚合使用 `1d`。
- 即使数据库/schema 已表示层级，物理表名仍必须带层前缀，保证在 Paimon、Doris、本地 Parquet、测试和文档间可移植。

### Table Naming

```text
ODS: ods_{source_database}_{source_table}_{storage_strategy}
DWD: dwd_{domain}_{subdomain}_{business_process}_{storage_strategy}
DWS: dws_{domain}_{subdomain}_{grain}_{business_process}_{period}
ADS: ads_{application_theme}_{subtheme}_{grain}_{business_process}
DIM: dim_{dimension_definition}_{storage_strategy}
TMP: tmp_{table_name}_{sequence_or_date}
VIEW: view_{table_name}
BACKUP: {table_name}_bak
```

规范示例：

```text
ods_ai_observability_llm_request_events_di
dwd_ai_llm_request_di
dwd_ai_agent_tool_call_di
dws_ai_llm_feature_request_1d
dws_ai_agent_tool_tool_call_1d
ads_observability_cost_feature_anomaly
dim_model_df
```

### Storage Strategy

| Suffix | Meaning |
|---|---|
| `df` | Day full snapshot |
| `di` | Day incremental |
| `hf` | Hour full snapshot |
| `hi` | Hour incremental |

## Field Naming

- ID 使用字符串：`request_id`、`run_id`、`span_id`、`tool_call_id`、`user_id`。
- 布尔标志使用 `is_{meaning}`；支持布尔的引擎使用 `BOOLEAN`，否则用 `BIGINT`（1/0）。
- 枚举使用 `{meaning}_type`，如 `request_type`、`tool_type`、`error_type`。
- 日期使用 `{meaning}_date`；时间戳使用 `{meaning}_time` 或稳定事件名，如 `created_at`、`start_time`、`end_time`。
- 新计数指标使用 `{metric}_cnt_{period}`。已有 `_count` 字段仅在改名会造成广泛兼容成本时保留；新 DWS/ADS 优先 `_cnt_1d`、`_cnt_30d`、`_cnt_td`。
- 金额使用 `{metric}_amt_{period}`。精确货币金额用 `DECIMAL`；估算分析成本可用 `DOUBLE`。
- 比率使用 `{metric}_rate_{period}`，原则上在查询/view/报表层派生，除非 ADR 明确允许落表。
- 时长使用 `{process}_dur`；事件测量允许引擎常用单位字段 `duration_ms`、`latency_ms`。
- 最近/最新值使用 `{metric}_last1`。

## Grain and Period Rules

表名、主键、DDL 注释和模型文档必须能共同说明粒度。核心示例：

- `dwd_ai_llm_request_di`：每个 LLM provider request attempt result 一行。
- `dwd_ai_agent_run_di`：每个端到端 Agent run 一行。
- `dwd_ai_agent_span_di`：每个 Agent runtime span 一行。
- `dwd_ai_agent_tool_call_di`：每次实际工具调用一行。
- `dws_ai_llm_feature_request_1d`：每天每 app、feature、model 一行。
- `dws_ai_agent_agent_run_1d`：每天每 app、agent、task type 一行。
- `dws_ai_agent_tool_tool_call_1d`：每天每 agent、tool、tool type 一行。

标准周期：`1h`、`1d`、`30d`、`3m`、`6m`、`1y`、`td`、`nd`。精确粒度全集以 `TABLE_GRAINS` 为准。

## Task and Script Naming

- 转换脚本尽量以目标表命名。
- 可复用 SQL 转换模块使用 `trans_{table_name}`；备份模块使用 `bak_{table_name}`。
- 新独立 Python helper 可用 `python_{operation}_{table_name}`；已有清晰的 `spark_*` 名称可以保留。
- 数据搬运任务必须体现 source 与 sink，例如 `postgres2kafka_ods_ai_observability_llm_request_events`。

## Data and Metric Rules

- ODS 不计算业务指标；DWD 不保存不必要的 prompt/response 明文，优先 hash 和 size。
- 无效事实进入 quarantine，不因单行坏数据终止整个批次。
- DWS 优先保存可加和计数、金额和直接聚合；成功率、错误率等由查询层用分子/分母派生。
- Flink 流式 percentile 能力受限时使用明确命名/文档化的上界指标；不能把 `MAX` 冒充 p95。参见 ADR 004。
- 新指标必须在 `docs/metric_definitions.md` 给出 grain、分子、分母、时间窗口、单位和排除项。

## Lifecycle and Security

- ODS/DWD 增量事件保留更久，用于追踪、回放和调试；可从上游重建的 DWS/ADS 可较短保留。
- 维度通常为全量快照并使用 `df`。
- Doris 动态分区至少保留 12 个月历史，除非 ADR 另有说明。
- Gravitino metalake 固定为 `ai_observability`，Paimon catalog 固定为 `paimon_lake`；catalog 凭据、URI 和持久化配置不得散落硬编码到作业中。
- 不提交 `.env`、API key、真实 prompt/response、直接个人联系方式或未脱敏 IP；演示默认密码仅限本机。

## Documentation Discipline

- 当前态入口：`README.md`、`docs/architecture.md`、`docs/technical_document.md`、`docs/product_document.md`、`docs/data_model.md`、`docs/runtime_runbook.md`。
- 计划与迁移文档必须在标题附近标明状态；完成后不能继续用未来时描述已实现能力。
- Markdown 相对链接必须可解析；命令必须对应 Makefile 或现有脚本；表名必须可在契约、SQL 或脚本中找到。
- 不引入新的分层/命名风格，除非先更新本文件并用 ADR 记录例外。

## Definition of Done

- 跨层引用、字段契约和表粒度同步。
- 相关测试通过；必要时全量测试通过。
- 当前态架构、数据模型、血缘、指标和运行手册已同步。
- 新服务具备 health check、启停方式和安全配置说明。
- 未覆盖或需外部系统支持的能力被明确标注，不把模型/DDL 存在等同于生产接入完成。
