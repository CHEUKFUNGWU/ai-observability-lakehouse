from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ValidationRule:
    expression: str
    category: str
    code: str


@dataclass(frozen=True)
class FieldContract:
    name: str
    spark_type: str
    flink_type: str
    doris_type: str
    source_name: str | None = None
    default_literal: str | int | float | bool | None = None
    sql_default_literal: str | int | float | bool | None = None
    nullable: bool = False

    @property
    def source_column(self) -> str:
        return self.source_name or self.name

    def spark_column(self, source_columns: set[str]) -> Column:
        if self.source_column in source_columns:
            source = F.col(self.source_column)
        else:
            source = F.lit(self.default_literal)
        return self._cast_spark(source).alias(self.name)

    def flink_select_expression(self) -> str:
        source = self.source_column
        target = f"`{self.name}`" if self.name == "date" else self.name
        if self.default_literal is None:
            if source == self.name:
                return f"`{source}`" if source == "date" else source
            return f"{source} AS {target}"
        sql_default = self.sql_default_literal if self.sql_default_literal is not None else self.default_literal
        default_sql = self._sql_literal(sql_default)
        expression = f"COALESCE({source}, {default_sql})"
        return f"{expression} AS {target}"

    def flink_column_definition(self) -> str:
        name = f"`{self.name}`" if self.name == "date" else self.name
        return f"{name} {self.flink_type}"

    def doris_column_definition(self) -> str:
        nullability = "NULL" if self.nullable else "NOT NULL"
        if self.default_literal is None:
            return f"{self.name} {self.doris_type} {nullability}"
        return f'{self.name} {self.doris_type} {nullability} DEFAULT "{self._doris_default()}"'

    def _cast_spark(self, column: Column) -> Column:
        if self.name == "created_at":
            return F.to_timestamp(column)
        if self.name == "date":
            return F.to_date(column)
        return column.cast(self.spark_type)

    def _doris_default(self) -> str:
        value = self.default_literal
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    @staticmethod
    def _sql_literal(value: str | int | float | bool) -> str:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        return str(value)


TABLE_GRAINS: dict[str, str] = {
    "dwd_ai_llm_request_di": "one row per LLM provider request attempt result",
    "dwd_ai_agent_run_di": "one row per Agent task/run",
    "dwd_ai_agent_span_di": "one row per Agent runtime span",
    "dwd_ai_agent_tool_call_di": "one row per Agent tool invocation",
    "dwd_ai_retrieval_request_di": "one row per retrieval request",
    "dwd_ai_feedback_action_di": "one row per feedback action",
    "dwd_ai_guardrail_check_di": "one row per guardrail rule evaluation",
    "dwd_ai_evaluation_judgment_di": "one row per evaluation judgment",
    "dwd_ai_model_deployment_di": "one row per model deployment action",
    "dwd_ai_compliance_access_audit_di": "one row per access attempt",
    "dwd_ai_compliance_data_retention_di": "one row per retention action on a table partition",
    "dwd_ai_agent_orchestration_di": "one row per inter-agent handoff",
    "dws_ai_llm_feature_request_1d": "one daily row per app, feature, and model",
    "dws_ai_agent_agent_run_1d": "one daily row per app, agent, and task type",
    "dws_ai_agent_tool_tool_call_1d": "one daily row per agent, tool, and tool type",
    "dws_ai_retrieval_knowledge_base_request_1d": "one daily row per app, knowledge base, embedding model, and strategy",
    "dws_ai_feedback_feature_action_1d": "one daily row per app, feature, and agent",
    "dws_ai_guardrail_rule_check_1d": "one daily row per app, rule category, and action",
    "dws_ai_cost_team_request_1d": "one daily row per team, app, and model",
    "dws_ai_evaluation_feature_judgment_1d": "one daily row per app, feature, evaluation dimension, and evaluated model",
    "dws_ai_prompt_version_request_1d": "one daily row per prompt, version, and model",
    "dws_ai_llm_feature_env_request_1d": "one daily row per app, feature, model, and environment",
    "dws_ai_llm_region_request_1d": "one daily row per region, environment, app, and model",
    "dws_ai_agent_team_run_1d": "one daily row per team, app, agent, and task type",
    "dws_ai_llm_feature_request_1h": "one hourly row per app, feature, and model",
    "dws_ai_llm_session_request_1d": "one daily row per app and feature",
    "dws_ai_agent_orchestration_handoff_1d": "one daily row per parent agent, child agent, and handoff type",
    "dws_ai_platform_component_health_1d": "one daily row per component and metric",
}


LLM_REQUEST_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("request_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("span_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_name", "string", "STRING", "VARCHAR(256)", default_literal=""),
    FieldContract("channel", "string", "STRING", "VARCHAR(64)", default_literal=""),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("session_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("conversation_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("feature_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("prompt_category", "string", "STRING", "VARCHAR(256)"),
    FieldContract("prompt_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("prompt_version", "string", "STRING", "VARCHAR(64)"),
    FieldContract("model_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("provider", "string", "STRING", "VARCHAR(128)"),
    FieldContract("prompt_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("response_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("input_chars", "int", "INT", "INT", default_literal=0),
    FieldContract("output_chars", "int", "INT", "INT", default_literal=0),
    FieldContract("prompt_tokens", "int", "INT", "INT"),
    FieldContract("completion_tokens", "int", "INT", "INT"),
    FieldContract("total_tokens", "int", "INT", "INT"),
    FieldContract("request_type", "string", "STRING", "VARCHAR(64)", default_literal="chat"),
    FieldContract("is_streaming", "boolean", "BOOLEAN", "BOOLEAN", default_literal=False),
    FieldContract("temperature", "double", "DOUBLE", "DOUBLE", default_literal=0, sql_default_literal=0.0),
    FieldContract("max_tokens", "int", "INT", "INT", default_literal=0),
    FieldContract("finish_reason", "string", "STRING", "VARCHAR(64)", default_literal=""),
    FieldContract("retry_count", "int", "INT", "INT", default_literal=0),
    FieldContract("latency_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("error_type", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("http_status", "int", "INT", "SMALLINT"),
    FieldContract("estimated_cost_usd", "double", "DOUBLE", "DOUBLE"),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("region", "string", "STRING", "VARCHAR(64)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

LLM_REQUEST_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("request_id IS NOT NULL", "completeness", "missing_request_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("prompt_tokens >= 0", "validity", "negative_prompt_tokens"),
    ValidationRule("completion_tokens >= 0", "validity", "negative_completion_tokens"),
    ValidationRule("total_tokens = prompt_tokens + completion_tokens", "consistency", "token_total_mismatch"),
    ValidationRule("latency_ms > 0", "validity", "non_positive_latency"),
    ValidationRule("status IN ('success', 'error')", "validity", "invalid_status"),
    ValidationRule("estimated_cost_usd >= 0", "validity", "negative_cost"),
    ValidationRule("mode IN ('mock', 'live', 'replay', 'hermes')", "validity", "invalid_mode"),
)

LLM_FEATURE_REQUEST_1D_Paimon_COLUMNS: tuple[str, ...] = (
    "app_name STRING",
    "feature_name STRING",
    "model_name STRING",
    "request_count BIGINT",
    "success_count BIGINT",
    "error_count BIGINT",
    "prompt_tokens BIGINT",
    "completion_tokens BIGINT",
    "total_tokens BIGINT",
    "estimated_cost_usd DOUBLE",
    "avg_latency_ms DOUBLE",
    "max_latency_ms BIGINT",
    "p95_latency_ms BIGINT",
    "`date` DATE",
)

LLM_REQUEST_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in LLM_REQUEST_FIELDS if field.name != "date"
)

AGENT_RUN_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("agent_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("agent_version", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("session_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("conversation_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("task_type", "string", "STRING", "VARCHAR(128)"),
    FieldContract("channel", "string", "STRING", "VARCHAR(64)", default_literal=""),
    FieldContract("toolsets_used", "string", "STRING", "STRING", nullable=True),
    FieldContract("input_text_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("output_text_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("start_time", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("end_time", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("duration_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("error_type", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("turn_count", "int", "INT", "INT", default_literal=0),
    FieldContract("llm_call_count", "int", "INT", "INT", default_literal=0),
    FieldContract("tool_call_count", "int", "INT", "INT", default_literal=0),
    FieldContract("retrieval_count", "int", "INT", "INT", default_literal=0),
    FieldContract("total_tokens", "int", "INT", "INT", default_literal=0),
    FieldContract("estimated_cost_usd", "double", "DOUBLE", "DOUBLE", default_literal=0, sql_default_literal=0.0),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("region", "string", "STRING", "VARCHAR(64)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

AGENT_SPAN_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("span_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("parent_span_id", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("span_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("span_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("span_order", "int", "INT", "INT", default_literal=0),
    FieldContract("start_time", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("end_time", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("duration_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("error_type", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("retry_count", "int", "INT", "INT", default_literal=0),
    FieldContract("input_size", "int", "INT", "INT", default_literal=0),
    FieldContract("output_size", "int", "INT", "INT", default_literal=0),
    FieldContract("model_name", "string", "STRING", "VARCHAR(256)", nullable=True),
    FieldContract("tool_name", "string", "STRING", "VARCHAR(256)", nullable=True),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("region", "string", "STRING", "VARCHAR(64)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

AGENT_TOOL_CALL_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("tool_call_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("span_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("tool_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("tool_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("arguments_json", "string", "STRING", "STRING"),
    FieldContract("result_text", "string", "STRING", "STRING"),
    FieldContract("result_size", "int", "INT", "INT", default_literal=0),
    FieldContract("duration_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("error_type", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("retry_count", "int", "INT", "INT", default_literal=0),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("region", "string", "STRING", "VARCHAR(64)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

MODEL_DEPLOYMENT_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("deployment_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("model_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("model_version", "string", "STRING", "VARCHAR(128)"),
    FieldContract("provider", "string", "STRING", "VARCHAR(128)"),
    FieldContract("deployment_action", "string", "STRING", "VARCHAR(64)"),
    FieldContract("traffic_percentage", "double", "DOUBLE", "DOUBLE"),
    FieldContract("target_environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("deployer_user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("deploy_reason", "string", "STRING", "VARCHAR(256)", default_literal=""),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

AGENT_RUN_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "agent_id VARCHAR(128) NOT NULL",
    "agent_name VARCHAR(256) NOT NULL",
    "task_type VARCHAR(128) NOT NULL",
    "run_count BIGINT NOT NULL",
    "success_count BIGINT NOT NULL",
    "error_count BIGINT NOT NULL",
    "turn_count BIGINT NOT NULL",
    "llm_call_count BIGINT NOT NULL",
    "tool_call_count BIGINT NOT NULL",
    "retrieval_count BIGINT NOT NULL",
    "total_tokens BIGINT NOT NULL",
    "estimated_cost_usd DOUBLE NOT NULL",
    "avg_duration_ms DOUBLE NOT NULL",
    "p95_duration_ms BIGINT NOT NULL",
    "span_count BIGINT NOT NULL",
    "failed_span_count BIGINT NOT NULL",
    "tool_span_count BIGINT NOT NULL",
    "llm_span_count BIGINT NOT NULL",
)

AGENT_TOOL_CALL_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "agent_id VARCHAR(128) NOT NULL",
    "tool_name VARCHAR(256) NOT NULL",
    "tool_type VARCHAR(64) NOT NULL",
    "tool_call_count BIGINT NOT NULL",
    "success_count BIGINT NOT NULL",
    "error_count BIGINT NOT NULL",
    "retry_count BIGINT NOT NULL",
    "avg_duration_ms DOUBLE NOT NULL",
    "p95_duration_ms BIGINT NOT NULL",
    "avg_result_size DOUBLE NOT NULL",
    "max_result_size BIGINT NOT NULL",
)

RETRIEVAL_REQUEST_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("retrieval_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("span_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("request_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("feature_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("knowledge_base_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("knowledge_base_name", "string", "STRING", "VARCHAR(256)", default_literal=""),
    FieldContract("embedding_model", "string", "STRING", "VARCHAR(256)"),
    FieldContract("retrieval_strategy", "string", "STRING", "VARCHAR(64)"),
    FieldContract("query_text_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("query_length", "int", "INT", "INT", default_literal=0),
    FieldContract("top_k", "int", "INT", "INT"),
    FieldContract("returned_count", "int", "INT", "INT"),
    FieldContract("hit_count", "int", "INT", "INT"),
    FieldContract("max_similarity_score", "double", "DOUBLE", "DOUBLE"),
    FieldContract("min_similarity_score", "double", "DOUBLE", "DOUBLE"),
    FieldContract("avg_similarity_score", "double", "DOUBLE", "DOUBLE"),
    FieldContract("embedding_latency_ms", "int", "INT", "INT"),
    FieldContract("search_latency_ms", "int", "INT", "INT"),
    FieldContract("total_latency_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("error_type", "string", "STRING", "VARCHAR(128)", nullable=True),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

RETRIEVAL_REQUEST_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("retrieval_id IS NOT NULL", "completeness", "missing_retrieval_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("top_k > 0", "validity", "non_positive_top_k"),
    ValidationRule("returned_count >= 0", "validity", "negative_returned_count"),
    ValidationRule("hit_count >= 0", "validity", "negative_hit_count"),
    ValidationRule("hit_count <= returned_count", "consistency", "hit_count_exceeds_returned_count"),
    ValidationRule("total_latency_ms > 0", "validity", "non_positive_total_latency"),
    ValidationRule("status IN ('success', 'error')", "validity", "invalid_status"),
    ValidationRule("mode IN ('mock', 'live', 'replay')", "validity", "invalid_mode"),
)

RETRIEVAL_KNOWLEDGE_BASE_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "knowledge_base_id STRING",
    "embedding_model STRING",
    "retrieval_strategy STRING",
    "retrieval_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "zero_result_cnt_1d BIGINT",
    "returned_cnt_1d BIGINT",
    "hit_cnt_1d BIGINT",
    "avg_similarity_score DOUBLE",
    "avg_total_latency_ms DOUBLE",
    "p95_total_latency_ms BIGINT",
    "avg_embedding_latency_ms DOUBLE",
    "avg_search_latency_ms DOUBLE",
)

RETRIEVAL_KNOWLEDGE_BASE_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "knowledge_base_id VARCHAR(128) NOT NULL",
    "embedding_model VARCHAR(256) NOT NULL",
    "retrieval_strategy VARCHAR(64) NOT NULL",
    "retrieval_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "zero_result_cnt_1d BIGINT NOT NULL",
    "returned_cnt_1d BIGINT NOT NULL",
    "hit_cnt_1d BIGINT NOT NULL",
    "avg_similarity_score DOUBLE NOT NULL",
    "avg_total_latency_ms DOUBLE NOT NULL",
    "p95_total_latency_ms BIGINT NOT NULL",
    "avg_embedding_latency_ms DOUBLE NOT NULL",
    "avg_search_latency_ms DOUBLE NOT NULL",
)

RETRIEVAL_REQUEST_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in RETRIEVAL_REQUEST_FIELDS if field.name != "date"
)

FEEDBACK_ACTION_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("feedback_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("request_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("session_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("conversation_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("feature_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("agent_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("feedback_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("rating_value", "int", "INT", "INT", nullable=True),
    FieldContract("feedback_text_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("feedback_text_length", "int", "INT", "INT", default_literal=0),
    FieldContract("response_latency_ms", "int", "INT", "INT"),
    FieldContract("model_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("prompt_version", "string", "STRING", "VARCHAR(64)"),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

FEEDBACK_ACTION_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("feedback_id IS NOT NULL", "completeness", "missing_feedback_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule(
        "feedback_type IN ('thumbs_up', 'thumbs_down', 'rating', 'regenerate', 'edit', 'report')",
        "validity",
        "invalid_feedback_type",
    ),
    ValidationRule("(rating_value IS NULL OR rating_value BETWEEN 1 AND 5)", "validity", "invalid_rating_value"),
    ValidationRule("response_latency_ms > 0", "validity", "non_positive_response_latency"),
    ValidationRule("mode IN ('mock', 'live')", "validity", "invalid_mode"),
)

FEEDBACK_FEATURE_ACTION_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "feature_name STRING",
    "agent_id STRING",
    "feedback_cnt_1d BIGINT",
    "thumbs_up_cnt_1d BIGINT",
    "thumbs_down_cnt_1d BIGINT",
    "regenerate_cnt_1d BIGINT",
    "report_cnt_1d BIGINT",
    "avg_rating DOUBLE",
    "rated_request_cnt_1d BIGINT",
)

FEEDBACK_FEATURE_ACTION_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "feature_name VARCHAR(256) NOT NULL",
    "agent_id VARCHAR(128) NOT NULL",
    "feedback_cnt_1d BIGINT NOT NULL",
    "thumbs_up_cnt_1d BIGINT NOT NULL",
    "thumbs_down_cnt_1d BIGINT NOT NULL",
    "regenerate_cnt_1d BIGINT NOT NULL",
    "report_cnt_1d BIGINT NOT NULL",
    "avg_rating DOUBLE NULL",
    "rated_request_cnt_1d BIGINT NOT NULL",
)

FEEDBACK_ACTION_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in FEEDBACK_ACTION_FIELDS if field.name != "date"
)

GUARDRAIL_CHECK_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("guardrail_event_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("request_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("feature_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("guardrail_stage", "string", "STRING", "VARCHAR(64)"),
    FieldContract("rule_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("rule_category", "string", "STRING", "VARCHAR(64)"),
    FieldContract("triggered", "boolean", "BOOLEAN", "BOOLEAN"),
    FieldContract("action_taken", "string", "STRING", "VARCHAR(64)"),
    FieldContract("severity", "string", "STRING", "VARCHAR(32)"),
    FieldContract("matched_pattern_hash", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("input_text_length", "int", "INT", "INT", default_literal=0),
    FieldContract("guardrail_latency_ms", "int", "INT", "INT"),
    FieldContract("model_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("prompt_version", "string", "STRING", "VARCHAR(64)"),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

GUARDRAIL_CHECK_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("guardrail_event_id IS NOT NULL", "completeness", "missing_guardrail_event_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("guardrail_stage IN ('pre_request', 'post_response')", "validity", "invalid_guardrail_stage"),
    ValidationRule(
        "rule_category IN ('content_filter', 'pii_detection', 'toxicity', 'topic_block', 'length_limit')",
        "validity",
        "invalid_rule_category",
    ),
    ValidationRule("action_taken IN ('pass', 'warn', 'block', 'redact', 'override')", "validity", "invalid_action_taken"),
    ValidationRule("severity IN ('low', 'medium', 'high', 'critical')", "validity", "invalid_severity"),
    ValidationRule("guardrail_latency_ms > 0", "validity", "non_positive_guardrail_latency"),
    ValidationRule("mode IN ('mock', 'live')", "validity", "invalid_mode"),
)

GUARDRAIL_RULE_CHECK_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "rule_category STRING",
    "action_taken STRING",
    "check_cnt_1d BIGINT",
    "triggered_cnt_1d BIGINT",
    "block_cnt_1d BIGINT",
    "redact_cnt_1d BIGINT",
    "warn_cnt_1d BIGINT",
    "avg_guardrail_latency_ms DOUBLE",
    "distinct_user_cnt_1d BIGINT",
)

GUARDRAIL_RULE_CHECK_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "rule_category VARCHAR(64) NOT NULL",
    "action_taken VARCHAR(64) NOT NULL",
    "check_cnt_1d BIGINT NOT NULL",
    "triggered_cnt_1d BIGINT NOT NULL",
    "block_cnt_1d BIGINT NOT NULL",
    "redact_cnt_1d BIGINT NOT NULL",
    "warn_cnt_1d BIGINT NOT NULL",
    "avg_guardrail_latency_ms DOUBLE NOT NULL",
    "distinct_user_cnt_1d BIGINT NOT NULL",
)

GUARDRAIL_CHECK_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in GUARDRAIL_CHECK_FIELDS if field.name != "date"
)

EVALUATION_JUDGMENT_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("evaluation_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("request_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("run_id", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("app_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("feature_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("evaluator_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("evaluator_model", "string", "STRING", "VARCHAR(256)", default_literal=""),
    FieldContract("evaluation_dimension", "string", "STRING", "VARCHAR(64)"),
    FieldContract("score", "double", "DOUBLE", "DOUBLE"),
    FieldContract("raw_score", "string", "STRING", "VARCHAR(128)", default_literal=""),
    FieldContract("pass_threshold", "double", "DOUBLE", "DOUBLE"),
    FieldContract("passed", "boolean", "BOOLEAN", "BOOLEAN"),
    FieldContract("evaluated_model_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("evaluated_prompt_version", "string", "STRING", "VARCHAR(64)"),
    FieldContract("evaluation_latency_ms", "int", "INT", "INT"),
    FieldContract("mode", "string", "STRING", "VARCHAR(32)"),
    FieldContract("environment", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME"),
    FieldContract("date", "date", "DATE", "DATE"),
)

EVALUATION_JUDGMENT_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("evaluation_id IS NOT NULL", "completeness", "missing_evaluation_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule(
        "evaluator_type IN ('llm_judge', 'human', 'ground_truth', 'regex', 'classifier')",
        "validity",
        "invalid_evaluator_type",
    ),
    ValidationRule(
        "evaluation_dimension IN ('relevance', 'faithfulness', 'coherence', 'toxicity', 'hallucination')",
        "validity",
        "invalid_evaluation_dimension",
    ),
    ValidationRule("score BETWEEN 0.0 AND 1.0", "validity", "invalid_score"),
    ValidationRule("pass_threshold BETWEEN 0.0 AND 1.0", "validity", "invalid_pass_threshold"),
    ValidationRule("evaluation_latency_ms > 0", "validity", "non_positive_evaluation_latency"),
    ValidationRule("mode IN ('mock', 'live', 'offline')", "validity", "invalid_mode"),
)

EVALUATION_FEATURE_JUDGMENT_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "feature_name STRING",
    "evaluation_dimension STRING",
    "evaluated_model_name STRING",
    "evaluation_cnt_1d BIGINT",
    "pass_cnt_1d BIGINT",
    "fail_cnt_1d BIGINT",
    "avg_score DOUBLE",
    "p10_score DOUBLE",
    "avg_evaluation_latency_ms DOUBLE",
)

EVALUATION_FEATURE_JUDGMENT_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "feature_name VARCHAR(256) NOT NULL",
    "evaluation_dimension VARCHAR(64) NOT NULL",
    "evaluated_model_name VARCHAR(256) NOT NULL",
    "evaluation_cnt_1d BIGINT NOT NULL",
    "pass_cnt_1d BIGINT NOT NULL",
    "fail_cnt_1d BIGINT NOT NULL",
    "avg_score DOUBLE NOT NULL",
    "p10_score DOUBLE NOT NULL",
    "avg_evaluation_latency_ms DOUBLE NOT NULL",
)

COMPLIANCE_ACCESS_AUDIT_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("audit_event_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("user_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("action_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("resource_type", "string", "STRING", "VARCHAR(64)"),
    FieldContract("resource_id", "string", "STRING", "VARCHAR(256)"),
    FieldContract("ip_address", "string", "STRING", "CHAR(64)"),
    FieldContract("access_granted", "boolean", "BOOLEAN", "BOOLEAN"),
    FieldContract("denial_reason", "string", "STRING", "VARCHAR(256)", nullable=True),
    FieldContract("data_classification", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME(3)"),
    FieldContract("date", "date", "DATE", "DATE"),
)

COMPLIANCE_ACCESS_AUDIT_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("audit_event_id IS NOT NULL", "completeness", "missing_audit_event_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("CHAR_LENGTH(ip_address) = 64", "validity", "invalid_ip_hash"),
    ValidationRule(
        "action_type IN ('query', 'export', 'view_prompt', 'view_response', 'delete', 'admin_override')",
        "validity",
        "invalid_action_type",
    ),
    ValidationRule(
        "resource_type IN ('dashboard', 'dwd_table', 'raw_log', 'prompt_text', 'response_text')",
        "validity",
        "invalid_resource_type",
    ),
    ValidationRule(
        "data_classification IN ('public', 'internal', 'confidential', 'restricted')",
        "validity",
        "invalid_data_classification",
    ),
    ValidationRule("(access_granted OR denial_reason IS NOT NULL)", "consistency", "missing_denial_reason"),
)

COMPLIANCE_DATA_RETENTION_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("retention_event_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("table_name", "string", "STRING", "VARCHAR(256)"),
    FieldContract("partition_date", "date", "DATE", "DATE"),
    FieldContract("action_type", "string", "STRING", "VARCHAR(32)"),
    FieldContract("rows_affected", "bigint", "BIGINT", "BIGINT"),
    FieldContract("policy_name", "string", "STRING", "VARCHAR(128)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME(3)"),
    FieldContract("date", "date", "DATE", "DATE"),
)

COMPLIANCE_DATA_RETENTION_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("retention_event_id IS NOT NULL", "completeness", "missing_retention_event_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("partition_date IS NOT NULL", "completeness", "missing_partition_date"),
    ValidationRule("action_type IN ('archive', 'anonymize', 'delete')", "validity", "invalid_action_type"),
    ValidationRule("rows_affected >= 0", "validity", "negative_rows_affected"),
    ValidationRule("policy_name IS NOT NULL", "completeness", "missing_policy_name"),
)

AGENT_ORCHESTRATION_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("orchestration_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("trace_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("parent_run_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("child_run_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("parent_agent_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("child_agent_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("handoff_type", "string", "STRING", "VARCHAR(32)"),
    FieldContract("payload_size", "int", "INT", "INT"),
    FieldContract("handoff_latency_ms", "int", "INT", "INT"),
    FieldContract("status", "string", "STRING", "VARCHAR(32)"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME(3)"),
    FieldContract("date", "date", "DATE", "DATE"),
)

AGENT_ORCHESTRATION_VALIDATION_RULES: tuple[ValidationRule, ...] = (
    ValidationRule("orchestration_id IS NOT NULL", "completeness", "missing_orchestration_id"),
    ValidationRule("created_at IS NOT NULL", "completeness", "missing_created_at"),
    ValidationRule("parent_run_id <> child_run_id", "consistency", "identical_run_ids"),
    ValidationRule("parent_agent_id <> child_agent_id", "consistency", "identical_agent_ids"),
    ValidationRule("handoff_type IN ('delegate', 'callback', 'broadcast', 'sequential')", "validity", "invalid_handoff_type"),
    ValidationRule("payload_size >= 0", "validity", "negative_payload_size"),
    ValidationRule("handoff_latency_ms >= 0", "validity", "negative_handoff_latency"),
    ValidationRule("status IN ('success', 'error', 'timeout')", "validity", "invalid_status"),
)

AGENT_ORCHESTRATION_HANDOFF_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "parent_agent_id STRING",
    "child_agent_id STRING",
    "handoff_type STRING",
    "handoff_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "timeout_cnt_1d BIGINT",
    "avg_handoff_latency_ms DOUBLE",
    "p95_handoff_latency_ms BIGINT",
)

AGENT_ORCHESTRATION_HANDOFF_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "parent_agent_id VARCHAR(128) NOT NULL",
    "child_agent_id VARCHAR(128) NOT NULL",
    "handoff_type VARCHAR(32) NOT NULL",
    "handoff_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "timeout_cnt_1d BIGINT NOT NULL",
    "avg_handoff_latency_ms DOUBLE NOT NULL",
    "p95_handoff_latency_ms BIGINT NOT NULL",
)

PLATFORM_HEALTH_METRIC_FIELDS: tuple[FieldContract, ...] = (
    FieldContract("metric_event_id", "string", "STRING", "VARCHAR(128)"),
    FieldContract("component", "string", "STRING", "VARCHAR(32)"),
    FieldContract("metric_name", "string", "STRING", "VARCHAR(128)"),
    FieldContract("metric_value", "double", "DOUBLE", "DOUBLE"),
    FieldContract("threshold", "double", "DOUBLE", "DOUBLE"),
    FieldContract("created_at", "timestamp", "TIMESTAMP(3)", "DATETIME(3)"),
    FieldContract("date", "date", "DATE", "DATE"),
)

PLATFORM_COMPONENT_HEALTH_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "component STRING",
    "metric_name STRING",
    "metric_value DOUBLE",
    "threshold DOUBLE",
    "is_breach BOOLEAN",
)

PLATFORM_COMPONENT_HEALTH_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "component VARCHAR(32) NOT NULL",
    "metric_name VARCHAR(128) NOT NULL",
    "metric_value DOUBLE NOT NULL",
    "threshold DOUBLE NOT NULL",
    "is_breach BOOLEAN NOT NULL",
)

COST_TEAM_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "team_id STRING",
    "app_name STRING",
    "model_name STRING",
    "request_cnt_1d BIGINT",
    "total_token_cnt_1d BIGINT",
    "estimated_cost_amt_1d DOUBLE",
    "agent_run_cnt_1d BIGINT",
    "agent_cost_amt_1d DOUBLE",
)

COST_TEAM_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "team_id VARCHAR(128) NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "model_name VARCHAR(256) NOT NULL",
    "request_cnt_1d BIGINT NOT NULL",
    "total_token_cnt_1d BIGINT NOT NULL",
    "estimated_cost_amt_1d DOUBLE NOT NULL",
    "agent_run_cnt_1d BIGINT NOT NULL",
    "agent_cost_amt_1d DOUBLE NOT NULL",
)

PROMPT_VERSION_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "prompt_id STRING",
    "prompt_version STRING",
    "model_name STRING",
    "request_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "avg_latency_ms DOUBLE",
    "p95_latency_ms BIGINT",
    "total_token_cnt_1d BIGINT",
    "estimated_cost_amt_1d DOUBLE",
    "avg_evaluation_score DOUBLE",
)

PROMPT_VERSION_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "prompt_id VARCHAR(128) NOT NULL",
    "prompt_version VARCHAR(64) NOT NULL",
    "model_name VARCHAR(256) NOT NULL",
    "request_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "avg_latency_ms DOUBLE NOT NULL",
    "p95_latency_ms BIGINT NOT NULL",
    "total_token_cnt_1d BIGINT NOT NULL",
    "estimated_cost_amt_1d DOUBLE NOT NULL",
    "avg_evaluation_score DOUBLE NULL",
)

LLM_FEATURE_ENV_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "feature_name STRING",
    "model_name STRING",
    "environment STRING",
    "request_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "prompt_token_cnt_1d BIGINT",
    "completion_token_cnt_1d BIGINT",
    "total_token_cnt_1d BIGINT",
    "estimated_cost_amt_1d DOUBLE",
    "avg_latency_ms DOUBLE",
    "max_latency_ms BIGINT",
    "p95_latency_ms BIGINT",
)

LLM_FEATURE_ENV_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "feature_name VARCHAR(256) NOT NULL",
    "model_name VARCHAR(256) NOT NULL",
    "environment VARCHAR(32) NOT NULL",
    "request_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "prompt_token_cnt_1d BIGINT NOT NULL",
    "completion_token_cnt_1d BIGINT NOT NULL",
    "total_token_cnt_1d BIGINT NOT NULL",
    "estimated_cost_amt_1d DOUBLE NOT NULL",
    "avg_latency_ms DOUBLE NOT NULL",
    "max_latency_ms BIGINT NOT NULL",
    "p95_latency_ms BIGINT NOT NULL",
)

LLM_REGION_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "region STRING",
    "environment STRING",
    "app_name STRING",
    "model_name STRING",
    "request_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "prompt_token_cnt_1d BIGINT",
    "completion_token_cnt_1d BIGINT",
    "total_token_cnt_1d BIGINT",
    "estimated_cost_amt_1d DOUBLE",
    "avg_latency_ms DOUBLE",
    "max_latency_ms BIGINT",
    "p95_latency_ms BIGINT",
)

LLM_REGION_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "region VARCHAR(64) NOT NULL",
    "environment VARCHAR(32) NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "model_name VARCHAR(256) NOT NULL",
    "request_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "prompt_token_cnt_1d BIGINT NOT NULL",
    "completion_token_cnt_1d BIGINT NOT NULL",
    "total_token_cnt_1d BIGINT NOT NULL",
    "estimated_cost_amt_1d DOUBLE NOT NULL",
    "avg_latency_ms DOUBLE NOT NULL",
    "max_latency_ms BIGINT NOT NULL",
    "p95_latency_ms BIGINT NOT NULL",
)

AGENT_TEAM_RUN_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "team_id STRING",
    "app_name STRING",
    "agent_id STRING",
    "agent_name STRING",
    "task_type STRING",
    "run_cnt_1d BIGINT",
    "success_cnt_1d BIGINT",
    "error_cnt_1d BIGINT",
    "turn_cnt_1d BIGINT",
    "llm_call_cnt_1d BIGINT",
    "tool_call_cnt_1d BIGINT",
    "retrieval_cnt_1d BIGINT",
    "total_token_cnt_1d BIGINT",
    "estimated_cost_amt_1d DOUBLE",
    "avg_duration_ms DOUBLE",
    "p95_duration_ms BIGINT",
    "span_cnt_1d BIGINT",
    "failed_span_cnt_1d BIGINT",
    "tool_span_cnt_1d BIGINT",
    "llm_span_cnt_1d BIGINT",
)

AGENT_TEAM_RUN_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "team_id VARCHAR(128) NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "agent_id VARCHAR(128) NOT NULL",
    "agent_name VARCHAR(256) NOT NULL",
    "task_type VARCHAR(128) NOT NULL",
    "run_cnt_1d BIGINT NOT NULL",
    "success_cnt_1d BIGINT NOT NULL",
    "error_cnt_1d BIGINT NOT NULL",
    "turn_cnt_1d BIGINT NOT NULL",
    "llm_call_cnt_1d BIGINT NOT NULL",
    "tool_call_cnt_1d BIGINT NOT NULL",
    "retrieval_cnt_1d BIGINT NOT NULL",
    "total_token_cnt_1d BIGINT NOT NULL",
    "estimated_cost_amt_1d DOUBLE NOT NULL",
    "avg_duration_ms DOUBLE NOT NULL",
    "p95_duration_ms BIGINT NOT NULL",
    "span_cnt_1d BIGINT NOT NULL",
    "failed_span_cnt_1d BIGINT NOT NULL",
    "tool_span_cnt_1d BIGINT NOT NULL",
    "llm_span_cnt_1d BIGINT NOT NULL",
)

LLM_FEATURE_REQUEST_1H_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "`hour` INT",
    "app_name STRING",
    "feature_name STRING",
    "model_name STRING",
    "request_cnt_1h BIGINT",
    "success_cnt_1h BIGINT",
    "error_cnt_1h BIGINT",
    "prompt_token_cnt_1h BIGINT",
    "completion_token_cnt_1h BIGINT",
    "total_token_cnt_1h BIGINT",
    "estimated_cost_amt_1h DOUBLE",
    "avg_latency_ms DOUBLE",
    "max_latency_ms BIGINT",
    "p95_latency_ms BIGINT",
)

LLM_FEATURE_REQUEST_1H_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "`hour` TINYINT NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "feature_name VARCHAR(256) NOT NULL",
    "model_name VARCHAR(256) NOT NULL",
    "request_cnt_1h BIGINT NOT NULL",
    "success_cnt_1h BIGINT NOT NULL",
    "error_cnt_1h BIGINT NOT NULL",
    "prompt_token_cnt_1h BIGINT NOT NULL",
    "completion_token_cnt_1h BIGINT NOT NULL",
    "total_token_cnt_1h BIGINT NOT NULL",
    "estimated_cost_amt_1h DOUBLE NOT NULL",
    "avg_latency_ms DOUBLE NOT NULL",
    "max_latency_ms BIGINT NOT NULL",
    "p95_latency_ms BIGINT NOT NULL",
)

LLM_SESSION_REQUEST_1D_FLINK_COLUMNS: tuple[str, ...] = (
    "`date` DATE",
    "app_name STRING",
    "feature_name STRING",
    "session_cnt_1d BIGINT",
    "avg_turns_per_session DOUBLE",
    "avg_tokens_per_session DOUBLE",
    "avg_duration_per_session_ms DOUBLE",
    "resolved_session_cnt_1d BIGINT",
)

LLM_SESSION_REQUEST_1D_DORIS_COLUMNS: tuple[str, ...] = (
    "`date` DATE NOT NULL",
    "app_name VARCHAR(256) NOT NULL",
    "feature_name VARCHAR(256) NOT NULL",
    "session_cnt_1d BIGINT NOT NULL",
    "avg_turns_per_session DOUBLE NOT NULL",
    "avg_tokens_per_session DOUBLE NOT NULL",
    "avg_duration_per_session_ms DOUBLE NOT NULL",
    "resolved_session_cnt_1d BIGINT NOT NULL",
)

EVALUATION_JUDGMENT_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in EVALUATION_JUDGMENT_FIELDS if field.name != "date"
)
AGENT_RUN_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in AGENT_RUN_FIELDS if field.name != "date"
)
AGENT_SPAN_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in AGENT_SPAN_FIELDS if field.name != "date"
)
AGENT_TOOL_CALL_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in AGENT_TOOL_CALL_FIELDS if field.name != "date"
)
MODEL_DEPLOYMENT_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in MODEL_DEPLOYMENT_FIELDS if field.name != "date"
)
COMPLIANCE_ACCESS_AUDIT_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in COMPLIANCE_ACCESS_AUDIT_FIELDS if field.name != "date"
)
COMPLIANCE_DATA_RETENTION_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in COMPLIANCE_DATA_RETENTION_FIELDS if field.name != "date"
)
AGENT_ORCHESTRATION_DATA_FIELDS: tuple[FieldContract, ...] = tuple(
    field for field in AGENT_ORCHESTRATION_FIELDS if field.name != "date"
)


def build_llm_request_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in LLM_REQUEST_FIELDS))


def build_agent_run_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in AGENT_RUN_FIELDS)).withColumn(
        "toolsets_used",
        F.coalesce(F.col("toolsets_used"), F.lit("[]")),
    )


def build_agent_span_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in AGENT_SPAN_FIELDS))


def build_agent_tool_call_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in AGENT_TOOL_CALL_FIELDS))


def build_model_deployment_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in MODEL_DEPLOYMENT_FIELDS))


def build_retrieval_request_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in RETRIEVAL_REQUEST_FIELDS))


def build_feedback_action_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in FEEDBACK_ACTION_FIELDS))


def build_guardrail_check_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in GUARDRAIL_CHECK_FIELDS))


def build_evaluation_judgment_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in EVALUATION_JUDGMENT_FIELDS))


def build_compliance_access_audit_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in COMPLIANCE_ACCESS_AUDIT_FIELDS))


def build_compliance_data_retention_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in COMPLIANCE_DATA_RETENTION_FIELDS))


def build_agent_orchestration_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in AGENT_ORCHESTRATION_FIELDS))


def build_platform_health_metric_projection(raw_events: DataFrame) -> DataFrame:
    source_columns = set(raw_events.columns)
    return raw_events.select(*(field.spark_column(source_columns) for field in PLATFORM_HEALTH_METRIC_FIELDS))


def build_cost_team_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "team_id",
        "app_name",
        "model_name",
        "request_cnt_1d",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "agent_run_cnt_1d",
        "agent_cost_amt_1d",
    )


def build_prompt_version_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "prompt_id",
        "prompt_version",
        "model_name",
        "request_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "avg_latency_ms",
        "p95_latency_ms",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "avg_evaluation_score",
    )


def build_llm_feature_env_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "feature_name",
        "model_name",
        "environment",
        "request_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "prompt_token_cnt_1d",
        "completion_token_cnt_1d",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "avg_latency_ms",
        "max_latency_ms",
        "p95_latency_ms",
    )


def build_llm_region_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "region",
        "environment",
        "app_name",
        "model_name",
        "request_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "prompt_token_cnt_1d",
        "completion_token_cnt_1d",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "avg_latency_ms",
        "max_latency_ms",
        "p95_latency_ms",
    )


def build_agent_team_run_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "team_id",
        "app_name",
        "agent_id",
        "agent_name",
        "task_type",
        "run_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "turn_cnt_1d",
        "llm_call_cnt_1d",
        "tool_call_cnt_1d",
        "retrieval_cnt_1d",
        "total_token_cnt_1d",
        "estimated_cost_amt_1d",
        "avg_duration_ms",
        "p95_duration_ms",
        "span_cnt_1d",
        "failed_span_cnt_1d",
        "tool_span_cnt_1d",
        "llm_span_cnt_1d",
    )


def build_llm_feature_request_1h_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "hour",
        "app_name",
        "feature_name",
        "model_name",
        "request_cnt_1h",
        "success_cnt_1h",
        "error_cnt_1h",
        "prompt_token_cnt_1h",
        "completion_token_cnt_1h",
        "total_token_cnt_1h",
        "estimated_cost_amt_1h",
        "avg_latency_ms",
        "max_latency_ms",
        "p95_latency_ms",
    )


def build_llm_session_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "feature_name",
        "session_cnt_1d",
        "avg_turns_per_session",
        "avg_tokens_per_session",
        "avg_duration_per_session_ms",
        "resolved_session_cnt_1d",
    )


def build_llm_feature_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "feature_name",
        "model_name",
        "request_count",
        "success_count",
        "error_count",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "estimated_cost_usd",
        "avg_latency_ms",
        "max_latency_ms",
        "p95_latency_ms",
    )


def build_retrieval_knowledge_base_request_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "knowledge_base_id",
        "embedding_model",
        "retrieval_strategy",
        "retrieval_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "zero_result_cnt_1d",
        "returned_cnt_1d",
        "hit_cnt_1d",
        "avg_similarity_score",
        "avg_total_latency_ms",
        "p95_total_latency_ms",
        "avg_embedding_latency_ms",
        "avg_search_latency_ms",
    )


def build_feedback_feature_action_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "feature_name",
        "agent_id",
        "feedback_cnt_1d",
        "thumbs_up_cnt_1d",
        "thumbs_down_cnt_1d",
        "regenerate_cnt_1d",
        "report_cnt_1d",
        "avg_rating",
        "rated_request_cnt_1d",
    )


def build_guardrail_rule_check_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "rule_category",
        "action_taken",
        "check_cnt_1d",
        "triggered_cnt_1d",
        "block_cnt_1d",
        "redact_cnt_1d",
        "warn_cnt_1d",
        "avg_guardrail_latency_ms",
        "distinct_user_cnt_1d",
    )


def build_evaluation_feature_judgment_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "feature_name",
        "evaluation_dimension",
        "evaluated_model_name",
        "evaluation_cnt_1d",
        "pass_cnt_1d",
        "fail_cnt_1d",
        "avg_score",
        "p10_score",
        "avg_evaluation_latency_ms",
    )


def build_agent_orchestration_handoff_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "parent_agent_id",
        "child_agent_id",
        "handoff_type",
        "handoff_cnt_1d",
        "success_cnt_1d",
        "error_cnt_1d",
        "timeout_cnt_1d",
        "avg_handoff_latency_ms",
        "p95_handoff_latency_ms",
    )


def build_agent_run_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "app_name",
        "agent_id",
        "agent_name",
        "task_type",
        "run_count",
        "success_count",
        "error_count",
        "turn_count",
        "llm_call_count",
        "tool_call_count",
        "retrieval_count",
        "total_tokens",
        "estimated_cost_usd",
        "avg_duration_ms",
        "p95_duration_ms",
        "span_count",
        "failed_span_count",
        "tool_span_count",
        "llm_span_count",
    )


def build_agent_tool_call_1d_projection(metrics: DataFrame) -> DataFrame:
    return metrics.select(
        "date",
        "agent_id",
        "tool_name",
        "tool_type",
        "tool_call_count",
        "success_count",
        "error_count",
        "retry_count",
        "avg_duration_ms",
        "p95_duration_ms",
        "avg_result_size",
        "max_result_size",
    )


def llm_request_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in LLM_REQUEST_VALIDATION_RULES]


def retrieval_request_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in RETRIEVAL_REQUEST_VALIDATION_RULES]


def feedback_action_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in FEEDBACK_ACTION_VALIDATION_RULES]


def guardrail_check_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in GUARDRAIL_CHECK_VALIDATION_RULES]


def evaluation_judgment_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in EVALUATION_JUDGMENT_VALIDATION_RULES]


def compliance_access_audit_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in COMPLIANCE_ACCESS_AUDIT_VALIDATION_RULES]


def compliance_data_retention_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in COMPLIANCE_DATA_RETENTION_VALIDATION_RULES]


def agent_orchestration_validation_rules() -> list[tuple[str, str, str]]:
    return [(rule.expression, rule.category, rule.code) for rule in AGENT_ORCHESTRATION_VALIDATION_RULES]


def render_llm_request_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in LLM_REQUEST_FIELDS[:-1]]
    lines.append(f"{indent}{LLM_REQUEST_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND,")
    lines.append(f"{indent}PRIMARY KEY (request_id) NOT ENFORCED")
    return "\n".join(lines)


def render_llm_request_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in LLM_REQUEST_FIELDS[:-1]]
    lines.append(f"{indent}{LLM_REQUEST_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_llm_request_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in LLM_REQUEST_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {LLM_REQUEST_VALIDATION_RULES[0].expression}", *rules])


def render_llm_request_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in LLM_REQUEST_DATA_FIELDS[:-1])
    lines.append(f"{indent}{LLM_REQUEST_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_agent_run_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in AGENT_RUN_DATA_FIELDS[:-1])
    lines.append(f"{indent}{AGENT_RUN_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_agent_span_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in AGENT_SPAN_DATA_FIELDS[:-1])
    lines.append(f"{indent}{AGENT_SPAN_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_agent_tool_call_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in AGENT_TOOL_CALL_DATA_FIELDS[:-1])
    lines.append(f"{indent}{AGENT_TOOL_CALL_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_model_deployment_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in MODEL_DEPLOYMENT_DATA_FIELDS[:-1])
    lines.append(f"{indent}{MODEL_DEPLOYMENT_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_llm_request_paimon_bootstrap(indent: str = "            ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in LLM_REQUEST_DATA_FIELDS]
    lines.append(f"{indent}`date` DATE")
    return "\n".join(lines)


def render_llm_feature_request_1d_paimon_bootstrap(indent: str = "            ") -> str:
    lines = [f"{indent}{column}," for column in LLM_FEATURE_REQUEST_1D_Paimon_COLUMNS[:-1]]
    lines.append(f"{indent}{LLM_FEATURE_REQUEST_1D_Paimon_COLUMNS[-1]}")
    return "\n".join(lines)


def render_retrieval_request_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in RETRIEVAL_REQUEST_FIELDS[:-1]]
    lines.append(f"{indent}{RETRIEVAL_REQUEST_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (retrieval_id) NOT ENFORCED")
    return "\n".join(lines)


def render_retrieval_request_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in RETRIEVAL_REQUEST_FIELDS[:-1]]
    lines.append(f"{indent}{RETRIEVAL_REQUEST_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_retrieval_request_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in RETRIEVAL_REQUEST_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {RETRIEVAL_REQUEST_VALIDATION_RULES[0].expression}", *rules])


def render_retrieval_request_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in RETRIEVAL_REQUEST_DATA_FIELDS[:-1])
    lines.append(f"{indent}{RETRIEVAL_REQUEST_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_retrieval_knowledge_base_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in RETRIEVAL_KNOWLEDGE_BASE_REQUEST_1D_FLINK_COLUMNS]
    lines.append(
        f"{indent}PRIMARY KEY (`date`, app_name, knowledge_base_id, embedding_model, retrieval_strategy) NOT ENFORCED"
    )
    return "\n".join(lines)


def render_retrieval_knowledge_base_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in RETRIEVAL_KNOWLEDGE_BASE_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{RETRIEVAL_KNOWLEDGE_BASE_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_feedback_action_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in FEEDBACK_ACTION_FIELDS[:-1]]
    lines.append(f"{indent}{FEEDBACK_ACTION_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (feedback_id) NOT ENFORCED")
    return "\n".join(lines)


def render_feedback_action_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in FEEDBACK_ACTION_FIELDS[:-1]]
    lines.append(f"{indent}{FEEDBACK_ACTION_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_feedback_action_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in FEEDBACK_ACTION_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {FEEDBACK_ACTION_VALIDATION_RULES[0].expression}", *rules])


def render_feedback_action_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in FEEDBACK_ACTION_DATA_FIELDS[:-1])
    lines.append(f"{indent}{FEEDBACK_ACTION_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_feedback_feature_action_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in FEEDBACK_FEATURE_ACTION_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, app_name, feature_name, agent_id) NOT ENFORCED")
    return "\n".join(lines)


def render_feedback_feature_action_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in FEEDBACK_FEATURE_ACTION_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{FEEDBACK_FEATURE_ACTION_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_guardrail_check_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in GUARDRAIL_CHECK_FIELDS[:-1]]
    lines.append(f"{indent}{GUARDRAIL_CHECK_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (guardrail_event_id) NOT ENFORCED")
    return "\n".join(lines)


def render_guardrail_check_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in GUARDRAIL_CHECK_FIELDS[:-1]]
    lines.append(f"{indent}{GUARDRAIL_CHECK_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_guardrail_check_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in GUARDRAIL_CHECK_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {GUARDRAIL_CHECK_VALIDATION_RULES[0].expression}", *rules])


def render_guardrail_check_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in GUARDRAIL_CHECK_DATA_FIELDS[:-1])
    lines.append(f"{indent}{GUARDRAIL_CHECK_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_guardrail_rule_check_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in GUARDRAIL_RULE_CHECK_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, app_name, rule_category, action_taken) NOT ENFORCED")
    return "\n".join(lines)


def render_guardrail_rule_check_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in GUARDRAIL_RULE_CHECK_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{GUARDRAIL_RULE_CHECK_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_evaluation_judgment_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in EVALUATION_JUDGMENT_FIELDS[:-1]]
    lines.append(f"{indent}{EVALUATION_JUDGMENT_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (evaluation_id) NOT ENFORCED")
    return "\n".join(lines)


def render_evaluation_judgment_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in EVALUATION_JUDGMENT_FIELDS[:-1]]
    lines.append(f"{indent}{EVALUATION_JUDGMENT_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_evaluation_judgment_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in EVALUATION_JUDGMENT_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {EVALUATION_JUDGMENT_VALIDATION_RULES[0].expression}", *rules])


def render_evaluation_judgment_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in EVALUATION_JUDGMENT_DATA_FIELDS[:-1])
    lines.append(f"{indent}{EVALUATION_JUDGMENT_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_evaluation_feature_judgment_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in EVALUATION_FEATURE_JUDGMENT_1D_FLINK_COLUMNS]
    lines.append(
        f"{indent}PRIMARY KEY (`date`, app_name, feature_name, evaluation_dimension, evaluated_model_name) NOT ENFORCED"
    )
    return "\n".join(lines)


def render_evaluation_feature_judgment_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in EVALUATION_FEATURE_JUDGMENT_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{EVALUATION_FEATURE_JUDGMENT_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_compliance_access_audit_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in COMPLIANCE_ACCESS_AUDIT_FIELDS[:-1]]
    lines.append(f"{indent}{COMPLIANCE_ACCESS_AUDIT_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (audit_event_id) NOT ENFORCED")
    return "\n".join(lines)


def render_compliance_access_audit_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in COMPLIANCE_ACCESS_AUDIT_FIELDS[:-1]]
    lines.append(f"{indent}{COMPLIANCE_ACCESS_AUDIT_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_compliance_access_audit_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in COMPLIANCE_ACCESS_AUDIT_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {COMPLIANCE_ACCESS_AUDIT_VALIDATION_RULES[0].expression}", *rules])


def render_compliance_access_audit_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in COMPLIANCE_ACCESS_AUDIT_DATA_FIELDS[:-1])
    lines.append(f"{indent}{COMPLIANCE_ACCESS_AUDIT_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_compliance_data_retention_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in COMPLIANCE_DATA_RETENTION_FIELDS[:-1]]
    lines.append(f"{indent}{COMPLIANCE_DATA_RETENTION_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (retention_event_id) NOT ENFORCED")
    return "\n".join(lines)


def render_compliance_data_retention_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in COMPLIANCE_DATA_RETENTION_FIELDS[:-1]]
    lines.append(f"{indent}{COMPLIANCE_DATA_RETENTION_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_compliance_data_retention_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in COMPLIANCE_DATA_RETENTION_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {COMPLIANCE_DATA_RETENTION_VALIDATION_RULES[0].expression}", *rules])


def render_compliance_data_retention_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in COMPLIANCE_DATA_RETENTION_DATA_FIELDS[:-1])
    lines.append(f"{indent}{COMPLIANCE_DATA_RETENTION_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_agent_orchestration_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_column_definition()}," for field in AGENT_ORCHESTRATION_FIELDS[:-1]]
    lines.append(f"{indent}{AGENT_ORCHESTRATION_FIELDS[-1].flink_column_definition()},")
    lines.append(f"{indent}PRIMARY KEY (orchestration_id) NOT ENFORCED")
    return "\n".join(lines)


def render_agent_orchestration_flink_select(indent: str = "    ") -> str:
    lines = [f"{indent}{field.flink_select_expression()}," for field in AGENT_ORCHESTRATION_FIELDS[:-1]]
    lines.append(f"{indent}{AGENT_ORCHESTRATION_FIELDS[-1].flink_select_expression()}")
    return "\n".join(lines)


def render_agent_orchestration_where_clause(indent: str = "  ") -> str:
    rules = [f"{indent}AND {rule.expression}" for rule in AGENT_ORCHESTRATION_VALIDATION_RULES[1:]]
    return "\n".join([f"WHERE {AGENT_ORCHESTRATION_VALIDATION_RULES[0].expression}", *rules])


def render_agent_orchestration_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}`date` DATE NOT NULL,"]
    lines.extend(f"{indent}{field.doris_column_definition()}," for field in AGENT_ORCHESTRATION_DATA_FIELDS[:-1])
    lines.append(f"{indent}{AGENT_ORCHESTRATION_DATA_FIELDS[-1].doris_column_definition()}")
    return "\n".join(lines)


def render_agent_orchestration_handoff_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_ORCHESTRATION_HANDOFF_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, parent_agent_id, child_agent_id, handoff_type) NOT ENFORCED")
    return "\n".join(lines)


def render_agent_orchestration_handoff_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_ORCHESTRATION_HANDOFF_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{AGENT_ORCHESTRATION_HANDOFF_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_platform_component_health_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in PLATFORM_COMPONENT_HEALTH_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, component, metric_name) NOT ENFORCED")
    return "\n".join(lines)


def render_platform_component_health_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in PLATFORM_COMPONENT_HEALTH_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{PLATFORM_COMPONENT_HEALTH_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_cost_team_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in COST_TEAM_REQUEST_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, team_id, app_name, model_name) NOT ENFORCED")
    return "\n".join(lines)


def render_cost_team_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in COST_TEAM_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{COST_TEAM_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_prompt_version_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in PROMPT_VERSION_REQUEST_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, prompt_id, prompt_version, model_name) NOT ENFORCED")
    return "\n".join(lines)


def render_prompt_version_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in PROMPT_VERSION_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{PROMPT_VERSION_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_llm_feature_env_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_FEATURE_ENV_REQUEST_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, app_name, feature_name, model_name, environment) NOT ENFORCED")
    return "\n".join(lines)


def render_llm_feature_env_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_FEATURE_ENV_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{LLM_FEATURE_ENV_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_llm_region_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_REGION_REQUEST_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, region, environment, app_name, model_name) NOT ENFORCED")
    return "\n".join(lines)


def render_llm_region_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_REGION_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{LLM_REGION_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_agent_team_run_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_TEAM_RUN_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, team_id, app_name, agent_id, agent_name, task_type) NOT ENFORCED")
    return "\n".join(lines)


def render_agent_team_run_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_TEAM_RUN_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{AGENT_TEAM_RUN_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_llm_feature_request_1h_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_FEATURE_REQUEST_1H_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, `hour`, app_name, feature_name, model_name) NOT ENFORCED")
    return "\n".join(lines)


def render_llm_feature_request_1h_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_FEATURE_REQUEST_1H_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{LLM_FEATURE_REQUEST_1H_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_llm_session_request_1d_flink_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_SESSION_REQUEST_1D_FLINK_COLUMNS]
    lines.append(f"{indent}PRIMARY KEY (`date`, app_name, feature_name) NOT ENFORCED")
    return "\n".join(lines)


def render_llm_session_request_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in LLM_SESSION_REQUEST_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{LLM_SESSION_REQUEST_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_agent_run_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_RUN_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{AGENT_RUN_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)


def render_agent_tool_call_1d_doris_columns(indent: str = "    ") -> str:
    lines = [f"{indent}{column}," for column in AGENT_TOOL_CALL_1D_DORIS_COLUMNS[:-1]]
    lines.append(f"{indent}{AGENT_TOOL_CALL_1D_DORIS_COLUMNS[-1]}")
    return "\n".join(lines)
