import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_paimon_spark_session


DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_trace_health_detail.parquet")
DEFAULT_LLM_TABLE = "paimon_lake.dwd.dwd_ai_llm_request_di"
DEFAULT_AGENT_RUN_TABLE = "paimon_lake.dwd.dwd_ai_agent_run_di"
DEFAULT_AGENT_SPAN_TABLE = "paimon_lake.dwd.dwd_ai_agent_span_di"
DEFAULT_TOOL_CALL_TABLE = "paimon_lake.dwd.dwd_ai_agent_tool_call_di"
DEFAULT_RETRIEVAL_TABLE = "paimon_lake.dwd.dwd_ai_retrieval_request_di"
LOGGER = get_logger(__name__)


def build_trace_health_detail(
    llm_requests: DataFrame,
    agent_runs: DataFrame,
    agent_spans: DataFrame,
    tool_calls: DataFrame,
    retrieval_requests: DataFrame,
    slow_trace_ms: int = 30000,
    slow_child_ms: int = 5000,
    high_cost_usd: float = 1.0,
) -> DataFrame:
    candidates = _build_candidates(llm_requests, agent_runs, agent_spans, tool_calls, retrieval_requests)
    trace_summary = _build_trace_summary(candidates, agent_runs, slow_trace_ms, slow_child_ms, high_cost_usd)
    selected_bottlenecks = _select_bottlenecks(candidates, slow_child_ms, high_cost_usd)

    return (
        trace_summary.join(selected_bottlenecks, on="trace_id", how="left")
        .withColumn(
            "child_observation_summary",
            F.concat_ws(
                ":",
                F.col("bottleneck_node_type"),
                F.col("bottleneck_node_id"),
                F.col("bottleneck_status"),
                F.coalesce(F.col("bottleneck_error_type"), F.lit("")),
                F.concat(F.coalesce(F.col("bottleneck_latency_ms").cast("string"), F.lit("0")), F.lit("ms")),
            ),
        )
        .filter(
            F.col("is_high_cost_trace")
            | F.col("is_slow_trace")
            | F.col("is_failed_trace")
            | F.col("has_failed_child_observation")
            | F.col("has_slow_child_observation")
            | F.col("has_missing_child_facts")
        )
        .select(
            "date",
            "trace_id",
            "run_id",
            "span_id",
            "request_id",
            "tool_call_id",
            "retrieval_id",
            "app_name",
            "feature_name",
            "user_id",
            "session_id",
            "agent_id",
            "agent_name",
            "model_name",
            "provider",
            "knowledge_base_id",
            "bottleneck_node_type",
            "bottleneck_node_id",
            "bottleneck_name",
            "bottleneck_status",
            "bottleneck_error_type",
            "bottleneck_latency_ms",
            "bottleneck_cost_usd",
            "bottleneck_input_size",
            "bottleneck_output_size",
            "prompt_hash",
            "response_hash",
            "query_text_hash",
            "trace_latency_ms",
            "trace_cost_usd",
            "trace_total_tokens",
            "trace_status",
            "is_high_cost_trace",
            "is_slow_trace",
            "is_failed_trace",
            "has_failed_child_observation",
            "has_slow_child_observation",
            "has_missing_child_facts",
            "declared_llm_call_count",
            "observed_llm_request_count",
            "declared_tool_call_count",
            "observed_tool_call_count",
            "declared_retrieval_count",
            "observed_retrieval_count",
            "child_observation_summary",
        )
    )


def _build_candidates(
    llm_requests: DataFrame,
    agent_runs: DataFrame,
    agent_spans: DataFrame,
    tool_calls: DataFrame,
    retrieval_requests: DataFrame,
) -> DataFrame:
    llm = _llm_candidates(llm_requests)
    runs = _run_candidates(agent_runs)
    spans = _span_candidates(agent_spans)
    tools = _tool_candidates(tool_calls)
    retrievals = _retrieval_candidates(retrieval_requests)
    return (
        llm.unionByName(runs)
        .unionByName(spans)
        .unionByName(tools)
        .unionByName(retrievals)
        .filter(F.col("trace_id").isNotNull() & (F.col("trace_id") != ""))
    )


def _llm_candidates(rows: DataFrame) -> DataFrame:
    return rows.select(
        _as_date(rows, "date"),
        _str_col(rows, "trace_id"),
        _str_col(rows, "run_id"),
        _str_col(rows, "span_id"),
        _str_col(rows, "request_id"),
        _empty("tool_call_id"),
        _empty("retrieval_id"),
        _str_col(rows, "app_name"),
        _str_col(rows, "feature_name"),
        _str_col(rows, "user_id"),
        _str_col(rows, "session_id"),
        _str_col(rows, "agent_id"),
        _str_col(rows, "agent_name"),
        _str_col(rows, "model_name"),
        _str_col(rows, "provider"),
        _empty("knowledge_base_id"),
        F.lit("llm_generation").alias("node_type"),
        _str_col(rows, "request_id").alias("node_id"),
        _str_col(rows, "model_name").alias("node_name"),
        _str_col(rows, "status"),
        _str_col(rows, "error_type"),
        _long_col(rows, "latency_ms"),
        _double_col(rows, "estimated_cost_usd"),
        _long_col(rows, "input_chars").alias("input_size"),
        _long_col(rows, "output_chars").alias("output_size"),
        _str_col(rows, "prompt_hash"),
        _str_col(rows, "response_hash"),
        _empty("query_text_hash"),
        _long_col(rows, "total_tokens"),
        _timestamp_col(rows, "created_at", "node_start_time"),
        _end_timestamp_col(rows, "created_at", "latency_ms", alias="node_end_time"),
    )


def _run_candidates(rows: DataFrame) -> DataFrame:
    return rows.select(
        _as_date(rows, "date"),
        _str_col(rows, "trace_id"),
        _str_col(rows, "run_id"),
        _empty("span_id"),
        _empty("request_id"),
        _empty("tool_call_id"),
        _empty("retrieval_id"),
        _str_col(rows, "app_name"),
        _empty("feature_name"),
        _str_col(rows, "user_id"),
        _str_col(rows, "session_id"),
        _str_col(rows, "agent_id"),
        _str_col(rows, "agent_name"),
        _empty("model_name"),
        _empty("provider"),
        _empty("knowledge_base_id"),
        F.lit("orchestration").alias("node_type"),
        _str_col(rows, "run_id").alias("node_id"),
        _str_col(rows, "task_type").alias("node_name"),
        _str_col(rows, "status"),
        _str_col(rows, "error_type"),
        _long_col(rows, "duration_ms").alias("latency_ms"),
        _double_col(rows, "estimated_cost_usd"),
        F.lit(0).cast("bigint").alias("input_size"),
        F.lit(0).cast("bigint").alias("output_size"),
        _empty("prompt_hash"),
        _empty("response_hash"),
        _empty("query_text_hash"),
        _long_col(rows, "total_tokens"),
        _timestamp_col(rows, "start_time", "node_start_time"),
        _end_timestamp_col(rows, "start_time", "duration_ms", "end_time", "node_end_time"),
    )


def _span_candidates(rows: DataFrame) -> DataFrame:
    return rows.select(
        _as_date(rows, "date"),
        _str_col(rows, "trace_id"),
        _str_col(rows, "run_id"),
        _str_col(rows, "span_id"),
        _empty("request_id"),
        _empty("tool_call_id"),
        _empty("retrieval_id"),
        _empty("app_name"),
        _empty("feature_name"),
        _empty("user_id"),
        _empty("session_id"),
        _str_col(rows, "agent_id"),
        _empty("agent_name"),
        _str_col(rows, "model_name"),
        _empty("provider"),
        _empty("knowledge_base_id"),
        F.lit("agent_span").alias("node_type"),
        _str_col(rows, "span_id").alias("node_id"),
        _str_col(rows, "span_name").alias("node_name"),
        _str_col(rows, "status"),
        _str_col(rows, "error_type"),
        _long_col(rows, "duration_ms").alias("latency_ms"),
        F.lit(0.0).cast("double").alias("estimated_cost_usd"),
        _long_col(rows, "input_size"),
        _long_col(rows, "output_size"),
        _empty("prompt_hash"),
        _empty("response_hash"),
        _empty("query_text_hash"),
        F.lit(0).cast("bigint").alias("total_tokens"),
        _timestamp_col(rows, "start_time", "node_start_time"),
        _end_timestamp_col(rows, "start_time", "duration_ms", "end_time", "node_end_time"),
    )


def _tool_candidates(rows: DataFrame) -> DataFrame:
    return rows.select(
        _as_date(rows, "date"),
        _str_col(rows, "trace_id"),
        _str_col(rows, "run_id"),
        _str_col(rows, "span_id"),
        _empty("request_id"),
        _str_col(rows, "tool_call_id"),
        _empty("retrieval_id"),
        _empty("app_name"),
        _empty("feature_name"),
        _empty("user_id"),
        _empty("session_id"),
        _str_col(rows, "agent_id"),
        _empty("agent_name"),
        _empty("model_name"),
        _empty("provider"),
        _empty("knowledge_base_id"),
        F.lit("tool_call").alias("node_type"),
        _str_col(rows, "tool_call_id").alias("node_id"),
        _str_col(rows, "tool_name").alias("node_name"),
        _str_col(rows, "status"),
        _str_col(rows, "error_type"),
        _long_col(rows, "duration_ms").alias("latency_ms"),
        F.lit(0.0).cast("double").alias("estimated_cost_usd"),
        F.lit(0).cast("bigint").alias("input_size"),
        _long_col(rows, "result_size").alias("output_size"),
        _empty("prompt_hash"),
        _empty("response_hash"),
        _empty("query_text_hash"),
        F.lit(0).cast("bigint").alias("total_tokens"),
        _timestamp_col(rows, "created_at", "node_start_time"),
        _end_timestamp_col(rows, "created_at", "duration_ms", alias="node_end_time"),
    )


def _retrieval_candidates(rows: DataFrame) -> DataFrame:
    return rows.select(
        _as_date(rows, "date"),
        _str_col(rows, "trace_id"),
        _str_col(rows, "run_id"),
        _str_col(rows, "span_id"),
        _str_col(rows, "request_id"),
        _empty("tool_call_id"),
        _str_col(rows, "retrieval_id"),
        _str_col(rows, "app_name"),
        _str_col(rows, "feature_name"),
        _str_col(rows, "user_id"),
        _empty("session_id"),
        _str_col(rows, "agent_id"),
        _empty("agent_name"),
        _str_col(rows, "embedding_model").alias("model_name"),
        _empty("provider"),
        _str_col(rows, "knowledge_base_id"),
        F.lit("retrieval").alias("node_type"),
        _str_col(rows, "retrieval_id").alias("node_id"),
        _str_col(rows, "knowledge_base_name").alias("node_name"),
        _str_col(rows, "status"),
        _str_col(rows, "error_type"),
        _long_col(rows, "total_latency_ms").alias("latency_ms"),
        F.lit(0.0).cast("double").alias("estimated_cost_usd"),
        _long_col(rows, "query_length").alias("input_size"),
        _long_col(rows, "returned_count").alias("output_size"),
        _empty("prompt_hash"),
        _empty("response_hash"),
        _str_col(rows, "query_text_hash"),
        F.lit(0).cast("bigint").alias("total_tokens"),
        _timestamp_col(rows, "created_at", "node_start_time"),
        _end_timestamp_col(rows, "created_at", "total_latency_ms", alias="node_end_time"),
    )


def _build_trace_summary(
    candidates: DataFrame,
    agent_runs: DataFrame,
    slow_trace_ms: int,
    slow_child_ms: int,
    high_cost_usd: float,
) -> DataFrame:
    candidate_summary = candidates.groupBy("trace_id").agg(
        F.min("date").alias("date"),
        _first_non_empty("run_id").alias("run_id"),
        _first_non_empty("app_name").alias("app_name"),
        _first_non_empty("feature_name").alias("feature_name"),
        _first_non_empty("user_id").alias("user_id"),
        _first_non_empty("session_id").alias("session_id"),
        _first_non_empty("agent_id").alias("agent_id"),
        _first_non_empty("agent_name").alias("agent_name"),
        F.max(F.coalesce(F.col("latency_ms"), F.lit(0))).cast("bigint").alias("max_node_latency_ms"),
        F.min("node_start_time").alias("trace_start_time"),
        F.max("node_end_time").alias("trace_end_time"),
        F.greatest(
            F.sum(F.when(F.col("node_type") == "llm_generation", F.coalesce(F.col("estimated_cost_usd"), F.lit(0.0))).otherwise(F.lit(0.0))),
            F.max(F.when(F.col("node_type") == "orchestration", F.coalesce(F.col("estimated_cost_usd"), F.lit(0.0))).otherwise(F.lit(0.0))),
        ).alias("trace_cost_usd"),
        F.greatest(
            F.sum(F.when(F.col("node_type") == "llm_generation", F.coalesce(F.col("total_tokens"), F.lit(0))).otherwise(F.lit(0))),
            F.max(F.when(F.col("node_type") == "orchestration", F.coalesce(F.col("total_tokens"), F.lit(0))).otherwise(F.lit(0))),
        ).cast("bigint").alias("trace_total_tokens"),
        F.max(F.when(_is_failed(F.col("status")), F.lit(1)).otherwise(F.lit(0))).alias("failed_trace_flag"),
        F.max(
            F.when((F.col("node_type") != "orchestration") & _is_failed(F.col("status")), F.lit(1)).otherwise(F.lit(0))
        ).alias("failed_child_flag"),
        F.max(
            F.when(
                (F.col("node_type") != "orchestration") & (F.coalesce(F.col("latency_ms"), F.lit(0)) >= slow_child_ms),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("slow_child_flag"),
        F.sum(F.when(F.col("node_type") == "llm_generation", F.lit(1)).otherwise(F.lit(0))).cast("bigint").alias(
            "observed_llm_request_count"
        ),
        F.sum(F.when(F.col("node_type") == "tool_call", F.lit(1)).otherwise(F.lit(0))).cast("bigint").alias(
            "observed_tool_call_count"
        ),
        F.sum(F.when(F.col("node_type") == "retrieval", F.lit(1)).otherwise(F.lit(0))).cast("bigint").alias(
            "observed_retrieval_count"
        ),
    )

    declared = agent_runs.filter(F.col("trace_id").isNotNull() & (F.col("trace_id") != "")).groupBy("trace_id").agg(
        F.sum(F.coalesce(_existing_long_col(agent_runs, "llm_call_count"), F.lit(0))).cast("bigint").alias(
            "declared_llm_call_count"
        ),
        F.sum(F.coalesce(_existing_long_col(agent_runs, "tool_call_count"), F.lit(0))).cast("bigint").alias(
            "declared_tool_call_count"
        ),
        F.sum(F.coalesce(_existing_long_col(agent_runs, "retrieval_count"), F.lit(0))).cast("bigint").alias(
            "declared_retrieval_count"
        ),
    )

    return (
        candidate_summary.join(declared, on="trace_id", how="left")
        .fillna(
            {
                "declared_llm_call_count": 0,
                "declared_tool_call_count": 0,
                "declared_retrieval_count": 0,
                "observed_llm_request_count": 0,
                "observed_tool_call_count": 0,
                "observed_retrieval_count": 0,
            }
        )
        .withColumn(
            "trace_latency_ms",
            F.when(
                F.col("trace_start_time").isNotNull()
                & F.col("trace_end_time").isNotNull()
                & (F.col("trace_end_time") >= F.col("trace_start_time")),
                F.unix_millis("trace_end_time") - F.unix_millis("trace_start_time"),
            )
            .otherwise(F.col("max_node_latency_ms"))
            .cast("bigint"),
        )
        .withColumn("is_high_cost_trace", F.col("trace_cost_usd") >= F.lit(high_cost_usd))
        .withColumn("is_slow_trace", F.col("trace_latency_ms") >= F.lit(slow_trace_ms))
        .withColumn("is_failed_trace", F.col("failed_trace_flag") > 0)
        .withColumn("has_failed_child_observation", F.col("failed_child_flag") > 0)
        .withColumn("has_slow_child_observation", F.col("slow_child_flag") > 0)
        .withColumn(
            "has_missing_child_facts",
            (F.col("declared_llm_call_count") > F.col("observed_llm_request_count"))
            | (F.col("declared_tool_call_count") > F.col("observed_tool_call_count"))
            | (F.col("declared_retrieval_count") > F.col("observed_retrieval_count")),
        )
        .withColumn("trace_status", F.when(F.col("is_failed_trace"), F.lit("error")).otherwise(F.lit("success")))
        .drop(
            "failed_trace_flag",
            "failed_child_flag",
            "slow_child_flag",
            "max_node_latency_ms",
            "trace_start_time",
            "trace_end_time",
        )
    )


def _select_bottlenecks(candidates: DataFrame, slow_child_ms: int, high_cost_usd: float) -> DataFrame:
    scored = (
        candidates.withColumn("is_failed_node", _is_failed(F.col("status")))
        .withColumn("is_slow_node", F.coalesce(F.col("latency_ms"), F.lit(0)) >= F.lit(slow_child_ms))
        .withColumn("is_high_cost_node", F.coalesce(F.col("estimated_cost_usd"), F.lit(0.0)) >= F.lit(high_cost_usd))
        .withColumn(
            "rank_score",
            F.when(F.col("is_failed_node"), F.lit(3))
            .when(F.col("is_slow_node"), F.lit(2))
            .when(F.col("is_high_cost_node"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("trace_id").orderBy(
                    F.col("rank_score").desc(),
                    F.coalesce(F.col("latency_ms"), F.lit(0)).desc(),
                    F.coalesce(F.col("estimated_cost_usd"), F.lit(0.0)).desc(),
                    F.col("node_type").asc(),
                    F.col("node_id").asc(),
                )
            ),
        )
    )
    return scored.filter(F.col("rn") == 1).select(
        "trace_id",
        F.col("span_id"),
        F.col("request_id"),
        F.col("tool_call_id"),
        F.col("retrieval_id"),
        F.col("model_name"),
        F.col("provider"),
        F.col("knowledge_base_id"),
        F.col("node_type").alias("bottleneck_node_type"),
        F.col("node_id").alias("bottleneck_node_id"),
        F.col("node_name").alias("bottleneck_name"),
        F.col("status").alias("bottleneck_status"),
        F.col("error_type").alias("bottleneck_error_type"),
        F.col("latency_ms").cast("bigint").alias("bottleneck_latency_ms"),
        F.col("estimated_cost_usd").cast("double").alias("bottleneck_cost_usd"),
        F.col("input_size").cast("bigint").alias("bottleneck_input_size"),
        F.col("output_size").cast("bigint").alias("bottleneck_output_size"),
        F.col("prompt_hash"),
        F.col("response_hash"),
        F.col("query_text_hash"),
    )


def _is_failed(status_col):
    return F.lower(F.coalesce(status_col, F.lit(""))).isin("error", "failed", "timeout")


def _first_non_empty(column_name: str):
    return F.first(F.when(F.col(column_name).isNotNull() & (F.col(column_name) != ""), F.col(column_name)), True)


def _empty(alias: str):
    return F.lit("").cast("string").alias(alias)


def _str_col(frame: DataFrame, name: str):
    if name in frame.columns:
        return F.coalesce(F.col(name).cast("string"), F.lit("")).alias(name)
    return _empty(name)


def _long_col(frame: DataFrame, name: str):
    if name in frame.columns:
        return F.coalesce(F.col(name).cast("bigint"), F.lit(0)).alias(name)
    return F.lit(0).cast("bigint").alias(name)


def _double_col(frame: DataFrame, name: str):
    if name in frame.columns:
        return F.coalesce(F.col(name).cast("double"), F.lit(0.0)).alias(name)
    return F.lit(0.0).cast("double").alias(name)


def _existing_long_col(frame: DataFrame, name: str):
    if name in frame.columns:
        return F.col(name).cast("bigint")
    return F.lit(0).cast("bigint")


def _as_date(frame: DataFrame, name: str):
    if name in frame.columns:
        return F.to_date(F.col(name)).alias(name)
    return F.lit(None).cast("date").alias(name)


def _timestamp_col(frame: DataFrame, name: str, alias: str):
    if name in frame.columns:
        return F.col(name).cast("timestamp").alias(alias)
    return F.lit(None).cast("timestamp").alias(alias)


def _end_timestamp_col(
    frame: DataFrame,
    start_name: str,
    duration_name: str,
    end_name: str | None = None,
    alias: str = "node_end_time",
):
    candidates = []
    if end_name and end_name in frame.columns:
        candidates.append(F.col(end_name).cast("timestamp"))
    if start_name in frame.columns and duration_name in frame.columns:
        start_time = F.col(start_name).cast("timestamp")
        duration_ms = F.coalesce(F.col(duration_name).cast("bigint"), F.lit(0))
        candidates.append(F.timestamp_millis(F.unix_millis(start_time) + duration_ms))
    if not candidates:
        return F.lit(None).cast("timestamp").alias(alias)
    return F.coalesce(*candidates).alias(alias)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-table", type=str, default=DEFAULT_LLM_TABLE)
    parser.add_argument("--agent-run-table", type=str, default=DEFAULT_AGENT_RUN_TABLE)
    parser.add_argument("--agent-span-table", type=str, default=DEFAULT_AGENT_SPAN_TABLE)
    parser.add_argument("--tool-call-table", type=str, default=DEFAULT_TOOL_CALL_TABLE)
    parser.add_argument("--retrieval-table", type=str, default=DEFAULT_RETRIEVAL_TABLE)
    parser.add_argument("--slow-trace-ms", type=int, default=30000)
    parser.add_argument("--slow-child-ms", type=int, default=5000)
    parser.add_argument("--high-cost-usd", type=float, default=1.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    from datetime import datetime, timezone

    started_at = datetime.now(timezone.utc)
    spark = build_paimon_spark_session("ai-observability-ads-trace-health-detail")
    try:
        llm_requests = spark.table(args.llm_table)
        agent_runs = spark.table(args.agent_run_table)
        agent_spans = spark.table(args.agent_span_table)
        tool_calls = spark.table(args.tool_call_table)
        retrieval_requests = spark.table(args.retrieval_table)
        result = build_trace_health_detail(
            llm_requests,
            agent_runs,
            agent_spans,
            tool_calls,
            retrieval_requests,
            slow_trace_ms=args.slow_trace_ms,
            slow_child_ms=args.slow_child_ms,
            high_cost_usd=args.high_cost_usd,
        )
        result.write.mode("overwrite").partitionBy("date").parquet(str(args.output))
        row_count = result.count()
        log_info(LOGGER, "ads_trace_health_detail_written", output=str(args.output), rows=row_count)
        append_pipeline_run(
            pipeline_name="spark_build_ads_trace_health_detail",
            layer="ads",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=sum(
                frame.count()
                for frame in (llm_requests, agent_runs, agent_spans, tool_calls, retrieval_requests)
            ),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
