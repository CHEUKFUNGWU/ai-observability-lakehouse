from datetime import date, datetime, timezone

import pytest
from pyspark.sql import types as T

from scripts.spark_build_ads_trace_health_detail import build_trace_health_detail


def test_trace_health_flags_slow_orchestration_trace_without_child_facts(spark):
    result = build_trace_health_detail(
        _llm(spark, []),
        _runs(
            spark,
            [
                {
                    "date": date(2026, 6, 26),
                    "trace_id": "trace_slow_orchestration",
                    "run_id": "run_slow",
                    "app_name": "support_app",
                    "user_id": "user_hash_001",
                    "session_id": "session_001",
                    "agent_id": "agent_support",
                    "agent_name": "support_agent",
                    "task_type": "support",
                    "status": "success",
                    "duration_ms": 61000,
                    "estimated_cost_usd": 0.2,
                    "total_tokens": 700,
                    "llm_call_count": 0,
                    "tool_call_count": 0,
                    "retrieval_count": 0,
                }
            ],
        ),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["trace_id"] == "trace_slow_orchestration"
    assert row["is_slow_trace"] is True
    assert row["bottleneck_node_type"] == "orchestration"
    assert row["run_id"] == "run_slow"
    assert row["trace_latency_ms"] == 61000


def test_trace_health_flags_high_cost_trace_from_llm_requests(spark):
    result = build_trace_health_detail(
        _llm(
            spark,
            [
                _llm_row("trace_high_cost", "request_a", cost=0.7, latency_ms=800, prompt_hash="hash_prompt_a"),
                _llm_row("trace_high_cost", "request_b", cost=0.6, latency_ms=900, response_hash="hash_response_b"),
            ],
        ),
        _runs(spark, []),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["is_high_cost_trace"] is True
    assert row["trace_cost_usd"] == pytest.approx(1.3)
    assert row["bottleneck_node_type"] == "llm_generation"
    assert row["request_id"] == "request_b"
    assert row["prompt_hash"] in {"", "hash_prompt_a"}
    assert "prompt_text" not in row
    assert "response_text" not in row


def test_trace_health_uses_trace_envelope_bounds_for_sequential_children(spark):
    result = build_trace_health_detail(
        _llm(
            spark,
            [
                {
                    **_llm_row("trace_sequential", "request_a", cost=0.1, latency_ms=20000),
                    "created_at": datetime(2026, 6, 26, 9, 0, 0, tzinfo=timezone.utc),
                },
                {
                    **_llm_row("trace_sequential", "request_b", cost=0.1, latency_ms=20000),
                    "created_at": datetime(2026, 6, 26, 9, 0, 20, tzinfo=timezone.utc),
                },
            ],
        ),
        _runs(spark, []),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=30000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["trace_latency_ms"] == 40000
    assert row["is_slow_trace"] is True
    assert row["has_slow_child_observation"] is False


def test_trace_health_selects_failed_tool_child_observation(spark):
    result = build_trace_health_detail(
        _llm(spark, []),
        _runs(spark, []),
        _spans(spark, []),
        _tools(
            spark,
            [
                {
                    "date": date(2026, 6, 26),
                    "trace_id": "trace_failed_tool",
                    "run_id": "run_failed_tool",
                    "span_id": "span_tool_parent",
                    "tool_call_id": "tool_call_001",
                    "agent_id": "agent_support",
                    "tool_name": "ticket_lookup",
                    "status": "error",
                    "error_type": "timeout",
                    "duration_ms": 1200,
                    "result_size": 0,
                    "arguments_json": '{"ticket_id":"T-1"}',
                    "result_text": "raw tool response",
                }
            ],
        ),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["is_failed_trace"] is True
    assert row["has_failed_child_observation"] is True
    assert row["bottleneck_node_type"] == "tool_call"
    assert row["tool_call_id"] == "tool_call_001"
    assert row["bottleneck_error_type"] == "timeout"
    assert row["child_observation_summary"].startswith("tool_call:tool_call_001:error:timeout")
    assert "arguments_json" not in row
    assert "result_text" not in row


def test_trace_health_flags_slow_retrieval_child_observation(spark):
    result = build_trace_health_detail(
        _llm(spark, []),
        _runs(spark, []),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(
            spark,
            [
                {
                    "date": date(2026, 6, 26),
                    "trace_id": "trace_slow_retrieval",
                    "run_id": "run_retrieval",
                    "span_id": "span_retrieval",
                    "request_id": "request_parent",
                    "retrieval_id": "retrieval_001",
                    "app_name": "support_app",
                    "feature_name": "rag",
                    "user_id": "user_hash_002",
                    "agent_id": "agent_support",
                    "knowledge_base_id": "kb_docs",
                    "knowledge_base_name": "docs",
                    "embedding_model": "bge-large",
                    "query_text_hash": "query_hash_001",
                    "query_length": 28,
                    "returned_count": 3,
                    "status": "success",
                    "error_type": "",
                    "total_latency_ms": 6500,
                }
            ],
        ),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["has_slow_child_observation"] is True
    assert row["bottleneck_node_type"] == "retrieval"
    assert row["retrieval_id"] == "retrieval_001"
    assert row["query_text_hash"] == "query_hash_001"


def test_trace_health_keeps_rows_when_declared_child_facts_are_missing(spark):
    result = build_trace_health_detail(
        _llm(spark, []),
        _runs(
            spark,
            [
                {
                    "date": date(2026, 6, 26),
                    "trace_id": "trace_missing_children",
                    "run_id": "run_missing",
                    "app_name": "support_app",
                    "user_id": "user_hash_003",
                    "session_id": "session_003",
                    "agent_id": "agent_support",
                    "agent_name": "support_agent",
                    "task_type": "support",
                    "status": "success",
                    "duration_ms": 1000,
                    "estimated_cost_usd": 0.1,
                    "total_tokens": 120,
                    "llm_call_count": 1,
                    "tool_call_count": 1,
                    "retrieval_count": 1,
                }
            ],
        ),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["has_missing_child_facts"] is True
    assert row["declared_llm_call_count"] == 1
    assert row["observed_llm_request_count"] == 0
    assert row["declared_tool_call_count"] == 1
    assert row["observed_tool_call_count"] == 0
    assert row["declared_retrieval_count"] == 1
    assert row["observed_retrieval_count"] == 0


def test_trace_health_output_is_privacy_safe(spark):
    result = build_trace_health_detail(
        _llm(
            spark,
            [
                {
                    **_llm_row("trace_privacy", "request_privacy", cost=1.2, latency_ms=700),
                    "prompt_text": "raw prompt should not appear",
                    "response_text": "raw response should not appear",
                    "prompt_hash": "prompt_hash_safe",
                    "response_hash": "response_hash_safe",
                }
            ],
        ),
        _runs(spark, []),
        _spans(spark, []),
        _tools(spark, []),
        _retrievals(spark, []),
        slow_trace_ms=30000,
        slow_child_ms=5000,
        high_cost_usd=1.0,
    )

    row = result.collect()[0].asDict()

    assert row["prompt_hash"] == "prompt_hash_safe"
    assert row["response_hash"] == "response_hash_safe"
    forbidden_columns = {"prompt_text", "response_text", "arguments_json", "result_text", "input_text_hash", "output_text_hash"}
    assert forbidden_columns.isdisjoint(row)
    assert "raw prompt" not in str(row)
    assert "raw response" not in str(row)


def _llm_row(trace_id: str, request_id: str, cost: float, latency_ms: int, prompt_hash: str = "", response_hash: str = ""):
    return {
        "date": date(2026, 6, 26),
        "trace_id": trace_id,
        "run_id": "",
        "span_id": "",
        "request_id": request_id,
        "app_name": "support_app",
        "feature_name": "chat",
        "user_id": "user_hash",
        "session_id": "session",
        "agent_id": "agent_support",
        "agent_name": "support_agent",
        "model_name": "gpt-4o-mini",
        "provider": "openai",
        "status": "success",
        "error_type": "",
        "latency_ms": latency_ms,
        "estimated_cost_usd": cost,
        "input_chars": 128,
        "output_chars": 256,
        "prompt_hash": prompt_hash,
        "response_hash": response_hash,
        "total_tokens": 1000,
    }


def _llm(spark, rows: list[dict]):
    return spark.createDataFrame(rows, schema=_schema(_LLM_FIELDS))


def _runs(spark, rows: list[dict]):
    return spark.createDataFrame(rows, schema=_schema(_RUN_FIELDS))


def _spans(spark, rows: list[dict]):
    return spark.createDataFrame(rows, schema=_schema(_SPAN_FIELDS))


def _tools(spark, rows: list[dict]):
    return spark.createDataFrame(rows, schema=_schema(_TOOL_FIELDS))


def _retrievals(spark, rows: list[dict]):
    return spark.createDataFrame(rows, schema=_schema(_RETRIEVAL_FIELDS))


def _schema(fields: dict[str, T.DataType]):
    return T.StructType([T.StructField(name, data_type, True) for name, data_type in fields.items()])


_LLM_FIELDS = {
    "date": T.DateType(),
    "trace_id": T.StringType(),
    "run_id": T.StringType(),
    "span_id": T.StringType(),
    "request_id": T.StringType(),
    "app_name": T.StringType(),
    "feature_name": T.StringType(),
    "user_id": T.StringType(),
    "session_id": T.StringType(),
    "agent_id": T.StringType(),
    "agent_name": T.StringType(),
    "model_name": T.StringType(),
    "provider": T.StringType(),
    "status": T.StringType(),
    "error_type": T.StringType(),
    "latency_ms": T.LongType(),
    "estimated_cost_usd": T.DoubleType(),
    "input_chars": T.LongType(),
    "output_chars": T.LongType(),
    "prompt_hash": T.StringType(),
    "response_hash": T.StringType(),
    "total_tokens": T.LongType(),
    "created_at": T.TimestampType(),
    "prompt_text": T.StringType(),
    "response_text": T.StringType(),
}

_RUN_FIELDS = {
    "date": T.DateType(),
    "trace_id": T.StringType(),
    "run_id": T.StringType(),
    "app_name": T.StringType(),
    "user_id": T.StringType(),
    "session_id": T.StringType(),
    "agent_id": T.StringType(),
    "agent_name": T.StringType(),
    "task_type": T.StringType(),
    "status": T.StringType(),
    "error_type": T.StringType(),
    "duration_ms": T.LongType(),
    "estimated_cost_usd": T.DoubleType(),
    "total_tokens": T.LongType(),
    "llm_call_count": T.LongType(),
    "tool_call_count": T.LongType(),
    "retrieval_count": T.LongType(),
    "input_text_hash": T.StringType(),
    "output_text_hash": T.StringType(),
}

_SPAN_FIELDS = {
    "date": T.DateType(),
    "trace_id": T.StringType(),
    "run_id": T.StringType(),
    "span_id": T.StringType(),
    "agent_id": T.StringType(),
    "span_name": T.StringType(),
    "span_type": T.StringType(),
    "status": T.StringType(),
    "error_type": T.StringType(),
    "duration_ms": T.LongType(),
    "input_size": T.LongType(),
    "output_size": T.LongType(),
    "model_name": T.StringType(),
}

_TOOL_FIELDS = {
    "date": T.DateType(),
    "trace_id": T.StringType(),
    "run_id": T.StringType(),
    "span_id": T.StringType(),
    "tool_call_id": T.StringType(),
    "agent_id": T.StringType(),
    "tool_name": T.StringType(),
    "status": T.StringType(),
    "error_type": T.StringType(),
    "duration_ms": T.LongType(),
    "result_size": T.LongType(),
    "arguments_json": T.StringType(),
    "result_text": T.StringType(),
}

_RETRIEVAL_FIELDS = {
    "date": T.DateType(),
    "trace_id": T.StringType(),
    "run_id": T.StringType(),
    "span_id": T.StringType(),
    "request_id": T.StringType(),
    "retrieval_id": T.StringType(),
    "app_name": T.StringType(),
    "feature_name": T.StringType(),
    "user_id": T.StringType(),
    "agent_id": T.StringType(),
    "knowledge_base_id": T.StringType(),
    "knowledge_base_name": T.StringType(),
    "embedding_model": T.StringType(),
    "query_text_hash": T.StringType(),
    "query_length": T.LongType(),
    "returned_count": T.LongType(),
    "status": T.StringType(),
    "error_type": T.StringType(),
    "total_latency_ms": T.LongType(),
}
