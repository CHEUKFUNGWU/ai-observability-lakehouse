from datetime import datetime

from scripts.spark_transform_agent_events import (
    transform_agent_run_events,
    transform_agent_span_events,
)


def test_transform_agent_run_events_casts_expected_types(spark):
    raw_runs = spark.createDataFrame(
        [
            {
                "run_id": "run_001",
                "trace_id": "trace_001",
                "agent_id": "agent_support",
                "agent_name": "customer_support_agent",
                "agent_version": "v1",
                "app_name": "ai_agent_platform",
                "user_id": "user_001",
                "session_id": "session_001",
                "conversation_id": "conv_001",
                "task_type": "customer_support",
                "channel": "web",
                "input_text_hash": "in_hash",
                "output_text_hash": "out_hash",
                "start_time": "2026-01-01T00:00:00+00:00",
                "end_time": "2026-01-01T00:00:01+00:00",
                "duration_ms": 1000,
                "status": "success",
                "error_type": "",
                "turn_count": 1,
                "llm_call_count": 1,
                "tool_call_count": 1,
                "retrieval_count": 1,
                "total_tokens": 100,
                "estimated_cost_usd": 0.0001,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    runs = transform_agent_run_events(raw_runs)
    schema = dict(runs.dtypes)

    assert schema["duration_ms"] == "int"
    assert schema["turn_count"] == "int"
    assert schema["total_tokens"] == "int"
    assert schema["estimated_cost_usd"] == "double"
    assert schema["start_time"] == "timestamp"
    assert schema["date"] == "date"


def test_transform_agent_span_events_casts_expected_types(spark):
    raw_spans = spark.createDataFrame(
        [
            {
                "span_id": "span_001",
                "parent_span_id": "",
                "run_id": "run_001",
                "trace_id": "trace_001",
                "agent_id": "agent_support",
                "span_name": "tool_call",
                "span_type": "tool_call",
                "span_order": 1,
                "start_time": "2026-01-01T00:00:00+00:00",
                "end_time": "2026-01-01T00:00:01+00:00",
                "duration_ms": 1000,
                "status": "success",
                "error_type": "",
                "retry_count": 0,
                "input_size": 100,
                "output_size": 200,
                "model_name": "",
                "tool_name": "order_lookup",
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    spans = transform_agent_span_events(raw_spans)
    schema = dict(spans.dtypes)

    assert schema["span_order"] == "int"
    assert schema["duration_ms"] == "int"
    assert schema["retry_count"] == "int"
    assert schema["input_size"] == "int"
    assert schema["start_time"] == "timestamp"
    assert schema["date"] == "date"
