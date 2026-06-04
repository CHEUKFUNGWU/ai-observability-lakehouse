import pytest
from pyspark.sql import SparkSession

from scripts.spark_transform_llm_events import (
    count_invalid_token_totals,
    transform_llm_events,
)

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-spark-transform-llm-events")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session
    session.stop()

def test_transform_llm_events_casts_expected_types(spark):
    raw_events = spark.createDataFrame(
        [
            {
                "request_id": "req_001",
                "user_id": "user_001",
                "session_id": "session_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "prompt_category": "support",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "hello",
                "response_text": "hi",
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
                "latency_ms": 800,
                "status": "success",
                "error_type": "",
                "http_status": 200,
                "estimated_cost_usd": 0.00001,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    events = transform_llm_events(raw_events)

    schema = dict(events.dtypes)

    assert schema["run_id"] == "string"
    assert schema["span_id"] == "string"
    assert schema["agent_id"] == "string"
    assert schema["agent_name"] == "string"
    assert schema["channel"] == "string"
    assert schema["prompt_tokens"] == "int"
    assert schema["completion_tokens"] == "int"
    assert schema["total_tokens"] == "int"
    assert schema["input_chars"] == "int"
    assert schema["output_chars"] == "int"
    assert schema["is_streaming"] == "boolean"
    assert schema["temperature"] == "double"
    assert schema["max_tokens"] == "int"
    assert schema["retry_count"] == "int"
    assert schema["latency_ms"] == "int"
    assert schema["http_status"] == "int"
    assert schema["estimated_cost_usd"] == "double"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"

def test_count_invalid_token_totals_returns_zero_for_valid_rows(spark):
    raw_events = spark.createDataFrame(
        [
            {
                "request_id": "req_001",
                "user_id": "user_001",
                "session_id": "session_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "prompt_category": "support",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "hello",
                "response_text": "hi",
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
                "latency_ms": 800,
                "status": "success",
                "error_type": "",
                "http_status": 200,
                "estimated_cost_usd": 0.00001,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    events = transform_llm_events(raw_events)

    assert count_invalid_token_totals(events) == 0

def test_count_invalid_token_totals_detects_invalid_rows(spark):
    raw_events = spark.createDataFrame(
        [
            {
                "request_id": "req_001",
                "user_id": "user_001",
                "session_id": "session_001",
                "app_name": "ai_support_bot",
                "feature_name": "chat",
                "prompt_category": "support",
                "prompt_id": "prompt_001",
                "prompt_version": "v1",
                "model_name": "deepseek-chat",
                "provider": "deepseek",
                "prompt_text": "hello",
                "response_text": "hi",
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 999,
                "latency_ms": 800,
                "status": "success",
                "error_type": "",
                "http_status": 200,
                "estimated_cost_usd": 0.00001,
                "mode": "mock",
                "region": "us",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    events = transform_llm_events(raw_events)

    assert count_invalid_token_totals(events) == 1
