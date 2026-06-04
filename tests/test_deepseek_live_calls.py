from datetime import datetime, timezone
from types import SimpleNamespace

from app.deepseek_client import DeepSeekCallResult
from scripts.run_deepseek_live_calls import (
    build_error_event,
    build_live_event,
    get_exception_http_status,
)


def test_build_live_event_maps_deepseek_result_to_llm_event():
    result = DeepSeekCallResult(
        prompt_text="hello",
        response_text="hi",
        model_name="deepseek-v4-pro",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        latency_ms=800,
        http_status=200,
    )

    event = build_live_event(
        result=result,
        prompt_id="prompt_live_001",
        prompt_version="v1",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    data = event.to_dict()

    assert data["provider"] == "deepseek"
    assert data["mode"] == "live"
    assert data["status"] == "success"
    assert data["run_id"]
    assert data["span_id"]
    assert data["agent_id"] == "agent_live_demo"
    assert data["agent_name"] == "live_demo_agent"
    assert data["channel"] == "api"
    assert data["prompt_text"] == "hello"
    assert data["response_text"] == "hi"
    assert data["input_chars"] == 5
    assert data["output_chars"] == 2
    assert data["request_type"] == "chat"
    assert data["is_streaming"] is False
    assert data["max_tokens"] == 0
    assert data["retry_count"] == 0
    assert data["finish_reason"] is None
    assert data["prompt_hash"]
    assert data["response_hash"]
    assert data["prompt_tokens"] == 10
    assert data["completion_tokens"] == 5
    assert data["total_tokens"] == 15
    assert data["latency_ms"] == 800
    assert data["http_status"] == 200
    assert data["prompt_id"] == "prompt_live_001"
    assert data["prompt_version"] == "v1"
    assert data["created_at"] == "2026-01-01T00:00:00+00:00"
    assert data["date"] == "2026-01-01"


def test_build_error_event_maps_failure_to_llm_event():
    event = build_error_event(
        prompt="hello",
        prompt_id="prompt_live_001",
        prompt_version="v1",
        model_name="deepseek-v4-flash",
        error_type="RateLimitError",
        http_status=429,
        latency_ms=1200,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    data = event.to_dict()

    assert data["provider"] == "deepseek"
    assert data["mode"] == "live"
    assert data["status"] == "error"
    assert data["run_id"]
    assert data["span_id"]
    assert data["agent_id"] == "agent_live_demo"
    assert data["agent_name"] == "live_demo_agent"
    assert data["channel"] == "api"
    assert data["error_type"] == "RateLimitError"
    assert data["http_status"] == 429
    assert data["prompt_text"] == "hello"
    assert data["response_text"] == ""
    assert data["input_chars"] == 5
    assert data["output_chars"] == 0
    assert data["request_type"] == "chat"
    assert data["is_streaming"] is False
    assert data["max_tokens"] == 0
    assert data["retry_count"] == 0
    assert data["finish_reason"] == "error"
    assert data["prompt_hash"]
    assert data["response_hash"]
    assert data["prompt_tokens"] == 0
    assert data["completion_tokens"] == 0
    assert data["total_tokens"] == 0
    assert data["estimated_cost_usd"] == 0.0
    assert data["latency_ms"] == 1200
    assert data["date"] == "2026-01-01"


def test_get_exception_http_status_reads_direct_status_code():
    error = SimpleNamespace(status_code=429)

    assert get_exception_http_status(error) == 429


def test_get_exception_http_status_reads_response_status_code():
    error = SimpleNamespace(response=SimpleNamespace(status_code=503))

    assert get_exception_http_status(error) == 503
