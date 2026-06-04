import json
from datetime import datetime

from scripts.generate_mock_llm_logs import build_mock_event, write_jsonl

REQUIRED_FIELDS = {
    "request_id",
    "trace_id",
    "run_id",
    "span_id",
    "agent_id",
    "agent_name",
    "channel",
    "user_id",
    "session_id",
    "conversation_id",
    "app_name",
    "feature_name",
    "prompt_category",
    "prompt_id",
    "prompt_version",
    "model_name",
    "provider",
    "prompt_text",
    "response_text",
    "prompt_hash",
    "response_hash",
    "input_chars",
    "output_chars",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "request_type",
    "is_streaming",
    "temperature",
    "max_tokens",
    "finish_reason",
    "retry_count",
    "latency_ms",
    "status",
    "error_type",
    "http_status",
    "estimated_cost_usd",
    "mode",
    "region",
    "environment",
    "created_at",
    "date",
}

def test_build_mock_event_has_required_fields():
    event = build_mock_event()
    data = event.to_dict()

    assert REQUIRED_FIELDS.issubset(data.keys())

def test_build_mock_event_token_totals_are_consistent():
    event = build_mock_event()
    data = event.to_dict()

    assert data["total_tokens"] == data["prompt_tokens"] + data["completion_tokens"]

def test_build_mock_event_date_matches_created_at():
    event = build_mock_event()
    data = event.to_dict()

    created_at = datetime.fromisoformat(data["created_at"])
    assert data["date"] == created_at.date().isoformat()

def test_write_jsonl_writes_expected_number_of_lines(tmp_path):
    output_path = tmp_path / "events.jsonl"
    write_jsonl(count=3, output_path=output_path)

    lines = output_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 3

    for line in lines:
        data = json.loads(line)
        assert REQUIRED_FIELDS.issubset(data.keys())
