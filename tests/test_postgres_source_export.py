import csv
import json
from io import StringIO

from scripts.export_llm_jsonl_to_postgres_copy import COPY_COLUMNS, export_copy_rows


def test_export_llm_jsonl_to_postgres_copy_keeps_date_column(tmp_path):
    input_path = tmp_path / "events.jsonl"
    event = {
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
        "error_type": None,
        "http_status": 200,
        "estimated_cost_usd": 0.00001,
        "mode": "mock",
        "region": "us",
        "environment": "dev",
        "created_at": "2026-01-01T00:00:00+00:00",
        "date": "2026-01-01",
    }
    input_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    output = StringIO()

    row_count = export_copy_rows(input_path, output)
    parsed_rows = list(csv.reader(StringIO(output.getvalue()), delimiter="\t"))
    row = dict(zip(COPY_COLUMNS, parsed_rows[0]))

    assert row_count == 1
    assert row["request_id"] == "req_001"
    assert row["date"] == "2026-01-01"
    assert row["error_type"] == r"\N"
    assert row["trace_id"] == ""
    assert row["request_type"] == "chat"
    assert row["is_streaming"] == "false"
