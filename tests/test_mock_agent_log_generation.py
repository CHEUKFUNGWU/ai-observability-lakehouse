import json
from datetime import datetime

from scripts.generate_mock_agent_logs import generate_agent_logs


def test_generate_agent_logs_writes_runs_and_spans(tmp_path):
    run_output = tmp_path / "agent_runs.jsonl"
    span_output = tmp_path / "agent_spans.jsonl"

    run_count, span_count = generate_agent_logs(
        count=3,
        run_output_path=run_output,
        span_output_path=span_output,
        seed=42,
        start_time=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )

    run_rows = [json.loads(line) for line in run_output.read_text(encoding="utf-8").splitlines()]
    span_rows = [json.loads(line) for line in span_output.read_text(encoding="utf-8").splitlines()]

    assert run_count == 3
    assert len(run_rows) == 3
    assert span_count == len(span_rows)
    assert span_count >= 9
    assert {"run_id", "trace_id", "agent_id", "task_type", "total_tokens", "date"}.issubset(run_rows[0])
    assert {"span_id", "run_id", "trace_id", "span_type", "duration_ms", "date"}.issubset(span_rows[0])
