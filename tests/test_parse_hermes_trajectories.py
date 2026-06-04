import json

from scripts.parse_hermes_trajectories import parse_trajectories


def test_parse_hermes_trajectories_outputs_runs_spans_and_tool_calls(tmp_path):
    input_path = tmp_path / "trajectories.jsonl"
    run_output = tmp_path / "runs.jsonl"
    span_output = tmp_path / "spans.jsonl"
    tool_call_output = tmp_path / "tool_calls.jsonl"
    trajectory = {
        "prompt_index": 42,
        "conversations": [
            {"from": "human", "value": "Find my order status"},
            {
                "from": "gpt",
                "value": "I will check the order database.",
                "tool_calls": [
                    {
                        "id": "call_001",
                        "type": "function",
                        "function": {
                            "name": "order_lookup",
                            "arguments": "{\"order_id\":\"A123\"}",
                        },
                    }
                ],
            },
            {"from": "tool", "value": "{\"status\":\"shipped\"}"},
            {"from": "gpt", "value": "Your order has shipped."},
        ],
        "metadata": {
            "timestamp": "2026-01-15T10:30:00+00:00",
            "model": "anthropic/claude-sonnet-4.6",
            "session_id": "sess_001",
            "user_id": "user_001",
        },
        "completed": True,
        "partial": False,
        "api_calls": 2,
        "toolsets_used": ["database"],
        "tool_stats": {"order_lookup": {"count": 1, "success": 1, "failure": 0}},
        "tool_error_counts": {"order_lookup": 0},
    }
    input_path.write_text(json.dumps(trajectory) + "\n", encoding="utf-8")

    run_count, span_count, tool_call_count = parse_trajectories(
        input_path=input_path,
        run_output_path=run_output,
        span_output_path=span_output,
        tool_call_output_path=tool_call_output,
    )

    run = json.loads(run_output.read_text(encoding="utf-8").splitlines()[0])
    spans = [json.loads(line) for line in span_output.read_text(encoding="utf-8").splitlines()]
    tool_call = json.loads(tool_call_output.read_text(encoding="utf-8").splitlines()[0])

    assert run_count == 1
    assert span_count == 3
    assert tool_call_count == 1
    assert run["run_id"] == "sess_001"
    assert run["toolsets_used"] == "[\"database\"]"
    assert run["llm_call_count"] == 2
    assert run["tool_call_count"] == 1
    assert {span["span_type"] for span in spans} == {"llm_call", "tool_call"}
    assert tool_call["tool_call_id"] == "call_001"
    assert tool_call["tool_name"] == "order_lookup"
    assert tool_call["arguments_json"] == "{\"order_id\":\"A123\"}"
    assert tool_call["result_text"] == "{\"status\":\"shipped\"}"
