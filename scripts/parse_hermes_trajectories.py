import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.agent_event import AgentRunEvent, AgentSpanEvent, AgentToolCallEvent
from app.llm_event import text_sha256


DEFAULT_INPUT_PATH = Path("data/raw/hermes_trajectories/trajectories.jsonl")
DEFAULT_RUN_OUTPUT_PATH = Path("data/raw/hermes_agent_runs/events.jsonl")
DEFAULT_SPAN_OUTPUT_PATH = Path("data/raw/hermes_agent_spans/events.jsonl")
DEFAULT_TOOL_CALL_OUTPUT_PATH = Path("data/raw/hermes_agent_tool_calls/events.jsonl")


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def load_jsonl(input_path: Path) -> list[dict]:
    rows = []
    with input_path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def write_jsonl(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def stable_id(prefix: str, *parts: Any) -> str:
    text = "|".join(json.dumps(part, sort_keys=True, ensure_ascii=False) for part in parts)
    return f"{prefix}_{text_sha256(text)[:32]}"


def extract_tool_call_name(tool_call: dict) -> str:
    function = tool_call.get("function")
    if isinstance(function, dict) and function.get("name"):
        return str(function["name"])
    if tool_call.get("name"):
        return str(tool_call["name"])
    return "unknown_tool"


def extract_tool_call_arguments(tool_call: dict) -> str:
    function = tool_call.get("function")
    if isinstance(function, dict) and "arguments" in function:
        arguments = function["arguments"]
    else:
        arguments = tool_call.get("arguments", {})

    if isinstance(arguments, str):
        return arguments
    return json.dumps(arguments, sort_keys=True, ensure_ascii=False)


def tool_call_status(result_text: str, tool_error_counts: dict, tool_name: str) -> tuple[str, str | None]:
    if int(tool_error_counts.get(tool_name, 0) or 0) > 0:
        return "error", "tool_error"
    lowered = result_text.lower()
    if "error" in lowered or "exception" in lowered or "traceback" in lowered:
        return "error", "tool_error"
    return "success", None


def map_trajectory_to_events(
    trajectory: dict,
    trajectory_index: int,
) -> tuple[dict, list[dict], list[dict]]:
    metadata = trajectory.get("metadata") or {}
    conversations = trajectory.get("conversations") or []
    tool_error_counts = trajectory.get("tool_error_counts") or {}
    timestamp = parse_timestamp(metadata.get("timestamp"))
    model_name = str(metadata.get("model") or "")
    run_id = str(
        metadata.get("session_id")
        or trajectory.get("session_id")
        or stable_id("hermes_run", trajectory.get("prompt_index", trajectory_index), conversations)
    )
    trace_id = str(metadata.get("trace_id") or stable_id("trace", run_id))
    agent_id = str(metadata.get("agent_id") or "hermes_agent")
    agent_name = str(metadata.get("agent_name") or "hermes_agent")
    app_name = str(metadata.get("app_name") or "hermes_agent")
    user_id = str(metadata.get("user_id") or "unknown_user")
    channel = str(metadata.get("source") or metadata.get("channel") or "batch")
    completed = bool(trajectory.get("completed"))
    partial = bool(trajectory.get("partial"))
    status = "success" if completed and not partial else "error"
    error_type = None if status == "success" else "incomplete_trajectory"
    toolsets_used = json.dumps(trajectory.get("toolsets_used") or [], ensure_ascii=False)
    prompt_text = next(
        (str(message.get("value", "")) for message in conversations if message.get("from") == "human"),
        "",
    )
    final_output = next(
        (
            str(message.get("value", ""))
            for message in reversed(conversations)
            if message.get("from") == "gpt"
        ),
        "",
    )

    spans: list[AgentSpanEvent] = []
    tool_calls: list[AgentToolCallEvent] = []
    pending_tool_results: list[str] = []
    for message in conversations:
        if message.get("from") == "tool":
            pending_tool_results.append(str(message.get("value", "")))

    span_order = 0
    llm_call_count = 0
    tool_call_count = 0
    current_time = timestamp

    for message_index, message in enumerate(conversations):
        role = message.get("from")
        if role == "gpt":
            span_order += 1
            llm_call_count += 1
            value = str(message.get("value", ""))
            span_id = stable_id("span", run_id, message_index, "llm_call")
            spans.append(
                AgentSpanEvent(
                    span_id=span_id,
                    parent_span_id=None,
                    run_id=run_id,
                    trace_id=trace_id,
                    agent_id=agent_id,
                    span_name="assistant_message",
                    span_type="llm_call",
                    span_order=span_order,
                    start_time=current_time,
                    end_time=current_time,
                    duration_ms=0,
                    status="success",
                    error_type=None,
                    retry_count=0,
                    input_size=0,
                    output_size=len(value),
                    model_name=model_name or None,
                    tool_name=None,
                    mode="hermes",
                    region=str(metadata.get("region") or "unknown"),
                    environment=str(metadata.get("environment") or "dev"),
                    created_at=current_time,
                )
            )

            for tool_call_index, tool_call in enumerate(message.get("tool_calls") or []):
                span_order += 1
                tool_call_count += 1
                tool_name = extract_tool_call_name(tool_call)
                arguments_json = extract_tool_call_arguments(tool_call)
                result_text = pending_tool_results.pop(0) if pending_tool_results else ""
                tool_status, tool_error_type = tool_call_status(
                    result_text,
                    tool_error_counts,
                    tool_name,
                )
                tool_span_id = stable_id("span", run_id, message_index, tool_call_index, "tool_call")
                tool_call_id = str(
                    tool_call.get("id") or stable_id("tool_call", run_id, message_index, tool_call_index)
                )
                spans.append(
                    AgentSpanEvent(
                        span_id=tool_span_id,
                        parent_span_id=span_id,
                        run_id=run_id,
                        trace_id=trace_id,
                        agent_id=agent_id,
                        span_name=tool_name,
                        span_type="tool_call",
                        span_order=span_order,
                        start_time=current_time,
                        end_time=current_time,
                        duration_ms=0,
                        status=tool_status,
                        error_type=tool_error_type,
                        retry_count=0,
                        input_size=len(arguments_json),
                        output_size=len(result_text),
                        model_name=None,
                        tool_name=tool_name,
                        mode="hermes",
                        region=str(metadata.get("region") or "unknown"),
                        environment=str(metadata.get("environment") or "dev"),
                        created_at=current_time,
                    )
                )
                tool_calls.append(
                    AgentToolCallEvent(
                        tool_call_id=tool_call_id,
                        span_id=tool_span_id,
                        run_id=run_id,
                        trace_id=trace_id,
                        agent_id=agent_id,
                        tool_name=tool_name,
                        tool_type=str(tool_call.get("type") or "function"),
                        arguments_json=arguments_json,
                        result_text=result_text,
                        result_size=len(result_text),
                        duration_ms=0,
                        status=tool_status,
                        error_type=tool_error_type,
                        retry_count=0,
                        mode="hermes",
                        region=str(metadata.get("region") or "unknown"),
                        environment=str(metadata.get("environment") or "dev"),
                        created_at=current_time,
                    )
                )

        current_time = timestamp + timedelta(milliseconds=(span_order + 1) * 100)

    end_time = current_time
    run = AgentRunEvent(
        run_id=run_id,
        trace_id=trace_id,
        agent_id=agent_id,
        agent_name=agent_name,
        agent_version=str(metadata.get("agent_version") or "unknown"),
        app_name=app_name,
        user_id=user_id,
        session_id=str(metadata.get("session_id") or run_id),
        conversation_id=str(metadata.get("conversation_id") or run_id),
        task_type=str(metadata.get("task_type") or "hermes_trajectory"),
        channel=channel,
        toolsets_used=toolsets_used,
        input_text_hash=text_sha256(prompt_text),
        output_text_hash=text_sha256(final_output),
        start_time=timestamp,
        end_time=end_time,
        duration_ms=max(0, int((end_time - timestamp).total_seconds() * 1000)),
        status=status,
        error_type=error_type,
        turn_count=sum(1 for message in conversations if message.get("from") == "human"),
        llm_call_count=int(trajectory.get("api_calls") or llm_call_count),
        tool_call_count=tool_call_count,
        retrieval_count=0,
        total_tokens=int(metadata.get("total_tokens") or trajectory.get("total_tokens") or 0),
        estimated_cost_usd=float(
            metadata.get("estimated_cost_usd") or trajectory.get("estimated_cost_usd") or 0.0
        ),
        mode="hermes",
        region=str(metadata.get("region") or "unknown"),
        environment=str(metadata.get("environment") or "dev"),
        created_at=timestamp,
    )

    return run.to_dict(), [span.to_dict() for span in spans], [call.to_dict() for call in tool_calls]


def parse_trajectories(
    input_path: Path,
    run_output_path: Path,
    span_output_path: Path,
    tool_call_output_path: Path,
) -> tuple[int, int, int]:
    trajectories = load_jsonl(input_path)
    run_rows: list[dict] = []
    span_rows: list[dict] = []
    tool_call_rows: list[dict] = []

    for index, trajectory in enumerate(trajectories):
        run, spans, tool_calls = map_trajectory_to_events(trajectory, index)
        run_rows.append(run)
        span_rows.extend(spans)
        tool_call_rows.extend(tool_calls)

    write_jsonl(run_rows, run_output_path)
    write_jsonl(span_rows, span_output_path)
    write_jsonl(tool_call_rows, tool_call_output_path)

    return len(run_rows), len(span_rows), len(tool_call_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--run-output", type=Path, default=DEFAULT_RUN_OUTPUT_PATH)
    parser.add_argument("--span-output", type=Path, default=DEFAULT_SPAN_OUTPUT_PATH)
    parser.add_argument("--tool-call-output", type=Path, default=DEFAULT_TOOL_CALL_OUTPUT_PATH)
    args = parser.parse_args()

    run_count, span_count, tool_call_count = parse_trajectories(
        input_path=args.input,
        run_output_path=args.run_output,
        span_output_path=args.span_output,
        tool_call_output_path=args.tool_call_output,
    )
    print(f"Parsed Hermes runs: {args.run_output} ({run_count} rows)")
    print(f"Parsed Hermes spans: {args.span_output} ({span_count} rows)")
    print(f"Parsed Hermes tool calls: {args.tool_call_output} ({tool_call_count} rows)")


if __name__ == "__main__":
    main()
