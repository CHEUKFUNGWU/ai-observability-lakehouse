import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.agent_event import AgentRunEvent, AgentSpanEvent
from app.logging_utils import get_logger, log_info
from app.llm_event import text_sha256
from app.model_pricing import DEFAULT_MODEL_NAME, estimate_model_cost_usd


DEFAULT_RUN_OUTPUT_PATH = Path("data/raw/mock_agent_runs/events.jsonl")
DEFAULT_SPAN_OUTPUT_PATH = Path("data/raw/mock_agent_spans/events.jsonl")
DEFAULT_START_TIME = "2026-01-01T00:00:00+00:00"
LOGGER = get_logger(__name__)

AGENTS = [
    ("agent_support", "customer_support_agent", "customer_support"),
    ("agent_sales", "sales_assistant_agent", "product_qa"),
    ("agent_ops", "ops_copilot_agent", "ticket_triage"),
]
CHANNELS = ["web", "slack", "api"]
REGIONS = ["us", "eu", "apac"]
SPAN_TYPES = ["planning", "retrieval", "llm_call", "tool_call", "final_response"]


def build_agent_events(
    run_index: int,
    created_at: datetime,
) -> tuple[AgentRunEvent, list[AgentSpanEvent]]:
    agent_id, agent_name, task_type = random.choice(AGENTS)
    toolsets_used = random.sample(["terminal", "file", "web_search", "database"], random.randint(1, 3))
    trace_id = f"trace_{random.getrandbits(128):032x}"
    run_id = f"run_{random.getrandbits(128):032x}"
    user_id = f"user_{random.randint(1, 500):04d}"
    session_id = f"session_{random.randint(1, 2000):05d}"
    conversation_id = f"conv_{random.randint(1, 3000):05d}"
    channel = random.choice(CHANNELS)
    region = random.choice(REGIONS)
    is_success = random.random() > 0.1
    status = "success" if is_success else "error"
    error_type = None if is_success else random.choice(["tool_error", "timeout", "guardrail_block"])
    span_count = random.randint(3, 6)

    spans = []
    total_tokens = 0
    estimated_cost_usd = 0.0
    llm_call_count = 0
    tool_call_count = 0
    retrieval_count = 0
    current_time = created_at

    for span_order in range(1, span_count + 1):
        span_type = SPAN_TYPES[min(span_order - 1, len(SPAN_TYPES) - 1)]
        duration_ms = random.randint(80, 2500)
        span_status = status if span_order == span_count else "success"
        span_error_type = error_type if span_status == "error" else None
        model_name = DEFAULT_MODEL_NAME if span_type == "llm_call" else None
        tool_name = random.choice(["order_lookup", "ticket_create", "crm_search"]) if span_type == "tool_call" else None

        if span_type == "llm_call":
            llm_call_count += 1
            span_tokens = random.randint(200, 2000)
            total_tokens += span_tokens
            estimated_cost_usd += estimate_model_cost_usd(model_name or DEFAULT_MODEL_NAME, 0, span_tokens)
        elif span_type == "tool_call":
            tool_call_count += 1
        elif span_type == "retrieval":
            retrieval_count += 1

        end_time = current_time + timedelta(milliseconds=duration_ms)
        spans.append(
            AgentSpanEvent(
                span_id=f"span_{random.getrandbits(128):032x}",
                parent_span_id=None,
                run_id=run_id,
                trace_id=trace_id,
                agent_id=agent_id,
                span_name=span_type,
                span_type=span_type,
                span_order=span_order,
                start_time=current_time,
                end_time=end_time,
                duration_ms=duration_ms,
                status=span_status,
                error_type=span_error_type,
                retry_count=random.randint(0, 2) if span_status == "error" else 0,
                input_size=random.randint(20, 2000),
                output_size=random.randint(0, 3000),
                model_name=model_name,
                tool_name=tool_name,
                mode="mock",
                region=region,
                environment="dev",
                created_at=current_time,
            )
        )
        current_time = end_time

    run = AgentRunEvent(
        run_id=run_id,
        trace_id=trace_id,
        agent_id=agent_id,
        agent_name=agent_name,
        agent_version=random.choice(["v1", "v2"]),
        app_name="ai_agent_platform",
        user_id=user_id,
        session_id=session_id,
        conversation_id=conversation_id,
        task_type=task_type,
        channel=channel,
        toolsets_used=json.dumps(toolsets_used),
        input_text_hash=text_sha256(f"mock agent input {run_index}"),
        output_text_hash=text_sha256(f"mock agent output {run_index}" if is_success else ""),
        start_time=created_at,
        end_time=current_time,
        duration_ms=int((current_time - created_at).total_seconds() * 1000),
        status=status,
        error_type=error_type,
        turn_count=random.randint(1, 4),
        llm_call_count=llm_call_count,
        tool_call_count=tool_call_count,
        retrieval_count=retrieval_count,
        total_tokens=total_tokens,
        estimated_cost_usd=round(estimated_cost_usd, 8),
        mode="mock",
        region=region,
        environment="dev",
        created_at=created_at,
    )

    return run, spans


def write_jsonl(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")


def generate_agent_logs(
    count: int,
    run_output_path: Path,
    span_output_path: Path,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> tuple[int, int]:
    if seed is not None:
        random.seed(seed)
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    run_rows = []
    span_rows = []
    for index in range(count):
        run, spans = build_agent_events(index, start_time + timedelta(seconds=index))
        run_rows.append(run.to_dict())
        span_rows.extend(span.to_dict() for span in spans)

    write_jsonl(run_rows, run_output_path)
    write_jsonl(span_rows, span_output_path)
    return len(run_rows), len(span_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--run-output", type=Path, default=DEFAULT_RUN_OUTPUT_PATH)
    parser.add_argument("--span-output", type=Path, default=DEFAULT_SPAN_OUTPUT_PATH)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--start-time", type=str, default=DEFAULT_START_TIME)
    args = parser.parse_args()

    run_count, span_count = generate_agent_logs(
        count=args.count,
        run_output_path=args.run_output,
        span_output_path=args.span_output,
        seed=args.seed,
        start_time=datetime.fromisoformat(args.start_time),
    )
    log_info(LOGGER, "mock_agent_runs_written", output=str(args.run_output), rows=run_count)
    log_info(LOGGER, "mock_agent_spans_written", output=str(args.span_output), rows=span_count)


if __name__ == "__main__":
    main()
