import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.orchestration_event import AgentOrchestrationEvent


OUTPUT_PATH = Path("data/raw/mock_agent_orchestration/events.jsonl")
AGENT_IDS = ["planner", "researcher", "tool_executor", "reviewer", "response_writer"]
HANDOFF_TYPES = ["delegate", "callback", "broadcast", "sequential"]


def build_mock_event(created_at: datetime) -> AgentOrchestrationEvent:
    parent_agent_id, child_agent_id = random.sample(AGENT_IDS, 2)
    return AgentOrchestrationEvent(
        orchestration_id=f"orchestration_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        parent_run_id=f"run_{random.getrandbits(128):032x}",
        child_run_id=f"run_{random.getrandbits(128):032x}",
        parent_agent_id=parent_agent_id,
        child_agent_id=child_agent_id,
        handoff_type=random.choice(HANDOFF_TYPES),
        payload_size=random.randint(32, 65536),
        handoff_latency_ms=random.randint(1, 5000),
        status=random.choices(["success", "error", "timeout"], weights=[85, 10, 5], k=1)[0],
        created_at=created_at,
    )


def write_jsonl(
    count: int,
    output_path: Path = OUTPUT_PATH,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> None:
    if seed is not None:
        random.seed(seed)
    if start_time is None:
        start_time = datetime.now(timezone.utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for index in range(count):
            event = build_mock_event(start_time + timedelta(seconds=index))
            file.write(json.dumps(event.to_dict()) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    write_jsonl(args.count, args.output, args.seed)


if __name__ == "__main__":
    main()
