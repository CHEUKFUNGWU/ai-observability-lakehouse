import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.feedback_event import FeedbackEvent
from app.llm_event import text_sha256
from app.logging_utils import get_logger, log_info
from app.model_pricing import available_model_names

OUTPUT_PATH = Path("data/raw/mock_feedback_actions/events.jsonl")

APPS = ["ai_support_bot", "sales_assistant", "internal_copilot"]
FEATURES = ["chat", "summary", "rewrite", "rag_answer"]
FEEDBACK_TYPES = ["thumbs_up", "thumbs_down", "rating", "regenerate", "edit", "report"]
MODEL_NAMES = available_model_names()
LOGGER = get_logger(__name__)


def build_mock_event(created_at: datetime | None = None) -> FeedbackEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    feedback_type = random.choice(FEEDBACK_TYPES)
    rating_value = random.randint(1, 5) if feedback_type == "rating" else None
    feedback_text = "" if random.random() > 0.25 else "mock feedback text"

    return FeedbackEvent(
        feedback_id=f"fb_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        request_id=f"req_{random.getrandbits(128):032x}",
        run_id=f"run_{random.getrandbits(128):032x}",
        session_id=f"session_{random.randint(1, 2000):05d}",
        conversation_id=f"conv_{random.randint(1, 3000):05d}",
        user_id=f"user_{random.randint(1, 500):04d}",
        app_name=random.choice(APPS),
        feature_name=random.choice(FEATURES),
        agent_id=f"agent_{random.randint(1, 20):03d}",
        feedback_type=feedback_type,
        rating_value=rating_value,
        feedback_text_hash=text_sha256(feedback_text) if feedback_text else "",
        feedback_text_length=len(feedback_text),
        response_latency_ms=random.randint(200, 5000),
        model_name=random.choice(MODEL_NAMES),
        prompt_version=random.choice(["v1", "v2", "v3"]),
        mode="mock",
        environment="dev",
        created_at=created_at,
    )


def write_jsonl(
    count: int,
    output_path: Path,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> None:
    if seed is not None:
        random.seed(seed)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if start_time is None:
        start_time = datetime.now(timezone.utc)

    with output_path.open("w", encoding="utf-8") as file:
        for index in range(count):
            event = build_mock_event(created_at=start_time + timedelta(seconds=index))
            file.write(json.dumps(event.to_dict()) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--start-time", type=str, default=None)
    args = parser.parse_args()
    start_time = datetime.fromisoformat(args.start_time) if args.start_time is not None else None

    write_jsonl(args.count, args.output, seed=args.seed, start_time=start_time)
    log_info(LOGGER, "mock_feedback_events_written", count=args.count, output=str(args.output))


if __name__ == "__main__":
    main()
