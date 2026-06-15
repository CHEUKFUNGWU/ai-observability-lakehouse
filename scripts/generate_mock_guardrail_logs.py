import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.guardrail_event import GuardrailEvent
from app.llm_event import text_sha256
from app.logging_utils import get_logger, log_info
from app.model_pricing import available_model_names

OUTPUT_PATH = Path("data/raw/mock_guardrail_checks/events.jsonl")

APPS = ["ai_support_bot", "sales_assistant", "internal_copilot"]
FEATURES = ["chat", "summary", "rewrite", "rag_answer"]
RULES = [
    ("pii_email", "pii_detection"),
    ("toxic_language", "toxicity"),
    ("restricted_topic", "topic_block"),
    ("prompt_length", "length_limit"),
    ("unsafe_content", "content_filter"),
]
SEVERITIES = ["low", "medium", "high", "critical"]
MODEL_NAMES = available_model_names()
LOGGER = get_logger(__name__)


def build_mock_event(created_at: datetime | None = None) -> GuardrailEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    rule_name, rule_category = random.choice(RULES)
    triggered = random.random() < 0.18
    action_taken = "pass"
    if triggered:
        action_taken = random.choice(["warn", "block", "redact", "override"])
    matched_text = f"mock matched pattern {random.randint(1, 1000)}" if triggered else ""

    return GuardrailEvent(
        guardrail_event_id=f"gr_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        request_id=f"req_{random.getrandbits(128):032x}",
        run_id=f"run_{random.getrandbits(128):032x}",
        user_id=f"user_{random.randint(1, 500):04d}",
        app_name=random.choice(APPS),
        feature_name=random.choice(FEATURES),
        guardrail_stage=random.choice(["pre_request", "post_response"]),
        rule_name=rule_name,
        rule_category=rule_category,
        triggered=triggered,
        action_taken=action_taken,
        severity=random.choice(SEVERITIES),
        matched_pattern_hash=text_sha256(matched_text) if matched_text else "",
        input_text_length=random.randint(20, 4000),
        guardrail_latency_ms=random.randint(5, 300),
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
    log_info(LOGGER, "mock_guardrail_events_written", count=args.count, output=str(args.output))


if __name__ == "__main__":
    main()
