import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.evaluation_event import EvaluationEvent
from app.logging_utils import get_logger, log_info
from app.model_pricing import available_model_names

OUTPUT_PATH = Path("data/raw/mock_evaluation_judgments/events.jsonl")

APPS = ["ai_support_bot", "sales_assistant", "internal_copilot"]
FEATURES = ["chat", "summary", "rewrite", "rag_answer"]
EVALUATOR_TYPES = ["llm_judge", "human", "ground_truth", "regex", "classifier"]
EVALUATION_DIMENSIONS = ["relevance", "faithfulness", "coherence", "toxicity", "hallucination"]
MODEL_NAMES = available_model_names()
LOGGER = get_logger(__name__)


def build_mock_event(created_at: datetime | None = None) -> EvaluationEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    evaluator_type = random.choice(EVALUATOR_TYPES)
    score = round(random.uniform(0.0, 1.0), 4)
    pass_threshold = random.choice([0.6, 0.7, 0.8])
    evaluator_model = random.choice(MODEL_NAMES) if evaluator_type == "llm_judge" else ""

    return EvaluationEvent(
        evaluation_id=f"eval_{random.getrandbits(128):032x}",
        trace_id=f"trace_{random.getrandbits(128):032x}",
        request_id=f"req_{random.getrandbits(128):032x}",
        run_id=f"run_{random.getrandbits(128):032x}",
        app_name=random.choice(APPS),
        feature_name=random.choice(FEATURES),
        evaluator_type=evaluator_type,
        evaluator_model=evaluator_model,
        evaluation_dimension=random.choice(EVALUATION_DIMENSIONS),
        score=score,
        raw_score=str(score),
        pass_threshold=pass_threshold,
        passed=score >= pass_threshold,
        evaluated_model_name=random.choice(MODEL_NAMES),
        evaluated_prompt_version=random.choice(["v1", "v2", "v3"]),
        evaluation_latency_ms=random.randint(20, 2500),
        mode=random.choice(["mock", "offline"]),
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
    log_info(LOGGER, "mock_evaluation_events_written", count=args.count, output=str(args.output))


if __name__ == "__main__":
    main()
