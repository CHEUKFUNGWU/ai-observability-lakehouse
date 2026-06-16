import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.logging_utils import get_logger, log_info
from app.model_deployment_event import ModelDeploymentEvent
from app.model_pricing import available_model_names

OUTPUT_PATH = Path("data/raw/mock_model_deployments/events.jsonl")

MODEL_NAMES = available_model_names()
DEPLOYMENT_ACTIONS = ["deploy", "rollback", "scale", "canary_start", "canary_promote", "canary_abort"]
ENVIRONMENTS = ["dev", "staging", "prod"]
STATUSES = ["success", "failed", "in_progress"]
LOGGER = get_logger(__name__)


def build_mock_event(created_at: datetime | None = None) -> ModelDeploymentEvent:
    if created_at is None:
        created_at = datetime.now(timezone.utc)

    model_name = random.choice(MODEL_NAMES)
    deployment_action = random.choice(DEPLOYMENT_ACTIONS)
    traffic_percentage = 100.0
    if deployment_action.startswith("canary"):
        traffic_percentage = random.choice([5.0, 10.0, 25.0, 50.0])
    elif deployment_action == "rollback":
        traffic_percentage = 0.0

    return ModelDeploymentEvent(
        deployment_id=f"deploy_{random.getrandbits(128):032x}",
        model_name=model_name,
        model_version=f"{model_name}-{random.choice(['20260424', '20260515', '20260610'])}",
        provider="deepseek",
        deployment_action=deployment_action,
        traffic_percentage=traffic_percentage,
        target_environment=random.choice(ENVIRONMENTS),
        deployer_user_id=f"user_{random.randint(1, 6):04d}",
        deploy_reason=random.choice(["capacity_update", "quality_improvement", "latency_regression", "cost_optimization"]),
        status=random.choice(STATUSES),
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
            event = build_mock_event(created_at=start_time + timedelta(minutes=index))
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
    log_info(LOGGER, "mock_model_deployment_events_written", count=args.count, output=str(args.output))


if __name__ == "__main__":
    main()
