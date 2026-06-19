import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from app.platform_health_metric import PlatformHealthMetric


OUTPUT_PATH = Path("data/raw/mock_platform_health/events.jsonl")
THRESHOLD_PATH = Path("config/platform_health_thresholds.yaml")


def load_thresholds(path: Path = THRESHOLD_PATH) -> dict[str, dict[str, float]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload["thresholds"]


def build_mock_metric(
    component: str,
    metric_name: str,
    threshold: float,
    created_at: datetime,
) -> PlatformHealthMetric:
    multiplier = random.uniform(0.1, 1.25)
    return PlatformHealthMetric(
        metric_event_id=f"health_{random.getrandbits(128):032x}",
        component=component,
        metric_name=metric_name,
        metric_value=round(threshold * multiplier, 4),
        threshold=float(threshold),
        created_at=created_at,
    )


def write_jsonl(
    sample_count: int,
    output_path: Path = OUTPUT_PATH,
    threshold_path: Path = THRESHOLD_PATH,
    seed: int | None = None,
    start_time: datetime | None = None,
) -> None:
    if seed is not None:
        random.seed(seed)
    if start_time is None:
        start_time = datetime.now(timezone.utc)
    thresholds = load_thresholds(threshold_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for sample_index in range(sample_count):
            created_at = start_time + timedelta(minutes=sample_index * 5)
            for component, metrics in thresholds.items():
                for metric_name, threshold in metrics.items():
                    event = build_mock_metric(component, metric_name, float(threshold), created_at)
                    file.write(json.dumps(event.to_dict()) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-count", type=int, default=12)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--thresholds", type=Path, default=THRESHOLD_PATH)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    write_jsonl(args.sample_count, args.output, args.thresholds, args.seed)


if __name__ == "__main__":
    main()
