import argparse
import json
from pathlib import Path

from app.langfuse_generation import normalize_generation_records
from app.logging_utils import get_logger, log_info


DEFAULT_INPUT_PATH = Path("data/raw/langfuse_generations/generations.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/raw/langfuse_llm_requests/events.jsonl")
DEFAULT_QUARANTINE_OUTPUT_PATH = Path("data/warehouse/quarantine/dwd_ai_llm_request_di/langfuse_generations.jsonl")
LOGGER = get_logger(__name__)


def load_jsonl(input_path: Path) -> list[dict]:
    with input_path.open("r", encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def write_jsonl(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def normalize_jsonl(input_path: Path, output_path: Path, quarantine_output_path: Path) -> dict[str, int]:
    records = load_jsonl(input_path)
    events, quarantine = normalize_generation_records(records)
    write_jsonl(events, output_path)
    write_jsonl(quarantine, quarantine_output_path)
    return {
        "input_rows": len(records),
        "output_rows": len(events),
        "quarantine_rows": len(quarantine),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--quarantine-output", type=Path, default=DEFAULT_QUARANTINE_OUTPUT_PATH)
    args = parser.parse_args()

    result = normalize_jsonl(args.input, args.output, args.quarantine_output)
    log_info(LOGGER, "langfuse_generations_normalized", **result)


if __name__ == "__main__":
    main()
