import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


COPY_COLUMNS = [
    "request_id",
    "trace_id",
    "run_id",
    "span_id",
    "agent_id",
    "agent_name",
    "channel",
    "user_id",
    "session_id",
    "conversation_id",
    "app_name",
    "feature_name",
    "prompt_category",
    "prompt_id",
    "prompt_version",
    "model_name",
    "provider",
    "prompt_text",
    "response_text",
    "prompt_hash",
    "response_hash",
    "input_chars",
    "output_chars",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "request_type",
    "is_streaming",
    "temperature",
    "max_tokens",
    "finish_reason",
    "retry_count",
    "latency_ms",
    "status",
    "error_type",
    "http_status",
    "estimated_cost_usd",
    "mode",
    "region",
    "environment",
    "created_at",
    "date",
]


DEFAULTS = {
    "trace_id": "",
    "run_id": "",
    "span_id": "",
    "agent_id": "",
    "agent_name": "",
    "channel": "",
    "conversation_id": "",
    "prompt_hash": "",
    "response_hash": "",
    "input_chars": 0,
    "output_chars": 0,
    "request_type": "chat",
    "is_streaming": False,
    "temperature": 0.0,
    "max_tokens": 0,
    "finish_reason": "",
    "retry_count": 0,
}


def normalize_value(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def row_from_event(event: dict) -> list[str]:
    row = []
    for column in COPY_COLUMNS:
        value = event.get(column, DEFAULTS.get(column))
        row.append(normalize_value(value))
    return row


def export_copy_rows(input_path: Path, output) -> int:
    writer = csv.writer(
        output,
        delimiter="\t",
        lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL,
    )
    row_count = 0
    with input_path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue
            writer.writerow(row_from_event(json.loads(stripped)))
            row_count += 1
    return row_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()

    export_copy_rows(args.input, sys.stdout)


if __name__ == "__main__":
    main()
