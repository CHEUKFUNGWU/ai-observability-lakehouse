import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_PIPELINE_METADATA_PATH = Path("data/warehouse/pipeline_runs.jsonl")


def append_pipeline_run(
    pipeline_name: str,
    layer: str,
    start_time: datetime,
    end_time: datetime,
    input_rows: int,
    output_rows: int,
    quarantine_rows: int = 0,
    status: str = "success",
    output_path: Path = DEFAULT_PIPELINE_METADATA_PATH,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pipeline_name": pipeline_name,
        "layer": layer,
        "start_time": start_time.astimezone(timezone.utc).isoformat(),
        "end_time": end_time.astimezone(timezone.utc).isoformat(),
        "duration_ms": int((end_time - start_time).total_seconds() * 1000),
        "input_rows": input_rows,
        "output_rows": output_rows,
        "quarantine_rows": quarantine_rows,
        "status": status,
    }
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")
