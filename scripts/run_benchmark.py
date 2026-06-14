import argparse
import time
from pathlib import Path

from app.logging_utils import get_logger, log_info
from scripts.generate_mock_llm_logs import write_jsonl
from datetime import datetime
from scripts.spark_paimon_backfill import DEFAULT_START_TIME, run_backfill
from scripts.spark_utils import build_spark_session

LOGGER = get_logger(__name__)
DEFAULT_SCALES = (10_000, 100_000, 1_000_000)
DEFAULT_OUTPUT_PATH = Path("docs/benchmark_results.md")


def run_scale(scale: int) -> dict:
    raw_output = Path(f"data/benchmarks/{scale}/raw/events.jsonl")
    quarantine_output = Path(f"data/benchmarks/{scale}/warehouse/quarantine/events.parquet")

    write_jsonl(
        count=scale,
        output_path=raw_output,
        seed=42,
        start_time=datetime.fromisoformat(DEFAULT_START_TIME),
    )

    spark = build_spark_session(f"benchmark-{scale}")
    try:
        started = time.perf_counter()
        result = run_backfill(
            spark=spark,
            input_path=raw_output,
            quarantine_output=quarantine_output,
            write_to_paimon=False,
        )
        backfill_duration = time.perf_counter() - started
    finally:
        spark.stop()

    parquet_size_mb = round(
        sum(path.stat().st_size for path in quarantine_output.rglob("*") if path.is_file()) / 1_048_576,
        2,
    )
    return {
        "scale": scale,
        "backfill_duration": round(backfill_duration, 2),
        "parquet_size_mb": parquet_size_mb,
        "quarantine_rows": result["quarantine_rows"],
        "dwd_rows": result["dwd_rows"],
        "dws_rows": result["dws_rows"],
        "doris_query_p95_ms": "n/a",
    }


def render_markdown(results: list[dict]) -> str:
    lines = [
        "# Benchmark Results",
        "",
        "| Scale | Backfill Duration (s) | Quarantine Size (MB) | DWD Rows | DWS Rows | Quarantine Rows | Doris Query P95 |",
        "|---|---|---|---|---|---|---|",
    ]
    for result in results:
        lines.append(
            f"| {result['scale']} | {result['backfill_duration']} | {result['parquet_size_mb']} | "
            f"{result['dwd_rows']} | {result['dws_rows']} | {result['quarantine_rows']} | "
            f"{result['doris_query_p95_ms']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--scales", type=int, nargs="*", default=list(DEFAULT_SCALES))
    args = parser.parse_args()

    results = [run_scale(scale) for scale in args.scales]
    args.output.write_text(render_markdown(results), encoding="utf-8")
    log_info(LOGGER, "benchmark_results_written", output=str(args.output), scales=",".join(map(str, args.scales)))


if __name__ == "__main__":
    main()
