import argparse
import time
from pathlib import Path

from app.logging_utils import get_logger, log_info
from scripts.run_local_batch_pipeline import build_ads, build_dwd, build_ods, DEFAULT_START_TIME
from scripts.generate_mock_llm_logs import write_jsonl
from scripts.spark_utils import build_spark_session
from datetime import datetime

LOGGER = get_logger(__name__)
DEFAULT_SCALES = (10_000, 100_000, 1_000_000)
DEFAULT_OUTPUT_PATH = Path("docs/benchmark_results.md")


def run_scale(scale: int) -> dict:
    raw_output = Path(f"data/benchmarks/{scale}/raw/events.jsonl")
    ods_output = Path(f"data/benchmarks/{scale}/warehouse/ods/events.parquet")
    dwd_output = Path(f"data/benchmarks/{scale}/warehouse/dwd/events.parquet")
    ads_output = Path(f"data/benchmarks/{scale}/warehouse/ads/events.parquet")

    write_jsonl(
        count=scale,
        output_path=raw_output,
        seed=42,
        start_time=datetime.fromisoformat(DEFAULT_START_TIME),
    )

    spark = build_spark_session(f"benchmark-{scale}")
    try:
        started = time.perf_counter()
        build_ods(spark, raw_output, ods_output)
        ods_duration = time.perf_counter() - started

        started = time.perf_counter()
        build_dwd(spark, ods_output, dwd_output)
        dwd_duration = time.perf_counter() - started

        started = time.perf_counter()
        build_ads(spark, dwd_output, ads_output)
        ads_duration = time.perf_counter() - started
    finally:
        spark.stop()

    parquet_size_mb = round(sum(path.stat().st_size for path in ads_output.rglob("*") if path.is_file()) / 1_048_576, 2)
    return {
        "scale": scale,
        "ods_duration": round(ods_duration, 2),
        "dwd_duration": round(dwd_duration, 2),
        "ads_duration": round(ads_duration, 2),
        "parquet_size_mb": parquet_size_mb,
        "clickhouse_query_p95_ms": "n/a",
    }


def render_markdown(results: list[dict]) -> str:
    lines = [
        "# Benchmark Results",
        "",
        "| Scale | ODS Duration (s) | DWD Duration (s) | ADS Duration (s) | Parquet Size (MB) | CH Query P95 |",
        "|---|---|---|---|---|---|",
    ]
    for result in results:
        lines.append(
            f"| {result['scale']} | {result['ods_duration']} | {result['dwd_duration']} | "
            f"{result['ads_duration']} | {result['parquet_size_mb']} | {result['clickhouse_query_p95_ms']} |"
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
