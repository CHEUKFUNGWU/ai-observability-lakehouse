import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import build_agent_run_projection, build_agent_span_projection
from scripts.spark_utils import build_spark_session


DEFAULT_RUN_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_agent_run_events_di/events.parquet")
DEFAULT_SPAN_INPUT_PATH = Path("data/warehouse/ods/ods_ai_observability_agent_span_events_di/events.parquet")
DEFAULT_RUN_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_run_di/events.parquet")
DEFAULT_SPAN_OUTPUT_PATH = Path("data/warehouse/dwd/dwd_ai_agent_span_di/events.parquet")
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_agent_run_events(raw_runs: DataFrame) -> DataFrame:
    return build_agent_run_projection(raw_runs)


def transform_agent_span_events(raw_spans: DataFrame) -> DataFrame:
    return build_agent_span_projection(raw_spans)


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-input", type=Path, default=DEFAULT_RUN_INPUT_PATH)
    parser.add_argument("--span-input", type=Path, default=DEFAULT_SPAN_INPUT_PATH)
    parser.add_argument("--run-output", type=Path, default=DEFAULT_RUN_OUTPUT_PATH)
    parser.add_argument("--span-output", type=Path, default=DEFAULT_SPAN_OUTPUT_PATH)
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-agent-events-batch")
    try:
        runs = transform_agent_run_events(load_ods_events(spark, args.run_input))
        spans = transform_agent_span_events(load_ods_events(spark, args.span_input))
        write_parquet(runs, args.run_output)
        write_parquet(spans, args.span_output)
        log_info(LOGGER, "dwd_agent_runs_written", output=str(args.run_output), rows=runs.count())
        log_info(LOGGER, "dwd_agent_spans_written", output=str(args.span_output), rows=spans.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
