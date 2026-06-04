import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_spark_session


DEFAULT_RUN_INPUT_PATH = Path("data/raw/mock_agent_runs/events.jsonl")
DEFAULT_SPAN_INPUT_PATH = Path("data/raw/mock_agent_spans/events.jsonl")
DEFAULT_RUN_OUTPUT_PATH = Path("data/warehouse/ods/agent_run/events.parquet")
DEFAULT_SPAN_OUTPUT_PATH = Path("data/warehouse/ods/agent_span/events.parquet")
LOGGER = get_logger(__name__)


def load_source_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.json(str(input_path))


def build_ods_agent_events(
    raw_events: DataFrame,
    source_name: str,
    source_event_type: str,
) -> DataFrame:
    return (
        raw_events.withColumn("source_name", F.lit(source_name))
        .withColumn("source_event_type", F.lit(source_event_type))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("raw_event_json", F.to_json(F.struct(*[F.col(c) for c in raw_events.columns])))
    )


def write_ods_events(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-input", type=Path, default=DEFAULT_RUN_INPUT_PATH)
    parser.add_argument("--span-input", type=Path, default=DEFAULT_SPAN_INPUT_PATH)
    parser.add_argument("--run-output", type=Path, default=DEFAULT_RUN_OUTPUT_PATH)
    parser.add_argument("--span-output", type=Path, default=DEFAULT_SPAN_OUTPUT_PATH)
    parser.add_argument("--source-name", type=str, default="mock_agent_generator")
    args = parser.parse_args()

    spark = build_spark_session("ai-observability-ods-agent-events")
    try:
        raw_runs = load_source_events(spark, args.run_input)
        raw_spans = load_source_events(spark, args.span_input)

        ods_runs = build_ods_agent_events(raw_runs, args.source_name, "agent_run")
        ods_spans = build_ods_agent_events(raw_spans, args.source_name, "agent_span")

        write_ods_events(ods_runs, args.run_output)
        write_ods_events(ods_spans, args.span_output)

        log_info(LOGGER, "ods_agent_runs_written", output=str(args.run_output), rows=ods_runs.count())
        log_info(LOGGER, "ods_agent_spans_written", output=str(args.span_output), rows=ods_spans.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
