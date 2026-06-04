import argparse
import os
from pathlib import Path

os.environ.pop("SPARK_HOME", None)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT_PATH = Path("data/raw/hermes_agent_tool_calls/events.jsonl")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ods/agent_tool_call/events.parquet")


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ai-observability-ods-agent-tool-calls")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_source_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.json(str(input_path))


def build_ods_agent_tool_call_events(raw_events: DataFrame, source_name: str) -> DataFrame:
    return (
        raw_events.withColumn("source_name", F.lit(source_name))
        .withColumn("source_event_type", F.lit("agent_tool_call"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("raw_event_json", F.to_json(F.struct(*[F.col(c) for c in raw_events.columns])))
    )


def write_ods_events(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--source-name", type=str, default="hermes_trajectory")
    args = parser.parse_args()

    spark = build_spark_session()
    try:
        raw_events = load_source_events(spark, args.input)
        ods_events = build_ods_agent_tool_call_events(raw_events, args.source_name)
        write_ods_events(ods_events, args.output)
        print(f"Built ODS agent tool calls: {args.output} ({ods_events.count()} rows)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
