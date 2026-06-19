import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from app.logging_utils import get_logger, log_info
from app.warehouse_contract import (
    build_compliance_access_audit_projection,
    build_compliance_data_retention_projection,
)
from scripts.spark_utils import build_spark_session


DEFAULT_ACCESS_INPUT_PATH = Path(
    "data/warehouse/ods/ods_ai_observability_compliance_access_audit_events_di/events.parquet"
)
DEFAULT_ACCESS_OUTPUT_PATH = Path(
    "data/warehouse/dwd/dwd_ai_compliance_access_audit_di/events.parquet"
)
DEFAULT_RETENTION_INPUT_PATH = Path(
    "data/warehouse/ods/ods_ai_observability_compliance_data_retention_events_di/events.parquet"
)
DEFAULT_RETENTION_OUTPUT_PATH = Path(
    "data/warehouse/dwd/dwd_ai_compliance_data_retention_di/events.parquet"
)
LOGGER = get_logger(__name__)


def load_ods_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def transform_access_audit_events(raw_events: DataFrame) -> DataFrame:
    return build_compliance_access_audit_projection(raw_events)


def transform_data_retention_events(raw_events: DataFrame) -> DataFrame:
    return build_compliance_data_retention_projection(raw_events)


def write_parquet(events: DataFrame, output_path: Path) -> None:
    events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--access-input", type=Path, default=DEFAULT_ACCESS_INPUT_PATH)
    parser.add_argument("--access-output", type=Path, default=DEFAULT_ACCESS_OUTPUT_PATH)
    parser.add_argument("--retention-input", type=Path, default=DEFAULT_RETENTION_INPUT_PATH)
    parser.add_argument("--retention-output", type=Path, default=DEFAULT_RETENTION_OUTPUT_PATH)
    args = parser.parse_args()
    spark = build_spark_session("ai-observability-compliance-events-batch")

    try:
        access_events = transform_access_audit_events(load_ods_events(spark, args.access_input))
        retention_events = transform_data_retention_events(load_ods_events(spark, args.retention_input))
        write_parquet(access_events, args.access_output)
        write_parquet(retention_events, args.retention_output)
        log_info(
            LOGGER,
            "dwd_compliance_events_written",
            access_rows=access_events.count(),
            retention_rows=retention_events.count(),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
