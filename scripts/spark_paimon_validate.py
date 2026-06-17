import argparse

from app.data_quality import validate_llm_events
from app.logging_utils import get_logger, log_info
from scripts.spark_utils import build_paimon_spark_session


LOGGER = get_logger(__name__)
DEFAULT_INPUT_TABLE = "paimon_lake.dwd.dwd_ai_llm_request_di"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-table", type=str, default=DEFAULT_INPUT_TABLE)
    parser.add_argument("--warehouse", type=str)
    args = parser.parse_args()

    spark = build_paimon_spark_session("ai-observability-spark-paimon-validate", warehouse=args.warehouse)
    try:
        events = spark.table(args.input_table)
        validated = validate_llm_events(events)
        stats = (
            validated.groupBy("_dq_status")
            .count()
            .orderBy("_dq_status")
            .collect()
        )
        log_info(
            LOGGER,
            "spark_paimon_validation_completed",
            input_table=args.input_table,
            total_rows=events.count(),
            stats=[row.asDict() for row in stats],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
