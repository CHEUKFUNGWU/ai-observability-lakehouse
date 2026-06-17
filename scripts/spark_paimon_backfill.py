import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.data_quality import split_valid_quarantine, validate_llm_events
from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.generate_mock_llm_logs import write_jsonl
from scripts.spark_build_dws_llm_feature_daily_metrics import build_feature_daily_metrics
from scripts.spark_transform_llm_events import transform_llm_events
from scripts.spark_utils import build_paimon_spark_session


DEFAULT_START_TIME = "2026-01-01T00:00:00+00:00"
DEFAULT_INPUT_PATH = Path("data/raw/mock_llm_requests/events.jsonl")
DEFAULT_QUARANTINE_PATH = Path("data/warehouse/quarantine/dwd_ai_llm_request_di/events.parquet")
LOGGER = get_logger(__name__)

DWD_TABLE = "paimon_lake.dwd.dwd_ai_llm_request_di"
DWS_TABLE = "paimon_lake.dws.dws_ai_llm_feature_request_1d"
DWD_PRIMARY_KEY = "request_id"
DWS_PRIMARY_KEY = "date,app_name,feature_name,model_name"
DWD_DYNAMIC_BUCKETS = "-1"
PAIMON_BUCKETS = "4"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--quarantine-output", type=Path, default=DEFAULT_QUARANTINE_PATH)
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument("--count", type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=str, default=DEFAULT_START_TIME)
    parser.add_argument("--warehouse", type=str)
    return parser.parse_args()


def load_raw_events(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.json(str(input_path))


def build_valid_dwd_events(raw_events: DataFrame, start_date: str | None = None, end_date: str | None = None) -> tuple[DataFrame, DataFrame]:
    transformed = transform_llm_events(raw_events)
    if start_date:
        transformed = transformed.filter(F.col("date") >= F.lit(start_date))
    if end_date:
        transformed = transformed.filter(F.col("date") <= F.lit(end_date))
    validated = validate_llm_events(transformed)
    return split_valid_quarantine(validated)


def ensure_paimon_tables(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon_lake.dwd")
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon_lake.dws")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon_lake.dwd.dwd_ai_llm_request_di (
            request_id STRING,
            trace_id STRING,
            run_id STRING,
            span_id STRING,
            agent_id STRING,
            agent_name STRING,
            channel STRING,
            user_id STRING,
            session_id STRING,
            conversation_id STRING,
            app_name STRING,
            feature_name STRING,
            prompt_category STRING,
            prompt_id STRING,
            prompt_version STRING,
            model_name STRING,
            provider STRING,
            prompt_hash STRING,
            response_hash STRING,
            input_chars INT,
            output_chars INT,
            prompt_tokens INT,
            completion_tokens INT,
            total_tokens INT,
            request_type STRING,
            is_streaming BOOLEAN,
            temperature DOUBLE,
            max_tokens INT,
            finish_reason STRING,
            retry_count INT,
            latency_ms INT,
            status STRING,
            error_type STRING,
            http_status INT,
            estimated_cost_usd DOUBLE,
            mode STRING,
            region STRING,
            environment STRING,
            created_at TIMESTAMP,
            `date` DATE
        ) USING paimon
        PARTITIONED BY (`date`)
        TBLPROPERTIES (
            'primary-key' = 'request_id',
            'bucket' = '-1'
        )
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS paimon_lake.dws.dws_ai_llm_feature_request_1d (
            app_name STRING,
            feature_name STRING,
            model_name STRING,
            request_count BIGINT,
            success_count BIGINT,
            error_count BIGINT,
            prompt_tokens BIGINT,
            completion_tokens BIGINT,
            total_tokens BIGINT,
            estimated_cost_usd DOUBLE,
            avg_latency_ms DOUBLE,
            max_latency_ms BIGINT,
            p95_latency_ms BIGINT,
            `date` DATE
        ) USING paimon
        PARTITIONED BY (`date`)
        TBLPROPERTIES (
            'primary-key' = 'date,app_name,feature_name,model_name',
            'bucket' = '4'
        )
        """
    )


def write_quarantine_events(quarantine_events: DataFrame, output_path: Path) -> None:
    if quarantine_events.count() > 0:
        quarantine_events.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def write_paimon_tables(valid_events: DataFrame, dws_metrics: DataFrame) -> None:
    valid_events.writeTo(DWD_TABLE).overwritePartitions()
    dws_metrics.writeTo(DWS_TABLE).overwritePartitions()


def run_backfill(
    spark: SparkSession,
    input_path: Path,
    quarantine_output: Path,
    start_date: str | None = None,
    end_date: str | None = None,
    write_to_paimon: bool = True,
) -> dict[str, int]:
    raw_events = load_raw_events(spark, input_path)
    valid_events, quarantine_events = build_valid_dwd_events(raw_events, start_date=start_date, end_date=end_date)
    dws_metrics = build_feature_daily_metrics(valid_events)

    if write_to_paimon:
        ensure_paimon_tables(spark)
        write_paimon_tables(valid_events, dws_metrics)
    write_quarantine_events(quarantine_events, quarantine_output)

    return {
        "input_rows": raw_events.count(),
        "dwd_rows": valid_events.count(),
        "dws_rows": dws_metrics.count(),
        "quarantine_rows": quarantine_events.count(),
    }


def main() -> None:
    args = parse_args()
    if args.count is not None:
        write_jsonl(
            count=args.count,
            output_path=args.input,
            seed=args.seed,
            start_time=datetime.fromisoformat(args.start_time),
        )

    spark = build_paimon_spark_session("ai-observability-spark-paimon-backfill", warehouse=args.warehouse)
    started_at = datetime.now(timezone.utc)
    try:
        result = run_backfill(
            spark=spark,
            input_path=args.input,
            quarantine_output=args.quarantine_output,
            start_date=args.start_date,
            end_date=args.end_date,
            write_to_paimon=True,
        )
        log_info(LOGGER, "spark_paimon_backfill_completed", **result, input=str(args.input))
        append_pipeline_run(
            pipeline_name="spark_paimon_backfill",
            layer="dws",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=result["input_rows"],
            output_rows=result["dws_rows"],
            quarantine_rows=result["quarantine_rows"],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
