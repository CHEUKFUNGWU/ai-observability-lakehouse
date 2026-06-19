import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame

from app.dim_model import MODEL_DIMENSIONS
from app.logging_utils import get_logger, log_info
from scripts.generate_mock_compliance_logs import (
    build_mock_access_event,
    build_mock_retention_event,
)
from scripts.generate_mock_llm_logs import build_mock_event as build_mock_llm_event
from scripts.generate_mock_orchestration_logs import build_mock_event as build_mock_orchestration_event
from scripts.generate_mock_platform_health_logs import build_mock_metric, load_thresholds
from scripts.spark_build_dws_agent_orchestration_daily_metrics import (
    build_agent_orchestration_daily_metrics,
)
from scripts.spark_build_dws_llm_feature_daily_metrics import build_feature_daily_metrics
from scripts.spark_build_dws_platform_health_daily_metrics import (
    build_platform_health_daily_metrics,
    transform_platform_health_metrics,
)
from scripts.spark_transform_agent_orchestration_events import (
    transform_agent_orchestration_events,
)
from scripts.spark_transform_compliance_events import (
    transform_access_audit_events,
    transform_data_retention_events,
)
from scripts.spark_transform_llm_events import transform_llm_events
from scripts.spark_utils import build_spark_session


LOGGER = get_logger(__name__)
UTC = timezone.utc
DEFAULT_START_TIME = datetime(2026, 6, 13, 0, 0, tzinfo=UTC)
DEFAULT_LLM_RAW_PATH = Path("data/raw/mock_llm_requests/events.jsonl")
DEFAULT_ACCESS_RAW_PATH = Path("data/raw/mock_compliance_access_audit/events.jsonl")
DEFAULT_RETENTION_RAW_PATH = Path("data/raw/mock_compliance_data_retention/events.jsonl")
DEFAULT_ORCHESTRATION_RAW_PATH = Path("data/raw/mock_agent_orchestration/events.jsonl")
DEFAULT_HEALTH_RAW_PATH = Path("data/raw/mock_platform_health/events.jsonl")

DEFAULT_LLM_DWD_PATH = Path("data/warehouse/dwd/dwd_ai_llm_request_di/events.parquet")
DEFAULT_LLM_DWS_PATH = Path("data/warehouse/dws/dws_ai_llm_feature_request_1d.parquet")
DEFAULT_ACCESS_DWD_PATH = Path("data/warehouse/dwd/dwd_ai_compliance_access_audit_di/events.parquet")
DEFAULT_RETENTION_DWD_PATH = Path("data/warehouse/dwd/dwd_ai_compliance_data_retention_di/events.parquet")
DEFAULT_ORCHESTRATION_DWD_PATH = Path("data/warehouse/dwd/dwd_ai_agent_orchestration_di/events.parquet")
DEFAULT_ORCHESTRATION_DWS_PATH = Path("data/warehouse/dws/dws_ai_agent_orchestration_handoff_1d.parquet")
DEFAULT_HEALTH_DWS_PATH = Path("data/warehouse/dws/dws_ai_platform_component_health_1d.parquet")
DEFAULT_DIM_MODEL_PATH = Path("data/warehouse/dim/dim_model_df.parquet")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm-count", type=int, default=210)
    parser.add_argument("--compliance-count", type=int, default=84)
    parser.add_argument("--orchestration-count", type=int, default=140)
    parser.add_argument("--health-sample-count", type=int, default=8)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-time", type=str, default=DEFAULT_START_TIME.isoformat())
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def day_start(base_time: datetime, day_index: int) -> datetime:
    return base_time + timedelta(days=day_index)


def write_jsonl(path: Path, payloads: list[dict]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as handle:
        for payload in payloads:
            handle.write(json.dumps(payload) + "\n")


def build_llm_payloads(count: int, days: int, start_time: datetime) -> list[dict]:
    per_day = max(1, count // days)
    payloads: list[dict] = []
    for day_index in range(days):
        current_start = day_start(start_time, day_index)
        for sample_index in range(per_day):
            payloads.append(
                build_mock_llm_event(created_at=current_start + timedelta(seconds=sample_index * 137)).to_dict()
            )
    return payloads


def build_compliance_payloads(count: int, days: int, start_time: datetime) -> tuple[list[dict], list[dict]]:
    per_day = max(1, count // days)
    access_payloads: list[dict] = []
    retention_payloads: list[dict] = []
    for day_index in range(days):
        current_start = day_start(start_time, day_index)
        for sample_index in range(per_day):
            created_at = current_start + timedelta(minutes=sample_index * 11)
            access_payloads.append(build_mock_access_event(created_at).to_dict())
            retention_payloads.append(build_mock_retention_event(created_at).to_dict())
    return access_payloads, retention_payloads


def build_orchestration_payloads(count: int, days: int, start_time: datetime) -> list[dict]:
    per_day = max(1, count // days)
    payloads: list[dict] = []
    for day_index in range(days):
        current_start = day_start(start_time, day_index)
        for sample_index in range(per_day):
            payloads.append(
                build_mock_orchestration_event(
                    created_at=current_start + timedelta(minutes=sample_index * 9)
                ).to_dict()
            )
    return payloads


def build_health_payloads(sample_count: int, days: int, start_time: datetime) -> list[dict]:
    payloads: list[dict] = []
    thresholds = load_thresholds()
    for day_index in range(days):
        current_start = day_start(start_time, day_index)
        for sample_index in range(sample_count):
            created_at = current_start + timedelta(hours=sample_index * 3)
            for component, metrics in thresholds.items():
                for metric_name, threshold in metrics.items():
                    payloads.append(
                        build_mock_metric(component, metric_name, float(threshold), created_at).to_dict()
                    )
    return payloads


def write_partitioned_parquet(frame: DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.mode("overwrite").partitionBy("date").parquet(str(output_path))


def load_json_frame(spark, input_path: Path) -> DataFrame:
    return spark.read.json(str(input_path))


def write_dim_model(output_path: Path) -> int:
    spark = build_spark_session("ai-observability-dashboard-demo-dim-model")
    try:
        rows = [dimension.__dict__ for dimension in MODEL_DIMENSIONS]
        frame = spark.createDataFrame(rows)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        frame.write.mode("overwrite").parquet(str(output_path))
        return frame.count()
    finally:
        spark.stop()


def build_datasets(args: argparse.Namespace) -> None:
    start_time = datetime.fromisoformat(args.start_time)
    random.seed(args.seed)

    llm_payloads = build_llm_payloads(args.llm_count, args.days, start_time)
    access_payloads, retention_payloads = build_compliance_payloads(
        args.compliance_count, args.days, start_time
    )
    orchestration_payloads = build_orchestration_payloads(
        args.orchestration_count, args.days, start_time
    )
    health_payloads = build_health_payloads(args.health_sample_count, args.days, start_time)

    write_jsonl(DEFAULT_LLM_RAW_PATH, llm_payloads)
    write_jsonl(DEFAULT_ACCESS_RAW_PATH, access_payloads)
    write_jsonl(DEFAULT_RETENTION_RAW_PATH, retention_payloads)
    write_jsonl(DEFAULT_ORCHESTRATION_RAW_PATH, orchestration_payloads)
    write_jsonl(DEFAULT_HEALTH_RAW_PATH, health_payloads)

    spark = build_spark_session("ai-observability-dashboard-demo-datasets")
    try:
        llm_dwd = transform_llm_events(load_json_frame(spark, DEFAULT_LLM_RAW_PATH))
        llm_dws = build_feature_daily_metrics(llm_dwd)
        write_partitioned_parquet(llm_dwd, DEFAULT_LLM_DWD_PATH)
        write_partitioned_parquet(llm_dws, DEFAULT_LLM_DWS_PATH)

        access_dwd = transform_access_audit_events(load_json_frame(spark, DEFAULT_ACCESS_RAW_PATH))
        retention_dwd = transform_data_retention_events(load_json_frame(spark, DEFAULT_RETENTION_RAW_PATH))
        write_partitioned_parquet(access_dwd, DEFAULT_ACCESS_DWD_PATH)
        write_partitioned_parquet(retention_dwd, DEFAULT_RETENTION_DWD_PATH)

        orchestration_dwd = transform_agent_orchestration_events(
            load_json_frame(spark, DEFAULT_ORCHESTRATION_RAW_PATH)
        )
        orchestration_dws = build_agent_orchestration_daily_metrics(orchestration_dwd)
        write_partitioned_parquet(orchestration_dwd, DEFAULT_ORCHESTRATION_DWD_PATH)
        write_partitioned_parquet(orchestration_dws, DEFAULT_ORCHESTRATION_DWS_PATH)

        health_dws = build_platform_health_daily_metrics(
            transform_platform_health_metrics(load_json_frame(spark, DEFAULT_HEALTH_RAW_PATH))
        )
        write_partitioned_parquet(health_dws, DEFAULT_HEALTH_DWS_PATH)

        log_info(
            LOGGER,
            "dashboard_demo_datasets_built",
            llm_rows=llm_dwd.count(),
            llm_feature_rows=llm_dws.count(),
            access_rows=access_dwd.count(),
            retention_rows=retention_dwd.count(),
            orchestration_rows=orchestration_dwd.count(),
            orchestration_daily_rows=orchestration_dws.count(),
            health_daily_rows=health_dws.count(),
        )
    finally:
        spark.stop()

    dim_model_rows = write_dim_model(DEFAULT_DIM_MODEL_PATH)
    log_info(LOGGER, "dashboard_demo_dim_model_written", rows=dim_model_rows, output=str(DEFAULT_DIM_MODEL_PATH))


def main() -> None:
    args = parse_args()
    build_datasets(args)


if __name__ == "__main__":
    main()
