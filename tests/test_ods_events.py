import pytest
from pyspark.sql import SparkSession

from scripts.spark_build_ods_agent_events import build_ods_agent_events
from scripts.spark_build_ods_llm_events import build_ods_llm_events


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("test-ods-events")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_ods_llm_events_adds_source_metadata(spark):
    raw_events = spark.createDataFrame(
        [
            {
                "request_id": "req_001",
                "status": "success",
                "prompt_tokens": 10,
                "date": "2026-01-01",
            }
        ]
    )

    ods_events = build_ods_llm_events(raw_events, source_name="unit_test_source")
    row = ods_events.first().asDict()
    schema = dict(ods_events.dtypes)

    assert row["request_id"] == "req_001"
    assert row["source_name"] == "unit_test_source"
    assert row["source_event_type"] == "llm_request"
    assert schema["ingested_at"] == "timestamp"
    assert "request_id" in row["raw_event_json"]


def test_build_ods_agent_events_adds_source_metadata(spark):
    raw_events = spark.createDataFrame(
        [
            {
                "run_id": "run_001",
                "agent_id": "agent_support",
                "status": "success",
                "date": "2026-01-01",
            }
        ]
    )

    ods_events = build_ods_agent_events(
        raw_events,
        source_name="unit_test_source",
        source_event_type="agent_run",
    )
    row = ods_events.first().asDict()
    schema = dict(ods_events.dtypes)

    assert row["run_id"] == "run_001"
    assert row["source_name"] == "unit_test_source"
    assert row["source_event_type"] == "agent_run"
    assert schema["ingested_at"] == "timestamp"
    assert "run_id" in row["raw_event_json"]
