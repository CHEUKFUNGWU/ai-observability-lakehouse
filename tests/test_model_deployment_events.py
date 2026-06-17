from app.model_version_dimension import MODEL_VERSION_DIMENSIONS
from scripts.generate_mock_model_deployment_logs import write_jsonl
from scripts.spark_transform_model_deployment_events import transform_model_deployment_events


def make_raw_model_deployment_events(spark):
    return spark.createDataFrame(
        [
            {
                "deployment_id": "deploy_001",
                "model_name": "deepseek-v4-flash",
                "model_version": "v4-flash-20260424",
                "provider": "deepseek",
                "deployment_action": "canary_start",
                "traffic_percentage": 10.0,
                "target_environment": "prod",
                "deployer_user_id": "user_0005",
                "deploy_reason": "quality_improvement",
                "status": "success",
                "created_at": "2026-06-01T00:00:00+00:00",
                "date": "2026-06-01",
            }
        ]
    )


def test_transform_model_deployment_events_casts_expected_types(spark):
    events = transform_model_deployment_events(make_raw_model_deployment_events(spark))
    schema = dict(events.dtypes)

    assert schema["deployment_id"] == "string"
    assert schema["traffic_percentage"] == "double"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"

    row = events.collect()[0]
    assert row["deployment_action"] == "canary_start"
    assert row["target_environment"] == "prod"


def test_mock_model_deployment_jsonl_writes_expected_rows(tmp_path):
    output_path = tmp_path / "deployments.jsonl"

    write_jsonl(count=4, output_path=output_path, seed=42)

    rows = output_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 4
    assert "deployment_id" in rows[0]


def test_model_version_dimension_marks_current_prod_model():
    dims = {(dimension.model_name, dimension.model_version): dimension for dimension in MODEL_VERSION_DIMENSIONS}

    assert dims[("deepseek-v4-flash", "v4-flash-20260424")].is_current_prod is True
    assert dims[("deepseek-v4-pro", "v4-pro-20260424")].deployment_status == "canary"
