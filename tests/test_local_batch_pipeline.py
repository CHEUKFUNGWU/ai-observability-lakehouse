from datetime import datetime

from scripts.run_local_batch_pipeline import DEFAULT_START_TIME, build_ads, build_dwd, build_ods
from scripts.generate_mock_llm_logs import write_jsonl


def test_pipeline_raw_generation_writes_expected_rows(tmp_path):
    raw_output = tmp_path / "events.jsonl"

    write_jsonl(
        count=3,
        output_path=raw_output,
        seed=42,
        start_time=datetime.fromisoformat(DEFAULT_START_TIME),
    )

    assert raw_output.exists()
    assert len(raw_output.read_text(encoding="utf-8").splitlines()) == 3

def test_pipeline_builds_dwd_and_ads(tmp_path):
    raw_output = tmp_path / "raw" / "events.jsonl"
    ods_output = tmp_path / "warehouse" / "ods" / "llm_request" / "events.parquet"
    dwd_output = tmp_path / "warehouse" / "llm_request" / "events.parquet"
    ads_output = tmp_path / "warehouse" / "ads" / "llm_feature_daily_metrics.parquet"

    write_jsonl(
        count=5,
        output_path=raw_output,
        seed=42,
        start_time=datetime.fromisoformat(DEFAULT_START_TIME),
    )

    ods_rows = build_ods(raw_output, ods_output)
    dwd_rows = build_dwd(ods_output, dwd_output)
    ads_rows = build_ads(dwd_output, ads_output)

    assert ods_rows == 5
    assert dwd_rows == 5
    assert 0 < ads_rows <= dwd_rows
