from datetime import datetime

import pytest

from scripts.spark_paimon_backfill import ensure_paimon_tables, run_backfill


@pytest.mark.paimon
def test_paimon_catalog_backfill_round_trip(paimon_spark, tmp_path):
    input_path = tmp_path / "events.jsonl"
    input_path.write_text(
        "\n".join(
            [
                '{"request_id":"req_1","user_id":"user_1","session_id":"session_1","app_name":"app","feature_name":"chat","prompt_category":"support","prompt_id":"prompt_1","prompt_version":"v1","model_name":"deepseek-chat","provider":"deepseek","prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"latency_ms":100,"status":"success","error_type":"","http_status":200,"estimated_cost_usd":0.01,"mode":"mock","region":"us","environment":"dev","created_at":"2026-01-01T00:00:00Z","date":"2026-01-01"}',
                '{"request_id":"req_2","user_id":"user_2","session_id":"session_2","app_name":"app","feature_name":"chat","prompt_category":"support","prompt_id":"prompt_1","prompt_version":"v1","model_name":"deepseek-chat","provider":"deepseek","prompt_tokens":8,"completion_tokens":2,"total_tokens":10,"latency_ms":200,"status":"error","error_type":"timeout","http_status":500,"estimated_cost_usd":0.02,"mode":"mock","region":"us","environment":"dev","created_at":"2026-01-01T00:01:00Z","date":"2026-01-01"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ensure_paimon_tables(paimon_spark)
    result = run_backfill(
        spark=paimon_spark,
        input_path=input_path,
        quarantine_output=tmp_path / "quarantine",
    )

    assert result["dwd_rows"] == 2
    assert result["quarantine_rows"] == 0
    assert paimon_spark.table("paimon_lake.dwd.llm_request_events").count() == 2
    metrics = paimon_spark.table("paimon_lake.dws.llm_feature_daily_metrics").collect()
    assert len(metrics) == 1
    assert metrics[0]["max_latency_ms"] == 200
