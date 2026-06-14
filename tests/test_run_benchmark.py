from pathlib import Path

import scripts.run_benchmark as run_benchmark


class FakeSparkSession:
    def stop(self) -> None:
        return None


def test_run_scale_passes_quarantine_path_and_returns_quarantine_rows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    captured = {}

    def fake_write_jsonl(**kwargs):
        output_path = kwargs["output_path"]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("{}\n", encoding="utf-8")

    def fake_build_spark_session(_app_name):
        return FakeSparkSession()

    def fake_build_ods(_spark, _raw_output, ods_output):
        ods_output.mkdir(parents=True, exist_ok=True)

    def fake_build_dwd(_spark, _ods_output, _dwd_output, quarantine_output):
        captured["quarantine_output"] = quarantine_output
        return 10, 3

    def fake_build_ads(_spark, _dwd_output, ads_output):
        ads_output.mkdir(parents=True, exist_ok=True)
        (ads_output / "part-000.parquet").write_bytes(b"data")

    monkeypatch.setattr(run_benchmark, "write_jsonl", fake_write_jsonl)
    monkeypatch.setattr(run_benchmark, "build_spark_session", fake_build_spark_session)
    monkeypatch.setattr(run_benchmark, "build_ods", fake_build_ods)
    monkeypatch.setattr(run_benchmark, "build_dwd", fake_build_dwd)
    monkeypatch.setattr(run_benchmark, "build_ads", fake_build_ads)

    result = run_benchmark.run_scale(10)

    assert result["quarantine_rows"] == 3
    assert result["doris_query_p95_ms"] == "n/a"
    assert captured["quarantine_output"] == Path("data/benchmarks/10/warehouse/quarantine/events.parquet")


def test_render_markdown_includes_quarantine_column():
    markdown = run_benchmark.render_markdown(
        [
            {
                "scale": 10,
                "ods_duration": 1.0,
                "dwd_duration": 2.0,
                "ads_duration": 3.0,
                "parquet_size_mb": 4.0,
                "quarantine_rows": 5,
                "doris_query_p95_ms": "n/a",
            }
        ]
    )

    assert "Quarantine Rows" in markdown
    assert "| 10 | 1.0 | 2.0 | 3.0 | 4.0 | 5 | n/a |" in markdown
