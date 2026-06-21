import zipfile

from scripts.provision_superset_dashboards import (
    build_specs,
    chart_export_config,
    dashboard_export_config,
    write_dashboard_bundles,
)


def test_dashboard_positions_reference_declared_charts() -> None:
    _, datasets, charts, dashboards = build_specs()

    for dashboard in dashboards.values():
        exported = dashboard_export_config(dashboard, charts)
        referenced = {
            node["meta"]["uuid"]
            for node in exported["position"].values()
            if isinstance(node, dict) and node.get("type") == "CHART"
        }
        expected = {charts[item.chart_key].uuid for row in dashboard.chart_rows for item in row}
        assert referenced == expected

    assert "superset_ai_overview_daily" == datasets["ai_overview_daily"]["table_name"]
    assert "dws_ai_llm_feature_request_1d" in datasets["ai_overview_daily"]["sql"]


def test_bundle_writer_creates_importable_zip_layout(tmp_path) -> None:
    bundle_paths = write_dashboard_bundles(tmp_path)
    assert sorted(path.name for path in bundle_paths) == [
        "agent_orchestration.zip",
        "ai_overview.zip",
        "compliance.zip",
    ]

    _, datasets, charts, dashboards = build_specs()
    ai_overview = dashboards["ai_overview"]

    with zipfile.ZipFile(tmp_path / "ai_overview.zip") as archive:
        names = set(archive.namelist())
        assert "dashboard_export_ai_overview/metadata.yaml" in names
        assert "dashboard_export_ai_overview/databases/AI_Observability_Doris.yaml" in names
        assert "dashboard_export_ai_overview/dashboards/AI_Observability_Overview.yaml" in names

        expected_datasets = {
            f"dashboard_export_ai_overview/datasets/AI_Observability_Doris/{datasets[charts[item.chart_key].dataset_key]['table_name']}.yaml"
            for row in ai_overview.chart_rows
            for item in row
        }
        assert expected_datasets.issubset(names)

        expected_charts = {
            f"dashboard_export_ai_overview/charts/{chart_export_config(charts[item.chart_key], datasets)['slice_name'].replace('&', 'And').replace('/', '_').replace(' ', '_')}.yaml"
            for row in ai_overview.chart_rows
            for item in row
        }
        assert expected_charts.issubset(names)
