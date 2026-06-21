from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5
import zipfile

import yaml


EXPORT_VERSION = "1.0.0"
EXPORT_TYPE = "Dashboard"
DATABASE_NAME = "AI Observability (Doris)"
DATABASE_URI = "mysql+pymysql://root:@doris-fe:9030/ai_observability"
EXPORT_TIMESTAMP = datetime(2026, 6, 20, tzinfo=timezone.utc).isoformat()


def stable_uuid(name: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"https://ai-observability.local/superset/{name}"))


def metric(metric_name: str, expression: str, d3format: str | None = None) -> dict[str, Any]:
    return {
        "metric_name": metric_name,
        "verbose_name": None,
        "metric_type": None,
        "expression": expression,
        "description": None,
        "d3format": d3format,
        "extra": None,
        "warning_text": None,
    }


def column(column_name: str, column_type: str, *, is_dttm: bool = False) -> dict[str, Any]:
    return {
        "column_name": column_name,
        "verbose_name": None,
        "is_dttm": is_dttm,
        "is_active": True,
        "type": column_type,
        "groupby": True,
        "filterable": True,
        "expression": None,
        "description": None,
        "python_date_format": None,
    }


def base_dataset(
    key: str,
    sql: str,
    columns: list[dict[str, Any]],
    *,
    metrics: list[dict[str, Any]] | None = None,
    main_dttm_col: str | None = None,
) -> dict[str, Any]:
    return {
        "table_name": f"superset_{key}",
        "main_dttm_col": main_dttm_col,
        "description": None,
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "catalog": None,
        "schema": "ai_observability",
        "sql": sql.strip(),
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "uuid": stable_uuid(f"dataset.{key}"),
        "metrics": metrics or [],
        "columns": columns,
        "version": EXPORT_VERSION,
        "database_uuid": stable_uuid("database.ai_observability_doris"),
    }


def big_number_params(metric_name: str, *, y_axis_format: str = "SMART_NUMBER") -> dict[str, Any]:
    return {
        "datasource": "0__table",
        "viz_type": "big_number_total",
        "metric": metric_name,
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "y_axis_format": y_axis_format,
        "time_format": "smart_date",
        "extra_form_data": {},
    }


def line_params(x_axis: str, metrics: list[str]) -> dict[str, Any]:
    return {
        "datasource": "0__table",
        "viz_type": "echarts_timeseries_line",
        "x_axis": x_axis,
        "metrics": metrics,
        "groupby": [],
        "x_axis_sort_asc": True,
        "x_axis_sort_series": "name",
        "x_axis_sort_series_ascending": True,
        "order_desc": False,
        "row_limit": 10000,
        "truncate_metric": True,
        "show_empty_columns": True,
        "comparison_type": "values",
        "annotation_layers": [],
        "x_axis_title_margin": 15,
        "y_axis_title_margin": 15,
        "y_axis_title_position": "Left",
        "sort_series_type": "sum",
        "color_scheme": "supersetColors",
        "seriesType": "line",
        "only_total": False,
        "opacity": 0.2,
        "markerSize": 6,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "x_axis_time_format": "smart_date",
        "rich_tooltip": True,
        "tooltipTimeFormat": "smart_date",
        "y_axis_format": "SMART_NUMBER",
        "truncateXAxis": True,
        "y_axis_bounds": [None, None],
        "extra_form_data": {},
    }


def pie_params(groupby: list[str], metric_name: str) -> dict[str, Any]:
    return {
        "datasource": "0__table",
        "viz_type": "pie",
        "groupby": groupby,
        "metric": metric_name,
        "row_limit": 100,
        "sort_by_metric": True,
        "color_scheme": "supersetColors",
        "show_labels_threshold": 5,
        "show_legend": True,
        "legendType": "scroll",
        "legendOrientation": "top",
        "label_type": "key",
        "number_format": "SMART_NUMBER",
        "date_format": "smart_date",
        "show_labels": True,
        "labels_outside": True,
        "outerRadius": 70,
        "innerRadius": 30,
        "donut": True,
        "extra_form_data": {},
    }


def table_params(
    all_columns: list[str],
    *,
    show_cell_bars: bool = False,
    order_desc: bool = True,
    temporal_columns: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "datasource": "0__table",
        "viz_type": "table",
        "query_mode": "raw",
        "groupby": [],
        "all_columns": all_columns,
        "percent_metrics": [],
        "order_by_cols": [],
        "row_limit": 1000,
        "server_page_length": 10,
        "order_desc": order_desc,
        "table_timestamp_format": "smart_date",
        "show_cell_bars": show_cell_bars,
        "color_pn": show_cell_bars,
        "allow_render_html": True,
        "temporal_columns_lookup": {name: True for name in temporal_columns or []},
        "extra_form_data": {},
    }


@dataclass(frozen=True)
class ChartSpec:
    key: str
    title: str
    dataset_key: str
    viz_type: str
    params: dict[str, Any]

    @property
    def uuid(self) -> str:
        return stable_uuid(f"chart.{self.key}")


@dataclass(frozen=True)
class DashboardChart:
    chart_key: str
    width: int
    height: int


@dataclass(frozen=True)
class DashboardSpec:
    key: str
    title: str
    chart_rows: list[list[DashboardChart]]

    @property
    def uuid(self) -> str:
        return stable_uuid(f"dashboard.{self.key}")


def dashboard_position(spec: DashboardSpec, charts: dict[str, ChartSpec]) -> dict[str, Any]:
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {
            "children": [],
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
            "type": "GRID",
        },
        "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": spec.title}, "type": "HEADER"},
    }

    chart_id = 1
    for row_index, row in enumerate(spec.chart_rows, start=1):
        row_id = f"ROW-{spec.key.upper()}-{row_index}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "children": [],
            "id": row_id,
            "meta": {"0": "ROOT_ID", "background": "BACKGROUND_TRANSPARENT"},
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
        }
        for item_index, item in enumerate(row, start=1):
            chart_spec = charts[item.chart_key]
            chart_node_id = f"CHART-{spec.key.upper()}-{row_index}-{item_index}"
            position[row_id]["children"].append(chart_node_id)
            position[chart_node_id] = {
                "children": [],
                "id": chart_node_id,
                "meta": {
                    "chartId": chart_id,
                    "height": item.height,
                    "sliceName": chart_spec.title,
                    "uuid": chart_spec.uuid,
                    "width": item.width,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "type": "CHART",
            }
            chart_id += 1
    return position


def build_specs() -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, ChartSpec], dict[str, DashboardSpec]]:
    database = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": DATABASE_URI,
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        "extra": {
            "metadata_params": {},
            "engine_params": {},
            "metadata_cache_timeout": {},
            "schemas_allowed_for_file_upload": [],
        },
        "uuid": stable_uuid("database.ai_observability_doris"),
        "version": EXPORT_VERSION,
    }

    datasets = {
        "kpi_total_requests": base_dataset(
            "kpi_total_requests",
            """
            SELECT SUM(request_count) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            """,
            [column("value", "BIGINT")],
            metrics=[metric("value", "MAX(value)")],
        ),
        "kpi_success_rate": base_dataset(
            "kpi_success_rate",
            """
            SELECT ROUND(SUM(success_count) / NULLIF(SUM(request_count), 0), 4) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            """,
            [column("value", "DOUBLE PRECISION")],
            metrics=[metric("value", "MAX(value)")],
        ),
        "kpi_total_cost": base_dataset(
            "kpi_total_cost",
            """
            SELECT ROUND(SUM(estimated_cost_usd), 8) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            """,
            [column("value", "DOUBLE PRECISION")],
            metrics=[metric("value", "MAX(value)", "$,.4f")],
        ),
        "kpi_avg_latency": base_dataset(
            "kpi_avg_latency",
            """
            SELECT ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            """,
            [column("value", "DOUBLE PRECISION")],
            metrics=[metric("value", "MAX(value)")],
        ),
        "ai_overview_daily": base_dataset(
            "ai_overview_daily",
            """
            SELECT
                `date`,
                request_count,
                success_count,
                error_count,
                ROUND(success_count / NULLIF(request_count, 0), 4) AS success_rate,
                ROUND(error_count / NULLIF(request_count, 0), 4) AS error_rate,
                total_tokens,
                estimated_cost_usd
            FROM
            (
                SELECT
                    `date`,
                    SUM(request_count) AS request_count,
                    SUM(success_count) AS success_count,
                    SUM(error_count) AS error_count,
                    SUM(total_tokens) AS total_tokens,
                    ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd
                FROM ai_observability.dws_ai_llm_feature_request_1d
                GROUP BY `date`
            ) daily
            ORDER BY `date`
            """,
            [
                column("date", "DATE", is_dttm=True),
                column("request_count", "BIGINT"),
                column("success_count", "BIGINT"),
                column("error_count", "BIGINT"),
                column("success_rate", "DOUBLE PRECISION"),
                column("error_rate", "DOUBLE PRECISION"),
                column("total_tokens", "BIGINT"),
                column("estimated_cost_usd", "DOUBLE PRECISION"),
            ],
            metrics=[
                metric("request_count", "SUM(request_count)"),
                metric("estimated_cost_usd", "SUM(estimated_cost_usd)", "$,.4f"),
                metric("error_rate", "AVG(error_rate)"),
            ],
            main_dttm_col="date",
        ),
        "request_by_feature": base_dataset(
            "request_by_feature",
            """
            SELECT
                feature_name,
                SUM(request_count) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            GROUP BY feature_name
            ORDER BY value DESC
            """,
            [column("feature_name", "STRING"), column("value", "BIGINT")],
            metrics=[metric("value", "SUM(value)")],
        ),
        "cost_by_feature": base_dataset(
            "cost_by_feature",
            """
            SELECT
                feature_name,
                ROUND(SUM(estimated_cost_usd), 8) AS value
            FROM ai_observability.dws_ai_llm_feature_request_1d
            GROUP BY feature_name
            ORDER BY value DESC
            """,
            [column("feature_name", "STRING"), column("value", "DOUBLE PRECISION")],
            metrics=[metric("value", "SUM(value)", "$,.4f")],
        ),
        "feature_reliability": base_dataset(
            "feature_reliability",
            """
            SELECT
                feature_name,
                request_count,
                success_count,
                error_count,
                ROUND(success_count / NULLIF(request_count, 0), 4) AS success_rate,
                ROUND(error_count / NULLIF(request_count, 0), 4) AS error_rate
            FROM
            (
                SELECT
                    feature_name,
                    SUM(request_count) AS request_count,
                    SUM(success_count) AS success_count,
                    SUM(error_count) AS error_count
                FROM ai_observability.dws_ai_llm_feature_request_1d
                GROUP BY feature_name
            ) feature_rollup
            ORDER BY error_rate DESC, request_count DESC
            """,
            [
                column("feature_name", "STRING"),
                column("request_count", "BIGINT"),
                column("success_count", "BIGINT"),
                column("error_count", "BIGINT"),
                column("success_rate", "DOUBLE PRECISION"),
                column("error_rate", "DOUBLE PRECISION"),
            ],
        ),
        "feature_latency": base_dataset(
            "feature_latency",
            """
            SELECT
                feature_name,
                ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS weighted_avg_latency_ms
            FROM ai_observability.dws_ai_llm_feature_request_1d
            GROUP BY feature_name
            ORDER BY weighted_avg_latency_ms DESC
            """,
            [column("feature_name", "STRING"), column("weighted_avg_latency_ms", "DOUBLE PRECISION")],
        ),
        "cost_by_model": base_dataset(
            "cost_by_model",
            """
            SELECT
                model_name,
                request_count,
                total_tokens,
                estimated_cost_usd,
                ROUND(estimated_cost_usd / NULLIF(request_count, 0), 8) AS avg_cost_per_request
            FROM
            (
                SELECT
                    model_name,
                    SUM(request_count) AS request_count,
                    SUM(total_tokens) AS total_tokens,
                    ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd
                FROM ai_observability.dws_ai_llm_feature_request_1d
                GROUP BY model_name
            ) model_rollup
            ORDER BY estimated_cost_usd DESC
            """,
            [
                column("model_name", "STRING"),
                column("request_count", "BIGINT"),
                column("total_tokens", "BIGINT"),
                column("estimated_cost_usd", "DOUBLE PRECISION"),
                column("avg_cost_per_request", "DOUBLE PRECISION"),
            ],
            metrics=[metric("estimated_cost_usd", "MAX(estimated_cost_usd)", "$,.4f")],
        ),
        "leaderboard": base_dataset(
            "leaderboard",
            """
            SELECT
                app_name,
                feature_name,
                request_count_sum AS request_count,
                estimated_cost_usd,
                weighted_avg_latency_ms,
                ROUND(error_count_sum / NULLIF(request_count_sum, 0), 4) AS error_rate
            FROM
            (
                SELECT
                    app_name,
                    feature_name,
                    SUM(request_count) AS request_count_sum,
                    SUM(error_count) AS error_count_sum,
                    ROUND(SUM(estimated_cost_usd), 8) AS estimated_cost_usd,
                    ROUND(SUM(avg_latency_ms * request_count) / NULLIF(SUM(request_count), 0), 2) AS weighted_avg_latency_ms
                FROM ai_observability.dws_ai_llm_feature_request_1d
                GROUP BY app_name, feature_name
            ) leaderboard
            ORDER BY request_count_sum DESC
            """,
            [
                column("app_name", "STRING"),
                column("feature_name", "STRING"),
                column("request_count", "BIGINT"),
                column("estimated_cost_usd", "DOUBLE PRECISION"),
                column("weighted_avg_latency_ms", "DOUBLE PRECISION"),
                column("error_rate", "DOUBLE PRECISION"),
            ],
        ),
        "model_pricing": base_dataset(
            "model_pricing",
            """
            SELECT
                m.model_name,
                m.provider,
                m.input_price_per_1m_tokens,
                m.output_price_per_1m_tokens,
                a.request_count,
                a.total_tokens,
                a.estimated_cost_usd
            FROM
            (
                SELECT
                    model_name,
                    SUM(request_count) AS request_count,
                    SUM(total_tokens) AS total_tokens,
                    SUM(estimated_cost_usd) AS estimated_cost_usd
                FROM ai_observability.dws_ai_llm_feature_request_1d
                GROUP BY model_name
            ) a
            JOIN ai_observability.dim_model_df m ON a.model_name = m.model_name
            ORDER BY estimated_cost_usd DESC
            """,
            [
                column("model_name", "STRING"),
                column("provider", "STRING"),
                column("input_price_per_1m_tokens", "DOUBLE PRECISION"),
                column("output_price_per_1m_tokens", "DOUBLE PRECISION"),
                column("request_count", "BIGINT"),
                column("total_tokens", "BIGINT"),
                column("estimated_cost_usd", "DOUBLE PRECISION"),
            ],
        ),
        "compliance_denied_access": base_dataset(
            "compliance_denied_access",
            """
            SELECT
                `date`,
                data_classification,
                action_type,
                denial_reason,
                COUNT(*) AS denied_access_cnt_1d,
                COUNT(DISTINCT user_id) AS denied_user_cnt_1d
            FROM ai_observability.dwd_ai_compliance_access_audit_di
            WHERE access_granted = FALSE
            GROUP BY `date`, data_classification, action_type, denial_reason
            ORDER BY `date` DESC, denied_access_cnt_1d DESC
            """,
            [
                column("date", "DATE", is_dttm=True),
                column("data_classification", "STRING"),
                column("action_type", "STRING"),
                column("denial_reason", "STRING"),
                column("denied_access_cnt_1d", "BIGINT"),
                column("denied_user_cnt_1d", "BIGINT"),
            ],
            main_dttm_col="date",
        ),
        "compliance_retention": base_dataset(
            "compliance_retention",
            """
            SELECT
                `date`,
                policy_name,
                table_name,
                action_type,
                COUNT(*) AS retention_action_cnt_1d,
                SUM(rows_affected) AS rows_affected_cnt_1d
            FROM ai_observability.dwd_ai_compliance_data_retention_di
            GROUP BY `date`, policy_name, table_name, action_type
            ORDER BY `date` DESC, rows_affected_cnt_1d DESC
            """,
            [
                column("date", "DATE", is_dttm=True),
                column("policy_name", "STRING"),
                column("table_name", "STRING"),
                column("action_type", "STRING"),
                column("retention_action_cnt_1d", "BIGINT"),
                column("rows_affected_cnt_1d", "BIGINT"),
            ],
            main_dttm_col="date",
        ),
        "agent_handoffs": base_dataset(
            "agent_handoffs",
            """
            SELECT
                `date`,
                parent_agent_id,
                child_agent_id,
                handoff_type,
                handoff_cnt_1d,
                error_cnt_1d,
                timeout_cnt_1d,
                avg_handoff_latency_ms,
                p95_handoff_latency_ms
            FROM ai_observability.dws_ai_agent_orchestration_handoff_1d
            ORDER BY `date` DESC, p95_handoff_latency_ms DESC, timeout_cnt_1d DESC
            """,
            [
                column("date", "DATE", is_dttm=True),
                column("parent_agent_id", "STRING"),
                column("child_agent_id", "STRING"),
                column("handoff_type", "STRING"),
                column("handoff_cnt_1d", "BIGINT"),
                column("error_cnt_1d", "BIGINT"),
                column("timeout_cnt_1d", "BIGINT"),
                column("avg_handoff_latency_ms", "DOUBLE PRECISION"),
                column("p95_handoff_latency_ms", "DOUBLE PRECISION"),
            ],
            main_dttm_col="date",
        ),
    }

    charts = {
        "total_requests": ChartSpec(
            key="total_requests",
            title="Total Requests",
            dataset_key="kpi_total_requests",
            viz_type="big_number_total",
            params=big_number_params("value"),
        ),
        "success_rate": ChartSpec(
            key="success_rate",
            title="Success Rate",
            dataset_key="kpi_success_rate",
            viz_type="big_number_total",
            params=big_number_params("value", y_axis_format=".2%"),
        ),
        "total_cost": ChartSpec(
            key="total_cost",
            title="Total Cost",
            dataset_key="kpi_total_cost",
            viz_type="big_number_total",
            params=big_number_params("value", y_axis_format="$,.4f"),
        ),
        "avg_latency": ChartSpec(
            key="avg_latency",
            title="Average Latency (ms)",
            dataset_key="kpi_avg_latency",
            viz_type="big_number_total",
            params=big_number_params("value"),
        ),
        "daily_trend": ChartSpec(
            key="daily_trend",
            title="Daily Requests, Cost, and Error Rate",
            dataset_key="ai_overview_daily",
            viz_type="echarts_timeseries_line",
            params=line_params("date", ["request_count", "estimated_cost_usd", "error_rate"]),
        ),
        "cost_by_feature": ChartSpec(
            key="cost_by_feature",
            title="Cost by Feature",
            dataset_key="cost_by_feature",
            viz_type="pie",
            params=pie_params(["feature_name"], "value"),
        ),
        "request_by_feature": ChartSpec(
            key="request_by_feature",
            title="Request Volume by Feature",
            dataset_key="request_by_feature",
            viz_type="table",
            params=table_params(["feature_name", "value"], show_cell_bars=True),
        ),
        "feature_reliability": ChartSpec(
            key="feature_reliability",
            title="Reliability by Feature",
            dataset_key="feature_reliability",
            viz_type="table",
            params=table_params(
                ["feature_name", "request_count", "success_count", "error_count", "success_rate", "error_rate"],
                show_cell_bars=True,
            ),
        ),
        "feature_latency": ChartSpec(
            key="feature_latency",
            title="Latency by Feature",
            dataset_key="feature_latency",
            viz_type="table",
            params=table_params(["feature_name", "weighted_avg_latency_ms"], show_cell_bars=True),
        ),
        "cost_by_model": ChartSpec(
            key="cost_by_model",
            title="Cost by Model",
            dataset_key="cost_by_model",
            viz_type="pie",
            params=pie_params(["model_name"], "estimated_cost_usd"),
        ),
        "leaderboard": ChartSpec(
            key="leaderboard",
            title="App and Feature Leaderboard",
            dataset_key="leaderboard",
            viz_type="table",
            params=table_params(
                ["app_name", "feature_name", "request_count", "estimated_cost_usd", "weighted_avg_latency_ms", "error_rate"],
                show_cell_bars=True,
            ),
        ),
        "model_pricing": ChartSpec(
            key="model_pricing",
            title="Model Pricing Table",
            dataset_key="model_pricing",
            viz_type="table",
            params=table_params(
                [
                    "model_name",
                    "provider",
                    "input_price_per_1m_tokens",
                    "output_price_per_1m_tokens",
                    "request_count",
                    "total_tokens",
                    "estimated_cost_usd",
                ],
                show_cell_bars=True,
            ),
        ),
        "denied_access": ChartSpec(
            key="denied_access",
            title="Denied Access Attempts",
            dataset_key="compliance_denied_access",
            viz_type="table",
            params=table_params(
                ["date", "data_classification", "action_type", "denial_reason", "denied_access_cnt_1d", "denied_user_cnt_1d"],
                show_cell_bars=True,
                temporal_columns=["date"],
            ),
        ),
        "retention_enforcement": ChartSpec(
            key="retention_enforcement",
            title="Retention Policy Enforcement",
            dataset_key="compliance_retention",
            viz_type="table",
            params=table_params(
                ["date", "policy_name", "table_name", "action_type", "retention_action_cnt_1d", "rows_affected_cnt_1d"],
                show_cell_bars=True,
                temporal_columns=["date"],
            ),
        ),
        "handoff_bottlenecks": ChartSpec(
            key="handoff_bottlenecks",
            title="Inter-Agent Handoff Bottlenecks",
            dataset_key="agent_handoffs",
            viz_type="table",
            params=table_params(
                [
                    "date",
                    "parent_agent_id",
                    "child_agent_id",
                    "handoff_type",
                    "handoff_cnt_1d",
                    "error_cnt_1d",
                    "timeout_cnt_1d",
                    "avg_handoff_latency_ms",
                    "p95_handoff_latency_ms",
                ],
                show_cell_bars=True,
                temporal_columns=["date"],
            ),
        ),
    }

    dashboards = {
        "ai_overview": DashboardSpec(
            key="ai_overview",
            title="AI Observability Overview",
            chart_rows=[
                [
                    DashboardChart("total_requests", 3, 18),
                    DashboardChart("success_rate", 3, 18),
                    DashboardChart("total_cost", 3, 18),
                    DashboardChart("avg_latency", 3, 18),
                ],
                [
                    DashboardChart("daily_trend", 8, 32),
                    DashboardChart("cost_by_feature", 4, 32),
                ],
                [
                    DashboardChart("request_by_feature", 4, 24),
                    DashboardChart("cost_by_model", 4, 24),
                    DashboardChart("feature_reliability", 4, 24),
                ],
                [
                    DashboardChart("feature_latency", 4, 24),
                    DashboardChart("leaderboard", 4, 24),
                    DashboardChart("model_pricing", 4, 24),
                ],
            ],
        ),
        "compliance": DashboardSpec(
            key="compliance",
            title="Compliance & Governance",
            chart_rows=[
                [
                    DashboardChart("denied_access", 6, 32),
                    DashboardChart("retention_enforcement", 6, 32),
                ]
            ],
        ),
        "agent_orchestration": DashboardSpec(
            key="agent_orchestration",
            title="Agent Orchestration",
            chart_rows=[[DashboardChart("handoff_bottlenecks", 12, 36)]],
        ),
    }

    return database, datasets, charts, dashboards


def dashboard_export_config(spec: DashboardSpec, charts: dict[str, ChartSpec]) -> dict[str, Any]:
    return {
        "dashboard_title": spec.title,
        "description": None,
        "css": None,
        "slug": None,
        "certified_by": None,
        "certification_details": None,
        "published": True,
        "uuid": spec.uuid,
        "position": dashboard_position(spec, charts),
        "metadata": {},
        "version": EXPORT_VERSION,
    }


def chart_export_config(spec: ChartSpec, datasets: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "slice_name": spec.title,
        "description": None,
        "certified_by": None,
        "certification_details": None,
        "viz_type": spec.viz_type,
        "params": spec.params.copy(),
        "query_context": None,
        "cache_timeout": None,
        "uuid": spec.uuid,
        "version": EXPORT_VERSION,
        "dataset_uuid": datasets[spec.dataset_key]["uuid"],
    }


def database_export_path() -> str:
    return "databases/AI_Observability_Doris.yaml"


def dataset_export_path(dataset: dict[str, Any]) -> str:
    return f"datasets/AI_Observability_Doris/{dataset['table_name']}.yaml"


def chart_export_path(spec: ChartSpec) -> str:
    safe_title = spec.title.replace("&", "And").replace("/", "_").replace(" ", "_")
    return f"charts/{safe_title}.yaml"


def dashboard_export_path(spec: DashboardSpec) -> str:
    safe_title = spec.title.replace("&", "And").replace("/", "_").replace(" ", "_")
    return f"dashboards/{safe_title}.yaml"


def write_dashboard_bundles(output_dir: Path) -> list[Path]:
    database, datasets, charts, dashboards = build_specs()
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle_paths: list[Path] = []

    for dashboard_key, dashboard_spec in dashboards.items():
        bundle_path = output_dir / f"{dashboard_key}.zip"
        related_chart_keys = [item.chart_key for row in dashboard_spec.chart_rows for item in row]
        related_charts = {key: charts[key] for key in related_chart_keys}
        related_dataset_keys = {charts[key].dataset_key for key in related_chart_keys}

        root = f"dashboard_export_{dashboard_key}"
        with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                f"{root}/metadata.yaml",
                yaml.safe_dump(
                    {
                        "version": EXPORT_VERSION,
                        "type": EXPORT_TYPE,
                        "timestamp": EXPORT_TIMESTAMP,
                    },
                    sort_keys=False,
                ),
            )
            archive.writestr(
                f"{root}/{database_export_path()}",
                yaml.safe_dump(database, sort_keys=False),
            )
            for dataset_key in sorted(related_dataset_keys):
                dataset = datasets[dataset_key]
                archive.writestr(
                    f"{root}/{dataset_export_path(dataset)}",
                    yaml.safe_dump(dataset, sort_keys=False),
                )
            for chart_key in related_chart_keys:
                chart = related_charts[chart_key]
                archive.writestr(
                    f"{root}/{chart_export_path(chart)}",
                    yaml.safe_dump(chart_export_config(chart, datasets), sort_keys=False),
                )
            archive.writestr(
                f"{root}/{dashboard_export_path(dashboard_spec)}",
                yaml.safe_dump(dashboard_export_config(dashboard_spec, charts), sort_keys=False),
            )
        bundle_paths.append(bundle_path)

    return bundle_paths


def provision_dashboards() -> None:
    from sqlalchemy import delete

    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.extensions import db
        from superset import security_manager
        from superset.commands.chart.importers.v1.utils import import_chart
        from superset.commands.dashboard.importers.v1.utils import import_dashboard, update_id_refs, find_chart_uuids
        from superset.commands.database.importers.v1.utils import import_database
        from superset.commands.dataset.importers.v1.utils import import_dataset
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard, dashboard_slices

        database_config, datasets, charts, dashboards = build_specs()

        admin = security_manager.find_user(username="admin")

        prototype = db.session.query(Dashboard).filter_by(dashboard_title="Superset Prototype Dashboard").one_or_none()
        if prototype:
            db.session.execute(delete(dashboard_slices).where(dashboard_slices.c.dashboard_id == prototype.id))
            db.session.delete(prototype)

        database = import_database(database_config, overwrite=True, ignore_permissions=True)
        if admin and hasattr(database, "owners") and admin not in database.owners:
            database.owners.append(admin)
        if str(database.uuid) != database_config["uuid"]:
            database.uuid = database_config["uuid"]

        dataset_models: dict[str, SqlaTable] = {}
        dataset_info: dict[str, dict[str, Any]] = {}
        for dataset_key, dataset_config in datasets.items():
            config = dataset_config.copy()
            config["database_id"] = database.id
            dataset = import_dataset(config, overwrite=True, ignore_permissions=True)
            if admin and admin not in dataset.owners:
                dataset.owners.append(admin)
            dataset_models[dataset_key] = dataset
            dataset_info[dataset_config["uuid"]] = {
                "datasource_id": dataset.id,
                "datasource_type": dataset.datasource_type,
                "datasource_name": dataset.table_name,
            }

        chart_ids: dict[str, int] = {}
        for chart_key, chart_spec in charts.items():
            dataset = dataset_models[chart_spec.dataset_key]
            config = chart_export_config(chart_spec, datasets)
            config.update(
                {
                    "datasource_id": dataset.id,
                    "datasource_type": dataset.datasource_type,
                    "datasource_name": dataset.table_name,
                }
            )
            params = config["params"].copy()
            params["datasource"] = dataset.uid
            config["params"] = params
            chart = import_chart(config, overwrite=True, ignore_permissions=True)
            if admin and admin not in chart.owners:
                chart.owners.append(admin)
            chart_ids[chart_spec.uuid] = chart.id

        expected_dashboard_titles = {spec.title for spec in dashboards.values()}
        for stale_dashboard in db.session.query(Dashboard).filter(Dashboard.dashboard_title.in_(expected_dashboard_titles)).all():
            if str(stale_dashboard.uuid) not in {spec.uuid for spec in dashboards.values()}:
                db.session.execute(delete(dashboard_slices).where(dashboard_slices.c.dashboard_id == stale_dashboard.id))
                db.session.delete(stale_dashboard)

        for dashboard_spec in dashboards.values():
            config = dashboard_export_config(dashboard_spec, charts)
            resolved = update_id_refs(config, chart_ids, dataset_info)
            dashboard = import_dashboard(resolved, overwrite=True, ignore_permissions=True)
            if admin and admin not in dashboard.owners:
                dashboard.owners.append(admin)

            db.session.execute(delete(dashboard_slices).where(dashboard_slices.c.dashboard_id == dashboard.id))
            links = [
                {"dashboard_id": dashboard.id, "slice_id": chart_ids[chart_uuid]}
                for chart_uuid in find_chart_uuids(resolved["position"])
            ]
            if links:
                db.session.execute(dashboard_slices.insert(), links)

        db.session.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision and export Superset dashboards.")
    parser.add_argument(
        "--write-bundles",
        type=Path,
        help="Write official Superset dashboard ZIP bundles to the target directory.",
    )
    parser.add_argument(
        "--provision",
        action="store_true",
        help="Provision or update dashboards inside a running Superset app context.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.write_bundles:
        write_dashboard_bundles(args.write_bundles)
    if args.provision:
        provision_dashboards()
    if not args.write_bundles and not args.provision:
        raise SystemExit("Specify at least one action: --write-bundles or --provision.")


if __name__ == "__main__":
    main()
