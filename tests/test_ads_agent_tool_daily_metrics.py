from datetime import date

from scripts.spark_build_ads_agent_tool_daily_metrics import build_agent_tool_daily_metrics


def test_build_agent_tool_daily_metrics_aggregates_by_agent_and_tool(spark):
    tool_calls = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "agent_id": "agent_support",
                "tool_name": "order_lookup",
                "tool_type": "function",
                "status": "success",
                "retry_count": 0,
                "duration_ms": 100,
                "result_size": 200,
            },
            {
                "date": date(2026, 1, 1),
                "agent_id": "agent_support",
                "tool_name": "order_lookup",
                "tool_type": "function",
                "status": "error",
                "retry_count": 1,
                "duration_ms": 300,
                "result_size": 50,
            },
            {
                "date": date(2026, 1, 1),
                "agent_id": "agent_support",
                "tool_name": "crm_search",
                "tool_type": "function",
                "status": "success",
                "retry_count": 0,
                "duration_ms": 80,
                "result_size": 120,
            },
        ]
    )

    rows = {
        row["tool_name"]: row.asDict()
        for row in build_agent_tool_daily_metrics(tool_calls).collect()
    }

    order_lookup = rows["order_lookup"]
    assert order_lookup["tool_call_count"] == 2
    assert order_lookup["success_count"] == 1
    assert order_lookup["error_count"] == 1
    assert order_lookup["retry_count"] == 1
    assert order_lookup["avg_duration_ms"] == 200.0
    assert order_lookup["p95_duration_ms"] == 300
    assert order_lookup["avg_result_size"] == 125.0
    assert order_lookup["max_result_size"] == 200
    assert "success_rate" not in order_lookup
    assert "error_rate" not in order_lookup

    crm_search = rows["crm_search"]
    assert crm_search["tool_call_count"] == 1
    assert crm_search["success_count"] == 1
    assert crm_search["error_count"] == 0
