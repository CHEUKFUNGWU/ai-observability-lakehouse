from scripts.spark_transform_agent_tool_calls import transform_agent_tool_call_events


def test_transform_agent_tool_call_events_casts_expected_types(spark):
    ods_events = spark.createDataFrame(
        [
            {
                "tool_call_id": "call_001",
                "span_id": "span_001",
                "run_id": "run_001",
                "trace_id": "trace_001",
                "agent_id": "agent_support",
                "tool_name": "order_lookup",
                "tool_type": "function",
                "arguments_json": "{\"order_id\":\"A123\"}",
                "result_text": "{\"status\":\"shipped\"}",
                "result_size": 20,
                "duration_ms": 123,
                "status": "success",
                "error_type": "",
                "retry_count": 0,
                "mode": "hermes",
                "region": "unknown",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            }
        ]
    )

    tool_calls = transform_agent_tool_call_events(ods_events)
    schema = dict(tool_calls.dtypes)

    assert schema["result_size"] == "int"
    assert schema["duration_ms"] == "int"
    assert schema["retry_count"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"
