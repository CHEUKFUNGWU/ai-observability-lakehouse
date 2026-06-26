import json

from app.langfuse_trace import normalize_trace_record, normalize_trace_records
from app.llm_event import text_sha256
from scripts.spark_transform_agent_events import transform_agent_run_events, transform_agent_span_events
from scripts.spark_transform_agent_tool_calls import transform_agent_tool_call_events
from scripts.spark_transform_retrieval_events import transform_retrieval_events


def trace_without_run_metadata() -> dict:
    return {
        "id": "trace_without_run_001",
        "name": "support-chat",
        "userId": "user_001",
        "sessionId": "session_001",
        "metadata": {
            "app_name": "ai_support_bot",
            "feature_name": "chat",
            "agent_id": "agent_support",
            "environment": "dev",
            "region": "us",
            "mode": "replay",
        },
        "observations": [
            {
                "id": "span_root_001",
                "traceId": "trace_without_run_001",
                "type": "SPAN",
                "name": "plan",
                "startTime": "2026-06-26T09:00:00.000Z",
                "endTime": "2026-06-26T09:00:00.400Z",
                "input": {"goal": "reset token"},
                "output": {"next": "retrieve policy"},
                "metadata": {"span_order": 1},
            }
        ],
    }


def trace_with_run_metadata() -> dict:
    return {
        "id": "trace_with_run_001",
        "name": "support-rag",
        "userId": "user_002",
        "sessionId": "session_002",
        "input": "How do I reset a workspace token?",
        "output": "Rotate the token in settings.",
        "metadata": {
            "run_id": "run_support_001",
            "task_type": "customer_support",
            "agent_id": "agent_support",
            "agent_name": "support_agent",
            "agent_version": "v2",
            "app_name": "ai_support_bot",
            "feature_name": "rag_answer",
            "conversation_id": "conv_002",
            "channel": "web",
            "environment": "dev",
            "region": "us",
            "mode": "replay",
            "turn_count": 1,
        },
        "observations": [
            {
                "id": "span_root_002",
                "traceId": "trace_with_run_001",
                "type": "SPAN",
                "name": "root",
                "startTime": "2026-06-26T09:01:00.000Z",
                "endTime": "2026-06-26T09:01:00.300Z",
            },
            {
                "id": "span_child_002",
                "traceId": "trace_with_run_001",
                "parentObservationId": "span_root_002",
                "type": "CHAIN",
                "name": "retrieve-and-answer",
                "startTime": "2026-06-26T09:01:00.300Z",
                "endTime": "2026-06-26T09:01:01.300Z",
            },
            {
                "id": "retriever_002",
                "traceId": "trace_with_run_001",
                "parentObservationId": "span_child_002",
                "type": "RETRIEVER",
                "name": "product-doc-search",
                "startTime": "2026-06-26T09:01:00.350Z",
                "endTime": "2026-06-26T09:01:00.850Z",
                "input": "workspace token reset",
                "output": [{"document_id": "doc_1", "score": 0.92}, {"document_id": "doc_2", "score": 0.84}],
                "metadata": {
                    "knowledge_base_id": "kb_product_docs",
                    "knowledge_base_name": "product_docs",
                    "embedding_model": "bge-large-en",
                    "retrieval_strategy": "hybrid",
                    "top_k": 5,
                    "returned_count": 2,
                    "hit_count": 2,
                    "embedding_latency_ms": 80,
                    "search_latency_ms": 420,
                },
            },
            {
                "id": "tool_002",
                "traceId": "trace_with_run_001",
                "parentObservationId": "span_child_002",
                "type": "TOOL",
                "name": "ticket_lookup",
                "startTime": "2026-06-26T09:01:00.900Z",
                "endTime": "2026-06-26T09:01:01.100Z",
                "input": {"ticket_id": "T-123"},
                "output": {"status": "open"},
                "metadata": {"tool_type": "function", "retry_count": 1},
            },
            {
                "id": "generation_002",
                "traceId": "trace_with_run_001",
                "parentObservationId": "span_child_002",
                "type": "GENERATION",
                "name": "final-answer",
                "startTime": "2026-06-26T09:01:01.100Z",
                "endTime": "2026-06-26T09:01:02.000Z",
                "model": "gpt-4o-mini",
                "usageDetails": {"input": 30, "output": 12, "total": 42},
                "costDetails": {"total": 0.0002},
                "metadata": {"provider": "openai"},
            },
        ],
    }


def test_trace_without_run_metadata_preserves_envelope_but_does_not_create_agent_run():
    result = normalize_trace_record(trace_without_run_metadata())

    assert len(result.trace_envelopes) == 1
    assert result.trace_envelopes[0]["trace_id"] == "trace_without_run_001"
    assert result.trace_envelopes[0]["has_run_metadata"] is False
    assert result.agent_runs == []
    assert len(result.agent_spans) == 1
    assert result.agent_spans[0]["trace_id"] == "trace_without_run_001"
    assert result.agent_spans[0]["run_id"] == ""
    assert result.quarantine == []


def test_trace_with_run_metadata_creates_agent_run_with_trace_boundary():
    result = normalize_trace_record(trace_with_run_metadata())

    assert len(result.agent_runs) == 1
    row = result.agent_runs[0]
    assert row["run_id"] == "run_support_001"
    assert row["trace_id"] == "trace_with_run_001"
    assert row["task_type"] == "customer_support"
    assert row["tool_call_count"] == 1
    assert row["retrieval_count"] == 1
    assert row["llm_call_count"] == 1
    assert row["total_tokens"] == 42
    assert row["estimated_cost_usd"] == 0.0002
    assert row["input_text_hash"] == text_sha256("How do I reset a workspace token?")


def test_span_tree_preserves_parent_observation_links():
    result = normalize_trace_record(trace_with_run_metadata())

    assert [row["span_id"] for row in result.agent_spans] == ["span_root_002", "span_child_002"]
    assert result.agent_spans[0]["parent_span_id"] is None
    assert result.agent_spans[1]["parent_span_id"] == "span_root_002"
    assert result.agent_spans[1]["span_type"] == "chain"
    assert result.agent_spans[1]["run_id"] == "run_support_001"


def test_tool_observation_maps_to_agent_tool_call_contract():
    result = normalize_trace_record(trace_with_run_metadata())

    assert len(result.agent_tool_calls) == 1
    row = result.agent_tool_calls[0]
    assert row["tool_call_id"] == "tool_002"
    assert row["span_id"] == "span_child_002"
    assert row["run_id"] == "run_support_001"
    assert row["trace_id"] == "trace_with_run_001"
    assert row["tool_name"] == "ticket_lookup"
    assert row["tool_type"] == "function"
    assert json.loads(row["arguments_json"]) == {"ticket_id": "T-123"}
    assert row["duration_ms"] == 200
    assert row["retry_count"] == 1


def test_retriever_observation_maps_to_retrieval_request_when_metadata_is_sufficient():
    result = normalize_trace_record(trace_with_run_metadata())

    assert len(result.retrieval_requests) == 1
    row = result.retrieval_requests[0]
    assert row["retrieval_id"] == "retriever_002"
    assert row["trace_id"] == "trace_with_run_001"
    assert row["run_id"] == "run_support_001"
    assert row["span_id"] == "span_child_002"
    assert row["knowledge_base_id"] == "kb_product_docs"
    assert row["retrieval_strategy"] == "hybrid"
    assert row["query_text_hash"] == text_sha256("workspace token reset")
    assert row["query_length"] == len("workspace token reset")
    assert row["top_k"] == 5
    assert row["returned_count"] == 2
    assert row["hit_count"] == 2
    assert row["max_similarity_score"] == 0.92
    assert row["min_similarity_score"] == 0.84
    assert row["avg_similarity_score"] == 0.88


def test_ambiguous_observations_enter_quarantine_without_wrong_fact_mapping():
    trace = trace_with_run_metadata()
    trace["observations"] = [
        {
            "id": "unknown_001",
            "traceId": "trace_with_run_001",
            "type": "EVENT",
            "name": "unclear",
            "startTime": "2026-06-26T09:01:00.000Z",
            "endTime": "2026-06-26T09:01:00.100Z",
        },
        {
            "id": "retriever_bad_001",
            "traceId": "trace_with_run_001",
            "type": "RETRIEVER",
            "name": "missing-query-and-kb",
            "startTime": "2026-06-26T09:01:00.000Z",
            "endTime": "2026-06-26T09:01:00.100Z",
            "metadata": {"top_k": 5},
        },
    ]

    result = normalize_trace_record(trace)

    assert result.agent_spans == []
    assert result.agent_tool_calls == []
    assert result.retrieval_requests == []
    assert len(result.quarantine) == 2
    assert any("validity:unsupported_observation_type:event" in row["_dq_errors"] for row in result.quarantine)
    assert any("completeness:missing_query_text" in row["_dq_errors"] for row in result.quarantine)


def test_trace_observation_outputs_are_existing_spark_projection_compatible(spark):
    result = normalize_trace_records([trace_with_run_metadata()])

    runs = transform_agent_run_events(spark.createDataFrame(result.agent_runs))
    spans = transform_agent_span_events(spark.createDataFrame(result.agent_spans))
    tool_calls = transform_agent_tool_call_events(spark.createDataFrame(result.agent_tool_calls))
    retrievals = transform_retrieval_events(spark.createDataFrame(result.retrieval_requests))

    assert runs.count() == 1
    assert spans.count() == 2
    assert tool_calls.count() == 1
    assert retrievals.count() == 1
    assert dict(runs.dtypes)["date"] == "date"
    assert dict(spans.dtypes)["parent_span_id"] == "string"
    assert dict(tool_calls.dtypes)["duration_ms"] == "int"
    assert dict(retrievals.dtypes)["top_k"] == "int"
