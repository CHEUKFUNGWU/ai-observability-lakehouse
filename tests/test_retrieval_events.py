from datetime import date, datetime

import pytest

from scripts.spark_build_ads_retrieval_quality import build_retrieval_quality
from scripts.spark_build_dws_retrieval_daily_metrics import build_retrieval_daily_metrics
from scripts.spark_transform_retrieval_events import transform_retrieval_events


def make_raw_retrieval_events(spark):
    return spark.createDataFrame(
        [
            {
                "retrieval_id": "ret_001",
                "trace_id": "trace_001",
                "run_id": "run_001",
                "span_id": "span_001",
                "request_id": "req_001",
                "agent_id": "agent_001",
                "app_name": "ai_support_bot",
                "feature_name": "rag_answer",
                "user_id": "user_001",
                "knowledge_base_id": "kb_product_docs",
                "knowledge_base_name": "product_docs",
                "embedding_model": "bge-large-en",
                "retrieval_strategy": "hybrid",
                "query_text_hash": "query_hash",
                "query_length": 24,
                "top_k": 5,
                "returned_count": 4,
                "hit_count": 3,
                "max_similarity_score": 0.95,
                "min_similarity_score": 0.70,
                "avg_similarity_score": 0.82,
                "embedding_latency_ms": 100,
                "search_latency_ms": 400,
                "total_latency_ms": 500,
                "status": "success",
                "error_type": "",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:00:00+00:00",
                "date": "2026-01-01",
            },
            {
                "retrieval_id": "ret_002",
                "trace_id": "trace_002",
                "run_id": "run_002",
                "span_id": "span_002",
                "request_id": "req_002",
                "agent_id": "agent_001",
                "app_name": "ai_support_bot",
                "feature_name": "rag_answer",
                "user_id": "user_002",
                "knowledge_base_id": "kb_product_docs",
                "knowledge_base_name": "product_docs",
                "embedding_model": "bge-large-en",
                "retrieval_strategy": "hybrid",
                "query_text_hash": "query_hash_2",
                "query_length": 26,
                "top_k": 5,
                "returned_count": 0,
                "hit_count": 0,
                "max_similarity_score": 0.0,
                "min_similarity_score": 0.0,
                "avg_similarity_score": 0.0,
                "embedding_latency_ms": 120,
                "search_latency_ms": 2300,
                "total_latency_ms": 2420,
                "status": "success",
                "error_type": "",
                "mode": "mock",
                "environment": "dev",
                "created_at": "2026-01-01T00:01:00+00:00",
                "date": "2026-01-01",
            },
        ]
    )


def test_transform_retrieval_events_casts_expected_types(spark):
    events = transform_retrieval_events(make_raw_retrieval_events(spark))
    schema = dict(events.dtypes)

    assert schema["top_k"] == "int"
    assert schema["returned_count"] == "int"
    assert schema["hit_count"] == "int"
    assert schema["avg_similarity_score"] == "double"
    assert schema["total_latency_ms"] == "int"
    assert schema["created_at"] == "timestamp"
    assert schema["date"] == "date"


def test_build_retrieval_daily_metrics_aggregates_quality_and_latency(spark):
    metrics = build_retrieval_daily_metrics(transform_retrieval_events(make_raw_retrieval_events(spark)))
    row = metrics.collect()[0]

    assert row["retrieval_cnt_1d"] == 2
    assert row["success_cnt_1d"] == 2
    assert row["zero_result_cnt_1d"] == 1
    assert row["returned_cnt_1d"] == 4
    assert row["hit_cnt_1d"] == 3
    assert row["avg_total_latency_ms"] == 1460.0
    assert row["p95_total_latency_ms"] == 2420


def test_build_retrieval_quality_derives_rates_and_breaches(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "app_name": "ai_support_bot",
                "knowledge_base_id": "kb_product_docs",
                "embedding_model": "bge-large-en",
                "retrieval_strategy": "hybrid",
                "retrieval_cnt_1d": 10,
                "success_cnt_1d": 10,
                "error_cnt_1d": 0,
                "zero_result_cnt_1d": 2,
                "returned_cnt_1d": 20,
                "hit_cnt_1d": 12,
                "avg_similarity_score": 0.72,
                "avg_total_latency_ms": 1300.0,
                "p95_total_latency_ms": 2400,
                "avg_embedding_latency_ms": 100.0,
                "avg_search_latency_ms": 1200.0,
            }
        ]
    )

    row = build_retrieval_quality(metrics).collect()[0]

    assert row["zero_result_rate_1d"] == 0.2
    assert row["hit_rate_1d"] == 0.6
    assert row["is_latency_breach"] is True
    assert row["is_zero_result_breach"] is True
