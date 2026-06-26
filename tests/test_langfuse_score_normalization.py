from app.langfuse_score import normalize_score_record, normalize_score_records
from app.langfuse_trace import normalize_trace_record
from app.llm_event import text_sha256
from scripts.spark_transform_evaluation_events import transform_evaluation_events
from scripts.spark_transform_feedback_events import transform_feedback_events


def score_context() -> dict:
    return {
        "traceId": "trace_score_001",
        "observationId": "req_score_001",
        "userId": "user_001",
        "sessionId": "session_001",
        "timestamp": "2026-06-26T10:00:00.000Z",
        "metadata": {
            "run_id": "run_score_001",
            "app_name": "ai_support_bot",
            "feature_name": "rag_answer",
            "agent_id": "agent_support",
            "environment": "dev",
            "model_name": "gpt-4o-mini",
            "prompt_version": "v3",
            "response_latency_ms": 900,
            "mode": "live",
        },
    }


def test_user_feedback_score_maps_to_feedback_action_contract():
    score = {
        **score_context(),
        "id": "score_user_001",
        "source": "USER",
        "name": "thumbs_up",
        "value": 1,
        "comment": "answer solved my issue",
    }

    result = normalize_score_record(score)

    assert result.quarantine == []
    assert result.evaluation_judgments == []
    assert len(result.feedback_actions) == 1
    row = result.feedback_actions[0]
    assert row["feedback_id"] == "score_user_001"
    assert row["trace_id"] == "trace_score_001"
    assert row["request_id"] == "req_score_001"
    assert row["run_id"] == "run_score_001"
    assert row["feedback_type"] == "thumbs_up"
    assert row["rating_value"] is None
    assert row["feedback_text_hash"] == text_sha256("answer solved my issue")
    assert row["feedback_text_length"] == len("answer solved my issue")


def test_evaluator_score_maps_to_evaluation_judgment_contract():
    score = {
        **score_context(),
        "id": "score_eval_001",
        "source": "EVALUATOR",
        "name": "faithfulness",
        "value": 0.92,
        "config": {
            "evaluatorType": "judge",
            "evaluator_model": "gpt-4o",
            "passThreshold": 0.8,
            "evaluation_latency_ms": 1200,
            "mode": "offline",
        },
    }

    result = normalize_score_record(score)

    assert result.quarantine == []
    assert result.feedback_actions == []
    assert len(result.evaluation_judgments) == 1
    row = result.evaluation_judgments[0]
    assert row["evaluation_id"] == "score_eval_001"
    assert row["trace_id"] == "trace_score_001"
    assert row["request_id"] == "req_score_001"
    assert row["evaluator_type"] == "llm_judge"
    assert row["evaluator_model"] == "gpt-4o"
    assert row["evaluation_dimension"] == "faithfulness"
    assert row["score"] == 0.92
    assert row["pass_threshold"] == 0.8
    assert row["passed"] is True


def test_dataset_run_score_maps_to_ground_truth_evaluation_judgment():
    score = {
        **score_context(),
        "id": "score_dataset_001",
        "source": "DATASET_RUN",
        "name": "relevance",
        "value": 0.61,
        "config": {"datasetRunId": "dataset_run_001", "passThreshold": 0.7},
    }

    result = normalize_score_record(score)

    assert result.quarantine == []
    assert len(result.evaluation_judgments) == 1
    row = result.evaluation_judgments[0]
    assert row["evaluator_type"] == "ground_truth"
    assert row["evaluation_dimension"] == "relevance"
    assert row["score"] == 0.61
    assert row["passed"] is False


def test_unknown_score_enters_quarantine_without_fact_mapping():
    score = {**score_context(), "id": "score_unknown_001", "source": "API", "name": "quality", "value": 0.7}

    result = normalize_score_record(score)

    assert result.feedback_actions == []
    assert result.evaluation_judgments == []
    assert len(result.quarantine) == 1
    assert "validity:unknown_score_classification" in result.quarantine[0]["_dq_errors"]


def test_missing_target_score_enters_quarantine():
    score = {
        "id": "score_missing_target_001",
        "source": "EVALUATOR",
        "name": "faithfulness",
        "value": 0.8,
        "timestamp": "2026-06-26T10:00:00.000Z",
    }

    result = normalize_score_record(score)

    assert result.evaluation_judgments == []
    assert len(result.quarantine) == 1
    assert "completeness:missing_score_target" in result.quarantine[0]["_dq_errors"]


def test_invalid_score_range_enters_quarantine():
    score = {
        **score_context(),
        "id": "score_invalid_range_001",
        "source": "AUTOMATED",
        "name": "toxicity",
        "value": 1.2,
    }

    result = normalize_score_record(score)

    assert result.evaluation_judgments == []
    assert len(result.quarantine) == 1
    assert "validity:invalid_score_range" in result.quarantine[0]["_dq_errors"]


def test_conflicting_score_classification_enters_quarantine():
    score = {
        **score_context(),
        "id": "score_conflict_001",
        "source": "USER",
        "name": "faithfulness",
        "value": 0.9,
        "config": {"evaluatorType": "judge"},
    }

    result = normalize_score_record(score)

    assert result.feedback_actions == []
    assert result.evaluation_judgments == []
    assert len(result.quarantine) == 1
    assert "consistency:conflicting_score_classification" in result.quarantine[0]["_dq_errors"]


def test_score_normalization_outputs_are_existing_spark_projection_compatible(spark):
    feedback_score = {
        **score_context(),
        "id": "score_rating_001",
        "source": "MANUAL",
        "name": "rating",
        "value": 4,
    }
    evaluation_score = {
        **score_context(),
        "id": "score_eval_002",
        "source": "JUDGE",
        "name": "coherence",
        "value": 0.88,
        "config": {"passThreshold": 0.7, "evaluation_latency_ms": 1000},
    }
    result = normalize_score_records([feedback_score, evaluation_score])

    feedback = transform_feedback_events(spark.createDataFrame(result.feedback_actions))
    evaluation = transform_evaluation_events(spark.createDataFrame(result.evaluation_judgments))

    assert feedback.count() == 1
    assert evaluation.count() == 1
    assert dict(feedback.dtypes)["rating_value"] == "int"
    assert dict(evaluation.dtypes)["score"] == "double"
    assert dict(evaluation.dtypes)["passed"] == "boolean"


def test_trace_embedded_scores_are_split_with_trace_context():
    trace = {
        "id": "trace_embedded_scores_001",
        "name": "support-rag",
        "userId": "user_002",
        "sessionId": "session_002",
        "timestamp": "2026-06-26T11:00:00.000Z",
        "metadata": {
            "run_id": "run_embedded_001",
            "task_type": "customer_support",
            "agent_id": "agent_support",
            "agent_name": "support_agent",
            "app_name": "ai_support_bot",
            "feature_name": "rag_answer",
            "environment": "dev",
            "region": "us",
            "mode": "live",
        },
        "observations": [],
        "scores": [
            {
                "id": "score_embedded_feedback_001",
                "observationId": "req_embedded_001",
                "source": "USER",
                "name": "thumbs_down",
                "value": 0,
                "timestamp": "2026-06-26T11:00:00.000Z",
            },
            {
                "id": "score_embedded_eval_001",
                "observationId": "req_embedded_001",
                "source": "AUTOMATED",
                "name": "relevance",
                "value": 0.73,
                "timestamp": "2026-06-26T11:00:01.000Z",
            },
        ],
    }

    result = normalize_trace_record(trace)

    assert len(result.feedback_actions) == 1
    assert len(result.evaluation_judgments) == 1
    assert result.feedback_actions[0]["trace_id"] == "trace_embedded_scores_001"
    assert result.evaluation_judgments[0]["run_id"] == "run_embedded_001"
