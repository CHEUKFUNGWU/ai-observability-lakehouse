from datetime import date

from scripts.spark_build_ads_cost_budget import build_cost_budget_daily
from scripts.spark_build_ads_cost_monthly_chargeback import build_cost_monthly_chargeback
from scripts.spark_build_dws_cost_team_daily_metrics import build_cost_team_daily_metrics


def test_build_cost_team_daily_metrics_joins_user_team_and_costs(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "user_id": "user_001",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            },
            {
                "date": date(2026, 1, 1),
                "user_id": "user_002",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "total_tokens": 200,
                "estimated_cost_usd": 0.20,
            },
            {
                "date": date(2026, 1, 1),
                "user_id": "user_missing",
                "app_name": "internal_copilot",
                "model_name": "deepseek-chat",
                "total_tokens": 300,
                "estimated_cost_usd": 0.30,
            },
        ]
    )
    user_dim = spark.createDataFrame(
        [
            {"user_id": "user_001", "team_id": "team_support"},
            {"user_id": "user_002", "team_id": "team_support"},
        ]
    )

    rows = {
        (row["team_id"], row["app_name"]): row
        for row in build_cost_team_daily_metrics(llm_events, user_dim).collect()
    }

    support = rows[("team_support", "ai_support_bot")]
    unknown = rows[("unknown", "internal_copilot")]

    assert support["request_cnt_1d"] == 2
    assert support["total_token_cnt_1d"] == 300
    assert support["estimated_cost_amt_1d"] == 0.30000000000000004
    assert support["agent_run_cnt_1d"] == 0
    assert unknown["request_cnt_1d"] == 1


def test_build_cost_team_daily_metrics_includes_agent_runs(spark):
    llm_events = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "user_id": "user_001",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "total_tokens": 100,
                "estimated_cost_usd": 0.10,
            }
        ]
    )
    agent_runs = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "user_id": "user_001",
                "app_name": "ai_support_bot",
                "estimated_cost_usd": 0.05,
            }
        ]
    )
    user_dim = spark.createDataFrame([{"user_id": "user_001", "team_id": "team_support"}])

    row = build_cost_team_daily_metrics(llm_events, user_dim, agent_runs).collect()[0]

    assert row["agent_run_cnt_1d"] == 1
    assert row["agent_cost_amt_1d"] == 0.05


def test_build_cost_budget_daily_computes_mtd_projection_and_breach(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "team_id": "team_support",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 10,
                "total_token_cnt_1d": 1000,
                "estimated_cost_amt_1d": 10.0,
                "agent_run_cnt_1d": 2,
                "agent_cost_amt_1d": 5.0,
            },
            {
                "date": date(2026, 1, 2),
                "team_id": "team_support",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 10,
                "total_token_cnt_1d": 1000,
                "estimated_cost_amt_1d": 15.0,
                "agent_run_cnt_1d": 2,
                "agent_cost_amt_1d": 5.0,
            },
        ]
    )
    team_dim = spark.createDataFrame(
        [
            {
                "team_id": "team_support",
                "team_name": "Support",
                "department": "Customer Success",
                "cost_center": "CC-1100",
                "budget_monthly_usd": 100.0,
            }
        ]
    )

    rows = {row["date"]: row for row in build_cost_budget_daily(metrics, team_dim).collect()}

    assert rows[date(2026, 1, 1)]["cost_mtd_amt"] == 15.0
    assert rows[date(2026, 1, 2)]["cost_mtd_amt"] == 35.0
    assert rows[date(2026, 1, 2)]["projected_month_end_cost_amt"] == 542.5
    assert rows[date(2026, 1, 2)]["budget_utilization_rate_mtd"] == 0.35
    assert rows[date(2026, 1, 2)]["is_budget_breach"] is True


def test_build_cost_monthly_chargeback_computes_finance_totals(spark):
    metrics = spark.createDataFrame(
        [
            {
                "date": date(2026, 1, 1),
                "team_id": "team_support",
                "app_name": "ai_support_bot",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 10,
                "total_token_cnt_1d": 1000,
                "estimated_cost_amt_1d": 10.0,
                "agent_run_cnt_1d": 2,
                "agent_cost_amt_1d": 5.0,
            },
            {
                "date": date(2026, 1, 1),
                "team_id": "team_support",
                "app_name": "ai_support_bot",
                "model_name": "gpt-4.1-mini",
                "request_cnt_1d": 5,
                "total_token_cnt_1d": 500,
                "estimated_cost_amt_1d": 5.0,
                "agent_run_cnt_1d": 2,
                "agent_cost_amt_1d": 5.0,
            },
            {
                "date": date(2026, 1, 15),
                "team_id": "team_support",
                "app_name": "internal_copilot",
                "model_name": "deepseek-chat",
                "request_cnt_1d": 20,
                "total_token_cnt_1d": 2000,
                "estimated_cost_amt_1d": 30.0,
                "agent_run_cnt_1d": 3,
                "agent_cost_amt_1d": 10.0,
            },
        ]
    )
    team_dim = spark.createDataFrame(
        [
            {
                "team_id": "team_support",
                "team_name": "Support",
                "department": "Customer Success",
                "cost_center": "CC-1100",
                "budget_monthly_usd": 100.0,
            }
        ]
    )

    row = build_cost_monthly_chargeback(metrics, team_dim).collect()[0]

    assert row["month_start_date"] == date(2026, 1, 1)
    assert row["request_cnt_1m"] == 35
    assert row["total_token_cnt_1m"] == 3500
    assert row["llm_cost_amt_1m"] == 45.0
    assert row["agent_run_cnt_1m"] == 5
    assert row["agent_cost_amt_1m"] == 15.0
    assert row["chargeback_amt_1m"] == 60.0
    assert row["budget_variance_amt_1m"] == 40.0
    assert row["budget_utilization_rate_1m"] == 0.6
    assert row["is_budget_overrun"] is False
