from app.dim_model import MODEL_DIMENSIONS
from app.model_pricing import available_model_names, estimate_model_cost_usd


def test_available_model_names_include_reasoner_alias():
    assert "deepseek-reasoner" in available_model_names()


def test_v4_pro_costs_more_than_v4_flash_for_same_usage():
    flash_cost = estimate_model_cost_usd("deepseek-v4-flash", prompt_tokens=1_000_000, completion_tokens=1_000_000)
    pro_cost = estimate_model_cost_usd("deepseek-v4-pro", prompt_tokens=1_000_000, completion_tokens=1_000_000)

    assert pro_cost > flash_cost


def test_dim_model_marks_compatibility_aliases_as_deprecated():
    dims = {dimension.model_name: dimension for dimension in MODEL_DIMENSIONS}

    assert dims["deepseek-chat"].status == "deprecated"
    assert dims["deepseek-reasoner"].status == "deprecated"
    assert dims["deepseek-v4-pro"].status == "active"
