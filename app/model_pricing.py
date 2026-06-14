from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPricing:
    input_price_per_1m_tokens: float
    output_price_per_1m_tokens: float


MODEL_PRICING_USD: dict[str, ModelPricing] = {
    "deepseek-chat": ModelPricing(
        input_price_per_1m_tokens=0.14,
        output_price_per_1m_tokens=0.28,
    ),
    "deepseek-reasoner": ModelPricing(
        input_price_per_1m_tokens=0.14,
        output_price_per_1m_tokens=0.28,
    ),
    "deepseek-v4-flash": ModelPricing(
        input_price_per_1m_tokens=0.14,
        output_price_per_1m_tokens=0.28,
    ),
    "deepseek-v4-pro": ModelPricing(
        input_price_per_1m_tokens=0.435,
        output_price_per_1m_tokens=0.87,
    ),
}

DEFAULT_MODEL_NAME = "deepseek-v4-flash"


def estimate_model_cost_usd(
    model_name: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> float:
    pricing = MODEL_PRICING_USD.get(model_name, MODEL_PRICING_USD[DEFAULT_MODEL_NAME])

    input_cost = prompt_tokens / 1_000_000 * pricing.input_price_per_1m_tokens
    output_cost = completion_tokens / 1_000_000 * pricing.output_price_per_1m_tokens
    return round(input_cost + output_cost, 8)


def available_model_names() -> tuple[str, ...]:
    return tuple(MODEL_PRICING_USD.keys())
