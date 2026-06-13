from dataclasses import dataclass

from app.model_pricing import MODEL_PRICING_USD


@dataclass(frozen=True)
class ModelDimension:
    model_name: str
    provider: str
    input_price_per_1m_tokens: float
    output_price_per_1m_tokens: float
    max_context_tokens: int
    release_date: str
    status: str


MODEL_DIMENSIONS: tuple[ModelDimension, ...] = tuple(
    ModelDimension(
        model_name=model_name,
        provider="deepseek",
        input_price_per_1m_tokens=pricing.input_price_per_1m_tokens,
        output_price_per_1m_tokens=pricing.output_price_per_1m_tokens,
        max_context_tokens=128_000,
        release_date="2025-01-01",
        status="active",
    )
    for model_name, pricing in MODEL_PRICING_USD.items()
)
