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


MODEL_METADATA: dict[str, dict[str, str | int]] = {
    "deepseek-chat": {
        "max_context_tokens": 1_000_000,
        "release_date": "2026-04-24",
        "status": "deprecated",
    },
    "deepseek-reasoner": {
        "max_context_tokens": 1_000_000,
        "release_date": "2026-04-24",
        "status": "deprecated",
    },
    "deepseek-v4-flash": {
        "max_context_tokens": 1_000_000,
        "release_date": "2026-04-24",
        "status": "active",
    },
    "deepseek-v4-pro": {
        "max_context_tokens": 1_000_000,
        "release_date": "2026-04-24",
        "status": "active",
    },
}


MODEL_DIMENSIONS: tuple[ModelDimension, ...] = tuple(
    ModelDimension(
        model_name=model_name,
        provider="deepseek",
        input_price_per_1m_tokens=pricing.input_price_per_1m_tokens,
        output_price_per_1m_tokens=pricing.output_price_per_1m_tokens,
        max_context_tokens=int(MODEL_METADATA[model_name]["max_context_tokens"]),
        release_date=str(MODEL_METADATA[model_name]["release_date"]),
        status=str(MODEL_METADATA[model_name]["status"]),
    )
    for model_name, pricing in MODEL_PRICING_USD.items()
)
