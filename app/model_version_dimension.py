from dataclasses import dataclass


@dataclass(frozen=True)
class ModelVersionDimension:
    model_name: str
    model_version: str
    provider: str
    deployment_status: str
    first_deployed_at: str
    last_deployed_at: str
    is_current_prod: bool


MODEL_VERSION_DIMENSIONS: tuple[ModelVersionDimension, ...] = (
    ModelVersionDimension(
        model_name="deepseek-chat",
        model_version="v3-compat",
        provider="deepseek",
        deployment_status="deprecated",
        first_deployed_at="2026-01-01T00:00:00+00:00",
        last_deployed_at="2026-04-24T00:00:00+00:00",
        is_current_prod=False,
    ),
    ModelVersionDimension(
        model_name="deepseek-reasoner",
        model_version="v3-compat",
        provider="deepseek",
        deployment_status="deprecated",
        first_deployed_at="2026-01-01T00:00:00+00:00",
        last_deployed_at="2026-04-24T00:00:00+00:00",
        is_current_prod=False,
    ),
    ModelVersionDimension(
        model_name="deepseek-v4-flash",
        model_version="v4-flash-20260424",
        provider="deepseek",
        deployment_status="active",
        first_deployed_at="2026-04-24T00:00:00+00:00",
        last_deployed_at="2026-06-01T00:00:00+00:00",
        is_current_prod=True,
    ),
    ModelVersionDimension(
        model_name="deepseek-v4-pro",
        model_version="v4-pro-20260424",
        provider="deepseek",
        deployment_status="canary",
        first_deployed_at="2026-05-15T00:00:00+00:00",
        last_deployed_at="2026-06-10T00:00:00+00:00",
        is_current_prod=False,
    ),
)
