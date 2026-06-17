from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class ModelDeploymentEvent:
    deployment_id: str
    model_name: str
    model_version: str
    provider: str
    deployment_action: str
    traffic_percentage: float
    target_environment: str
    deployer_user_id: str
    deploy_reason: str
    status: str
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data
