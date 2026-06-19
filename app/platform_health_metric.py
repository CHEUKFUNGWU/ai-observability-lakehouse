from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class PlatformHealthMetric:
    metric_event_id: str
    component: str
    metric_name: str
    metric_value: float
    threshold: float
    created_at: datetime

    @property
    def date(self) -> str:
        return self.created_at.date().isoformat()

    @property
    def is_breach(self) -> bool:
        return self.metric_value > self.threshold

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        data["date"] = self.date
        return data
