from dataclasses import dataclass


@dataclass(frozen=True)
class TeamDimension:
    team_id: str
    team_name: str
    department: str
    cost_center: str
    budget_monthly_usd: float
    manager: str
    status: str


@dataclass(frozen=True)
class UserDimension:
    user_id: str
    user_name: str
    team_id: str
    role: str
    ai_access_tier: str
    status: str


TEAM_DIMENSIONS: tuple[TeamDimension, ...] = (
    TeamDimension(
        team_id="team_support",
        team_name="Customer Support AI",
        department="Customer Success",
        cost_center="CC-1100",
        budget_monthly_usd=1200.0,
        manager="support_ai_manager",
        status="active",
    ),
    TeamDimension(
        team_id="team_sales",
        team_name="Sales Productivity",
        department="Revenue",
        cost_center="CC-2200",
        budget_monthly_usd=900.0,
        manager="sales_ops_manager",
        status="active",
    ),
    TeamDimension(
        team_id="team_platform",
        team_name="Internal Platform",
        department="Engineering",
        cost_center="CC-3300",
        budget_monthly_usd=1800.0,
        manager="platform_manager",
        status="active",
    ),
)


USER_DIMENSIONS: tuple[UserDimension, ...] = tuple(
    UserDimension(
        user_id=f"user_{index:04d}",
        user_name=f"user_{index:04d}",
        team_id=team_id,
        role=role,
        ai_access_tier=access_tier,
        status="active",
    )
    for index, team_id, role, access_tier in [
        (1, "team_support", "support_agent", "power"),
        (2, "team_support", "support_agent", "basic"),
        (3, "team_sales", "account_executive", "power"),
        (4, "team_sales", "sales_ops", "admin"),
        (5, "team_platform", "engineer", "admin"),
        (6, "team_platform", "product_manager", "power"),
    ]
)
