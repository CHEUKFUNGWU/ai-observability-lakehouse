from dataclasses import dataclass


@dataclass(frozen=True)
class PromptVersionDimension:
    prompt_id: str
    prompt_version: str
    prompt_name: str
    owner_team_id: str
    status: str
    release_date: str
    ab_test_group: str
    description: str


PROMPT_VERSION_DIMENSIONS: tuple[PromptVersionDimension, ...] = (
    PromptVersionDimension(
        prompt_id="prompt_001",
        prompt_version="v1",
        prompt_name="Support Chat Baseline",
        owner_team_id="team_support",
        status="deprecated",
        release_date="2026-01-01",
        ab_test_group="control",
        description="Initial support assistant prompt.",
    ),
    PromptVersionDimension(
        prompt_id="prompt_001",
        prompt_version="v2",
        prompt_name="Support Chat Grounded",
        owner_team_id="team_support",
        status="active",
        release_date="2026-02-15",
        ab_test_group="treatment",
        description="Adds retrieval-grounding and escalation instructions.",
    ),
    PromptVersionDimension(
        prompt_id="prompt_002",
        prompt_version="v1",
        prompt_name="Summary Baseline",
        owner_team_id="team_platform",
        status="active",
        release_date="2026-01-20",
        ab_test_group="control",
        description="General summarization prompt for internal workflows.",
    ),
)
