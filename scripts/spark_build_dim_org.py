import argparse
from datetime import datetime, timezone
from pathlib import Path

from app.logging_utils import get_logger, log_info
from app.org_dimension import TEAM_DIMENSIONS, USER_DIMENSIONS
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_spark_session


DEFAULT_TEAM_OUTPUT_PATH = Path("data/warehouse/dim/dim_team_df.parquet")
DEFAULT_USER_OUTPUT_PATH = Path("data/warehouse/dim/dim_user_df.parquet")
LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--team-output", type=Path, default=DEFAULT_TEAM_OUTPUT_PATH)
    parser.add_argument("--user-output", type=Path, default=DEFAULT_USER_OUTPUT_PATH)
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    spark = build_spark_session("ai-observability-dim-org")
    try:
        team_rows = [dimension.__dict__ for dimension in TEAM_DIMENSIONS]
        user_rows = [dimension.__dict__ for dimension in USER_DIMENSIONS]
        team_frame = spark.createDataFrame(team_rows)
        user_frame = spark.createDataFrame(user_rows)

        team_frame.write.mode("overwrite").parquet(str(args.team_output))
        user_frame.write.mode("overwrite").parquet(str(args.user_output))

        team_count = team_frame.count()
        user_count = user_frame.count()
        log_info(
            LOGGER,
            "dim_org_written",
            team_output=str(args.team_output),
            user_output=str(args.user_output),
            teams=team_count,
            users=user_count,
        )
        append_pipeline_run(
            pipeline_name="spark_build_dim_org",
            layer="dim",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=len(team_rows) + len(user_rows),
            output_rows=team_count + user_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
