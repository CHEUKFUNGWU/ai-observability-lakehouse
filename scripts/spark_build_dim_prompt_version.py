import argparse
from datetime import datetime, timezone
from pathlib import Path

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from app.prompt_dimension import PROMPT_VERSION_DIMENSIONS
from scripts.spark_utils import build_spark_session


DEFAULT_OUTPUT_PATH = Path("data/warehouse/dim/dim_prompt_version_df.parquet")
LOGGER = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    spark = build_spark_session("ai-observability-dim-prompt-version")
    try:
        rows = [dimension.__dict__ for dimension in PROMPT_VERSION_DIMENSIONS]
        frame = spark.createDataFrame(rows)
        frame.write.mode("overwrite").parquet(str(args.output))
        row_count = frame.count()
        log_info(LOGGER, "dim_prompt_version_written", output=str(args.output), rows=row_count)
        append_pipeline_run(
            pipeline_name="spark_build_dim_prompt_version",
            layer="dim",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=len(rows),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
