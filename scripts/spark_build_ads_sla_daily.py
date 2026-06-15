import argparse
from pathlib import Path

import yaml
from pyspark.sql import functions as F

from app.logging_utils import get_logger, log_info
from app.pipeline_metadata import append_pipeline_run
from scripts.spark_utils import build_paimon_spark_session

DEFAULT_RULES_PATH = Path("config/sla_rules.yaml")
DEFAULT_OUTPUT_PATH = Path("data/warehouse/ads/ads_observability_sla_feature_report.parquet")
DEFAULT_INPUT_TABLE = "paimon_lake.dws.dws_ai_llm_feature_request_1d"
LOGGER = get_logger(__name__)


def load_rules(path: Path) -> list[dict]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload.get("rules", [])


def build_sla_daily_report(metrics, rules):
    spark = metrics.sparkSession
    rules_frame = spark.createDataFrame(rules)
    return (
        metrics.join(rules_frame, on="feature_name", how="left")
        .withColumn(
            "error_rate",
            F.when(F.col("request_count") > 0, F.col("error_count") / F.col("request_count")).otherwise(F.lit(0.0)),
        )
        .withColumn("is_latency_breach", F.col("p95_latency_ms") > F.col("p95_latency_ms_max"))
        .withColumn("is_error_breach", F.col("error_rate") > F.col("error_rate_max"))
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-table", type=str, default=DEFAULT_INPUT_TABLE)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    from datetime import datetime, timezone

    started_at = datetime.now(timezone.utc)
    spark = build_paimon_spark_session("ai-observability-ads-sla-daily")
    try:
        metrics = spark.table(args.input_table)
        rules = load_rules(args.rules)
        report = build_sla_daily_report(metrics, rules)
        report.write.mode("overwrite").partitionBy("date").parquet(str(args.output))
        row_count = report.count()
        log_info(LOGGER, "ads_sla_daily_written", output=str(args.output), rows=row_count)
        append_pipeline_run(
            pipeline_name="spark_build_ads_sla_daily",
            layer="ads",
            start_time=started_at,
            end_time=datetime.now(timezone.utc),
            input_rows=metrics.count(),
            output_rows=row_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
