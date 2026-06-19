from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.warehouse_contract import llm_request_validation_rules

VALIDATION_RULES: list[tuple[str, str, str]] = llm_request_validation_rules()


def validate_llm_events(df: DataFrame) -> DataFrame:
    error_columns = []
    for expression, category, code in VALIDATION_RULES:
        error_columns.append(
            F.when(~F.expr(expression), F.lit(f"{category}:{code}")).otherwise(F.lit(None))
        )

    dq_errors = F.expr(
        "filter(_dq_error_candidates, candidate -> candidate is not null)"
    )
    return (
        df.withColumn("_dq_error_candidates", F.array(*error_columns))
        .withColumn("_dq_errors", dq_errors)
        .drop("_dq_error_candidates")
        .withColumn(
            "_dq_status",
            F.when(F.size(F.col("_dq_errors")) == 0, F.lit("valid")).otherwise(F.lit("quarantine")),
        )
    )


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.col("_dq_status") == "valid").drop("_dq_status", "_dq_errors")
    quarantine = df.filter(F.col("_dq_status") == "quarantine")
    return valid, quarantine
