import os
import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def _reset_spark_state() -> None:
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        active_session.stop()
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None

    active_context = SparkContext._active_spark_context
    if active_context is not None:
        active_context.stop()
    SparkContext._active_spark_context = None
    SparkContext._gateway = None
    SparkContext._jvm = None


def build_paimon_spark_session(app_name: str, warehouse: str | None = None) -> SparkSession:
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    _reset_spark_state()

    gravitino_uri = os.environ.get("GRAVITINO_URI")

    if gravitino_uri:
        return _build_gravitino_paimon_session(app_name, gravitino_uri)

    return _build_direct_paimon_session(app_name, warehouse)


def _build_gravitino_paimon_session(app_name: str, gravitino_uri: str) -> SparkSession:
    gravitino_metalake = os.environ.get("GRAVITINO_METALAKE", "ai_observability")
    paimon_package = os.environ.get(
        "PAIMON_SPARK_PACKAGE", "org.apache.paimon:paimon-spark-3.5:1.2.0"
    )
    gravitino_package = os.environ.get(
        "GRAVITINO_SPARK_PACKAGE",
        "org.apache.gravitino:gravitino-spark-connector-runtime-3.5_2.12:1.2.0",
    )

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", f"{paimon_package},{gravitino_package}")
        .config("spark.jars.excludes", "org.slf4j:slf4j-api")
        .config("spark.sql.gravitino.uri", gravitino_uri)
        .config("spark.sql.gravitino.metalake", gravitino_metalake)
        .config("spark.sql.gravitino.enablePaimonSupport", "true")
        .config(
            "spark.sql.catalog.paimon_lake",
            "org.apache.gravitino.spark.connector.GravitinoCatalog",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def _build_direct_paimon_session(app_name: str, warehouse: str | None) -> SparkSession:
    wh = warehouse or os.environ.get("PAIMON_WAREHOUSE", "data/paimon")
    paimon_package = os.environ.get(
        "PAIMON_SPARK_PACKAGE", "org.apache.paimon:paimon-spark-3.5:1.2.0"
    )

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", paimon_package)
        .config("spark.jars.excludes", "org.slf4j:slf4j-api")
        .config("spark.sql.catalog.paimon_lake", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon_lake.warehouse", wh)
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
