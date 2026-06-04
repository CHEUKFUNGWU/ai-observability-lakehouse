import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark():
    session = (
        SparkSession.builder.appName("test-ai-observability-lakehouse")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
