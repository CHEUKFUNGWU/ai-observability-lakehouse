import pytest

from scripts.spark_utils import build_spark_session


@pytest.fixture
def spark():
    session = build_spark_session("test-ai-observability-lakehouse")
    yield session
    session.stop()
