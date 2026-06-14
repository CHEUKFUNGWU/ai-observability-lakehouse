import os

import pytest

from scripts.spark_utils import build_paimon_spark_session, build_spark_session


@pytest.fixture
def spark():
    session = build_spark_session("test-ai-observability-lakehouse")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def paimon_spark(tmp_path_factory):
    if not os.environ.get("RUN_PAIMON_TESTS"):
        pytest.skip("set RUN_PAIMON_TESTS=1 to run Spark-Paimon integration tests")

    warehouse = tmp_path_factory.mktemp("paimon-warehouse")
    session = build_paimon_spark_session("test-ai-observability-paimon", warehouse=str(warehouse))
    yield session
    session.stop()
