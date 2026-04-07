"""Shared test fixtures."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Session-scoped Databricks Connect SparkSession."""
    try:
        from delphi.session import get_spark_session
        session = get_spark_session()
        session.sql("SELECT 1").collect()
        return session
    except Exception as e:
        pytest.skip(f"Databricks Connect not available: {e}")


@pytest.fixture(scope="session")
def prices(spark):
    return spark.table("delphi.default.prices")


@pytest.fixture(scope="session")
def security(spark):
    return spark.table("delphi.default.security")


@pytest.fixture(scope="session")
def v_prices(spark):
    return spark.table("delphi.default.v_prices")
