import pytest
from unittest.mock import patch, MagicMock
from delphi.session import resolve_connection_config
from delphi.config import DelphiConfig, ConnectionConfig, load_config


def test_resolve_default_profile():
    cfg = DelphiConfig(
        connection=ConnectionConfig(
            host="https://prod.databricks.com",
            cluster_id="prod-123",
            auth_type="pat",
        )
    )
    conn = resolve_connection_config(cfg, profile=None)
    assert conn.host == "https://prod.databricks.com"
    assert conn.cluster_id == "prod-123"


def test_resolve_named_profile():
    cfg = DelphiConfig(
        connection=ConnectionConfig(host="prod", cluster_id="prod-123", auth_type="env"),
        _profiles={
            "staging": ConnectionConfig(
                host="https://staging.databricks.com",
                cluster_id="stg-456",
                auth_type="env",
            )
        },
    )
    conn = resolve_connection_config(cfg, profile="staging")
    assert conn.host == "https://staging.databricks.com"
    assert conn.cluster_id == "stg-456"


def test_resolve_missing_profile_raises():
    cfg = DelphiConfig()
    with pytest.raises(KeyError, match="nonexistent"):
        resolve_connection_config(cfg, profile="nonexistent")


def test_resolve_serverless_config():
    """Serverless config resolves correctly."""
    cfg = DelphiConfig(
        connection=ConnectionConfig(
            host="https://serverless.databricks.com",
            serverless=True,
            auth_type="pat",
        )
    )
    conn = resolve_connection_config(cfg, profile=None)
    assert conn.host == "https://serverless.databricks.com"
    assert conn.serverless is True
    assert conn.cluster_id == ""


def test_handle_connect_error_version_mismatch(caplog):
    """Version mismatch error logs helpful guidance."""
    from delphi.session import _handle_connect_error
    import logging

    with caplog.at_level(logging.ERROR):
        _handle_connect_error(Exception(
            "Unsupported combination of Databricks Runtime & Databricks Connect versions: "
            "15.4 (Databricks Runtime) < 18.1.2 (Databricks Connect)."
        ))
    assert "databricks-connect" in caplog.text
    assert "pip install" in caplog.text


def test_handle_connect_error_both_set(caplog):
    """Both cluster_id and serverless error logs fix."""
    from delphi.session import _handle_connect_error
    import logging

    with caplog.at_level(logging.ERROR):
        _handle_connect_error(Exception("Can't set both cluster id and serverless."))
    assert "cluster_id" in caplog.text
    assert "serverless" in caplog.text


def test_handle_connect_error_pyspark_conflict(caplog):
    """PySpark conflict error logs fix."""
    from delphi.session import _handle_connect_error
    import logging

    with caplog.at_level(logging.ERROR):
        _handle_connect_error(Exception("pyspark and databricks-connect cannot be installed"))
    assert "pip uninstall" in caplog.text
