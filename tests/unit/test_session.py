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
