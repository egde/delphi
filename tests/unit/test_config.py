import pytest
from pathlib import Path
from delphi.config import load_config, DelphiConfig


def test_default_config():
    cfg = load_config(config_path=None)
    assert cfg.default_confidence == 0.95
    assert cfg.sample_floor == 1000
    assert cfg.sample_ceiling == 100000
    assert cfg.evidence_rows == 10
    assert cfg.redact_columns == []
    assert cfg.connection_retries == 3
    assert cfg.connection_timeout == 300


def test_load_from_toml(tmp_path):
    toml_file = tmp_path / "delphi.toml"
    toml_file.write_text(
        "[delphi]\n"
        "default_confidence = 0.99\n"
        "sample_floor = 5000\n"
        "sample_ceiling = 50000\n"
        "evidence_rows = 20\n"
        'redact_columns = ["ssn"]\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.default_confidence == 0.99
    assert cfg.sample_floor == 5000
    assert cfg.sample_ceiling == 50000
    assert cfg.evidence_rows == 20
    assert cfg.redact_columns == ["ssn"]


def test_load_connection_profile(tmp_path):
    toml_file = tmp_path / "delphi.toml"
    toml_file.write_text(
        "[delphi.connection]\n"
        'host = "https://example.cloud.databricks.com"\n'
        'cluster_id = "0123-456789-abc"\n'
        'auth_type = "pat"\n'
        'default_catalog = "main"\n'
        'default_schema = "default"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.connection.host == "https://example.cloud.databricks.com"
    assert cfg.connection.cluster_id == "0123-456789-abc"
    assert cfg.connection.auth_type == "pat"


def test_load_named_profile(tmp_path):
    toml_file = tmp_path / "delphi.toml"
    toml_file.write_text(
        "[delphi.connection]\n"
        'host = "https://prod.cloud.databricks.com"\n'
        'cluster_id = "prod-cluster"\n'
        'auth_type = "env"\n'
        "\n"
        "[delphi.connection.profiles.staging]\n"
        'host = "https://staging.cloud.databricks.com"\n'
        'cluster_id = "staging-cluster"\n'
        'auth_type = "env"\n'
    )
    cfg = load_config(config_path=toml_file)
    staging = cfg.get_profile("staging")
    assert staging.host == "https://staging.cloud.databricks.com"
    assert staging.cluster_id == "staging-cluster"


def test_missing_profile_raises(tmp_path):
    toml_file = tmp_path / "delphi.toml"
    toml_file.write_text("[delphi.connection]\nhost = 'x'\ncluster_id = 'y'\nauth_type = 'env'\n")
    cfg = load_config(config_path=toml_file)
    with pytest.raises(KeyError, match="staging"):
        cfg.get_profile("staging")
