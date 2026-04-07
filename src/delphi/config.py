"""Configuration loader for Delphi."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ConnectionConfig:
    host: str = ""
    cluster_id: str = ""
    auth_type: str = "env"
    token: str = ""
    default_catalog: str = ""
    default_schema: str = ""


@dataclass
class DelphiConfig:
    default_confidence: float = 0.95
    sample_floor: int = 1000
    sample_ceiling: int = 100000
    evidence_rows: int = 10
    redact_columns: list[str] = field(default_factory=list)
    connection_retries: int = 3
    connection_timeout: int = 300
    time_column_names: list[str] = field(
        default_factory=lambda: [
            "timestamp", "created_at", "event_time", "date", "event_date",
        ]
    )
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    _profiles: dict[str, ConnectionConfig] = field(default_factory=dict)

    def get_profile(self, name: str) -> ConnectionConfig:
        if name not in self._profiles:
            raise KeyError(f"Connection profile not found: {name}")
        return self._profiles[name]


def load_config(config_path: Path | None = None) -> DelphiConfig:
    """Load config from a delphi.toml file, falling back to defaults."""
    if config_path is None or not config_path.exists():
        return DelphiConfig()

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    delphi_section = raw.get("delphi", {})
    conn_raw = delphi_section.pop("connection", {})
    profiles_raw = conn_raw.pop("profiles", {})

    cfg = DelphiConfig(
        default_confidence=delphi_section.get("default_confidence", 0.95),
        sample_floor=delphi_section.get("sample_floor", 1000),
        sample_ceiling=delphi_section.get("sample_ceiling", 100000),
        evidence_rows=delphi_section.get("evidence_rows", 10),
        redact_columns=delphi_section.get("redact_columns", []),
        connection_retries=delphi_section.get("connection_retries", 3),
        connection_timeout=delphi_section.get("connection_timeout", 300),
        time_column_names=delphi_section.get(
            "time_column_names",
            ["timestamp", "created_at", "event_time", "date", "event_date"],
        ),
    )

    if conn_raw:
        cfg.connection = ConnectionConfig(
            host=conn_raw.get("host", ""),
            cluster_id=conn_raw.get("cluster_id", ""),
            auth_type=conn_raw.get("auth_type", "env"),
            token=conn_raw.get("token", ""),
            default_catalog=conn_raw.get("default_catalog", ""),
            default_schema=conn_raw.get("default_schema", ""),
        )

    for name, profile_raw in profiles_raw.items():
        cfg._profiles[name] = ConnectionConfig(
            host=profile_raw.get("host", ""),
            cluster_id=profile_raw.get("cluster_id", ""),
            auth_type=profile_raw.get("auth_type", "env"),
            token=profile_raw.get("token", ""),
            default_catalog=profile_raw.get("default_catalog", ""),
            default_schema=profile_raw.get("default_schema", ""),
        )

    return cfg
