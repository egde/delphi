"""Spark session builder with Databricks Connect profile resolution."""

from __future__ import annotations

from delphi.config import ConnectionConfig, DelphiConfig


def resolve_connection_config(
    config: DelphiConfig, profile: str | None = None
) -> ConnectionConfig:
    if profile is not None:
        return config.get_profile(profile)
    return config.connection


def get_spark_session(
    config: DelphiConfig | None = None,
    profile: str | None = None,
):
    from databricks.connect import DatabricksSession

    if config is None:
        return DatabricksSession.builder.getOrCreate()

    conn = resolve_connection_config(config, profile)

    if not conn.host or not conn.cluster_id:
        return DatabricksSession.builder.getOrCreate()

    builder = DatabricksSession.builder.remote(
        host=conn.host,
        cluster_id=conn.cluster_id,
    )

    if conn.auth_type == "pat" and conn.token:
        builder = builder.token(conn.token)

    return builder.getOrCreate()
