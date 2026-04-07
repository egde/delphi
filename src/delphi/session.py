"""Spark session builder with Databricks Connect profile resolution."""

from __future__ import annotations

import logging
import time

from delphi.config import ConnectionConfig, DelphiConfig

logger = logging.getLogger(__name__)

NON_RETRYABLE = (PermissionError, KeyError, ValueError)


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


def get_spark_session_with_retry(
    config: DelphiConfig,
    profile: str | None = None,
) -> object:
    """Get Spark session with retry on transient connection errors."""
    retries = config.connection_retries
    for attempt in range(1, retries + 1):
        try:
            return get_spark_session(config, profile)
        except NON_RETRYABLE:
            raise
        except Exception as e:
            if attempt == retries:
                raise
            delay = 2 ** attempt
            logger.warning("Connection attempt %d/%d failed: %s. Retrying in %ds...", attempt, retries, e, delay)
            time.sleep(delay)
