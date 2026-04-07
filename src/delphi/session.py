"""Spark session builder with Databricks Connect profile resolution."""

from __future__ import annotations

import logging
import os
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

    if not conn.host:
        return DatabricksSession.builder.getOrCreate()

    # Need either cluster_id or serverless
    if not conn.cluster_id and not conn.serverless:
        return DatabricksSession.builder.getOrCreate()

    try:
        if conn.serverless:
            builder = DatabricksSession.builder.remote(
                host=conn.host,
                serverless=True,
            )
        else:
            builder = DatabricksSession.builder.remote(
                host=conn.host,
                cluster_id=conn.cluster_id,
            )

        if conn.auth_type == "pat" and conn.token:
            builder = builder.token(conn.token)
        elif conn.auth_type == "env":
            token = os.environ.get("DATABRICKS_TOKEN", "")
            if token:
                builder = builder.token(token)

        return builder.getOrCreate()

    except Exception as e:
        _handle_connect_error(e)
        raise


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


def _handle_connect_error(error: Exception):
    """Provide actionable guidance for common databricks-connect errors."""
    msg = str(error)

    if "Unsupported combination" in msg and "Databricks Runtime" in msg:
        logger.error(
            "\n"
            "--- Databricks Connect Version Mismatch ---\n"
            "\n"
            "Your databricks-connect version doesn't match the Databricks Runtime (DBR) on your cluster.\n"
            "\n"
            "To fix, install the version that matches your DBR:\n"
            "  pip install 'databricks-connect==<DBR_MAJOR>.<DBR_MINOR>.*'\n"
            "\n"
            "Examples:\n"
            "  DBR 15.4 -> pip install 'databricks-connect==15.4.*'\n"
            "  DBR 16.1 -> pip install 'databricks-connect==16.1.*'\n"
            "  DBR 18.1 -> pip install 'databricks-connect==18.1.*'\n"
            "\n"
            "Find your DBR version: Databricks UI -> Compute -> your cluster -> Runtime Version.\n"
            "For serverless compute, use the latest databricks-connect.\n"
            "\n"
            "See docs/databricks-connect-guide.md for details.\n"
        )

    elif "Can't set both cluster id and serverless" in msg:
        logger.error(
            "\n"
            "Both cluster_id and serverless are set in your config.\n"
            "Use one or the other in delphi.toml:\n"
            "  - For a cluster:    cluster_id = \"your-cluster-id\"\n"
            "  - For serverless:   serverless = true  (remove cluster_id)\n"
        )

    elif "pyspark and databricks-connect cannot be installed" in msg:
        logger.error(
            "\n"
            "pyspark and databricks-connect conflict.\n"
            "Fix:\n"
            "  pip uninstall -y pyspark pyspark-connect pyspark-client databricks-connect\n"
            "  pip install databricks-connect\n"
        )

    elif "Cluster id or serverless are required" in msg:
        logger.error(
            "\n"
            "No compute target specified.\n"
            "Set either cluster_id or serverless=true in delphi.toml,\n"
            "or run 'delphi setup' to configure.\n"
        )
