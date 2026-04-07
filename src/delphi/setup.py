"""Interactive Databricks Connect setup wizard."""

from __future__ import annotations

import tomllib
from pathlib import Path

import click
from rich.console import Console

console = Console()


def run_setup(profile: str | None = None):
    """Interactive setup wizard for Databricks Connect."""
    console.print("[bold]Delphi Setup[/bold] — Configure Databricks Connect\n")

    host = click.prompt("Databricks workspace URL", type=str)
    if not host.startswith("https://"):
        host = f"https://{host}"

    console.print("\nCompute type:")
    console.print("  1. Classic cluster (requires cluster ID)")
    console.print("  2. Serverless compute (no cluster needed)")
    compute_choice = click.prompt("Choose", type=click.Choice(["1", "2"]), default="2")

    cluster_id = ""
    serverless = False
    if compute_choice == "1":
        cluster_id = click.prompt("Cluster ID", type=str)
    else:
        serverless = True

    auth_choices = {"1": ("pat", "Personal Access Token"), "2": ("oauth", "OAuth"), "3": ("env", "Environment variables")}
    console.print("\nAuthentication method:")
    for k, (_, desc) in auth_choices.items():
        console.print(f"  {k}. {desc}")
    auth_choice = click.prompt("Choose", type=click.Choice(["1", "2", "3"]), default="1")
    auth_type = auth_choices[auth_choice][0]

    token = ""
    if auth_type == "pat":
        token = click.prompt("Personal Access Token", hide_input=True, type=str)

    catalog = click.prompt("Default catalog (optional)", default="", show_default=False)
    schema = click.prompt("Default schema (optional)", default="", show_default=False)

    config_path = Path("delphi.toml")
    _write_config(config_path, host, cluster_id, serverless, auth_type, token, catalog, schema, profile)
    _ensure_gitignore(config_path)

    console.print("\n[bold]Verifying connection...[/bold]")
    try:
        _verify(host, cluster_id, serverless, auth_type, token)
        console.print("[green]Connection successful![/green]")
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        console.print("[yellow]Config saved — fix credentials and run 'delphi setup --verify'[/yellow]")


def verify_connection(profile: str | None = None):
    """Re-verify the current connection."""
    from delphi.config import load_config
    config = load_config(config_path=Path("delphi.toml"))
    conn = config.get_profile(profile) if profile else config.connection

    console.print(f"Verifying connection to [bold]{conn.host}[/bold]...")
    try:
        _verify(conn.host, conn.cluster_id, conn.serverless, conn.auth_type, conn.token)
        console.print("[green]Connection successful![/green]")
    except Exception as e:
        console.print(f"[red]Connection failed:[/red] {e}")


def _verify(host: str, cluster_id: str, serverless: bool, auth_type: str, token: str):
    from databricks.connect import DatabricksSession
    if serverless:
        builder = DatabricksSession.builder.remote(host=host, serverless=True)
    else:
        builder = DatabricksSession.builder.remote(host=host, cluster_id=cluster_id)
    if auth_type == "pat" and token:
        builder = builder.token(token)
    spark = builder.getOrCreate()
    spark.sql("SELECT 1").collect()


def _write_config(path, host, cluster_id, serverless, auth_type, token, catalog, schema, profile):
    existing = {}
    if path.exists():
        with open(path, "rb") as f:
            existing = tomllib.load(f)

    delphi = existing.setdefault("delphi", {})
    if profile:
        conn = delphi.setdefault("connection", {}).setdefault("profiles", {}).setdefault(profile, {})
    else:
        conn = delphi.setdefault("connection", {})

    conn["host"] = host
    if serverless:
        conn["serverless"] = True
        conn.pop("cluster_id", None)
    else:
        conn["cluster_id"] = cluster_id
        conn.pop("serverless", None)
    conn["auth_type"] = auth_type
    if auth_type == "pat" and token:
        conn["token"] = token
    if catalog:
        conn["default_catalog"] = catalog
    if schema:
        conn["default_schema"] = schema

    _write_toml(path, existing)
    console.print(f"\nConfig written to [bold]{path}[/bold]")
    if auth_type == "pat":
        console.print("[yellow]Warning: Token stored in plaintext. Use 'env' auth for shared machines.[/yellow]")


def _write_toml(path, data):
    lines = []
    _write_toml_section(lines, data, [])
    path.write_text("\n".join(lines) + "\n")


def _write_toml_section(lines, data, prefix):
    for key, value in data.items():
        if not isinstance(value, dict):
            if isinstance(value, str):
                lines.append(f'{key} = "{value}"')
            elif isinstance(value, bool):
                lines.append(f"{key} = {'true' if value else 'false'}")
            elif isinstance(value, list):
                items = ", ".join(f'"{v}"' if isinstance(v, str) else str(v) for v in value)
                lines.append(f"{key} = [{items}]")
            else:
                lines.append(f"{key} = {value}")
    for key, value in data.items():
        if isinstance(value, dict):
            section = prefix + [key]
            lines.append(f"\n[{'.'.join(section)}]")
            _write_toml_section(lines, value, section)


def _ensure_gitignore(config_path):
    gitignore = Path(".gitignore")
    name = str(config_path)
    if gitignore.exists():
        content = gitignore.read_text()
        if name not in content:
            with open(gitignore, "a") as f:
                f.write(f"\n{name}\n")
