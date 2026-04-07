"""Delphi CLI entry point."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from delphi import __version__


@click.group()
@click.version_option(version=__version__, prog_name="delphi")
def main():
    """Delphi — Probabilistic data test framework for Databricks."""
    pass


@main.command()
@click.option("--profile", default=None, help="Named connection profile to configure.")
@click.option("--verify", is_flag=True, help="Verify current connection without re-prompting.")
def setup(profile, verify):
    """Configure Databricks Connect connection."""
    from delphi.setup import run_setup, verify_connection as verify_conn

    if verify:
        verify_conn(profile=profile)
    else:
        run_setup(profile=profile)


@main.command()
@click.argument("path")
@click.option("--output", type=click.Choice(["terminal", "notebook", "ci", "json"]), default=None)
@click.option("--evidence-rows", type=int, default=None)
@click.option("--no-evidence", is_flag=True)
@click.option("--confidence", type=float, default=None)
@click.option("--sample-ceiling", type=int, default=None)
@click.option("--time-column", default=None, help="Explicit time column for stratified sampling.")
@click.option("--profile", default=None, help="Named connection profile.")
def run(path, output, evidence_rows, no_evidence, confidence, sample_ceiling, time_column, profile):
    """Run data tests from Python files or YAML."""
    import importlib.util
    from pathlib import Path as P

    from delphi.config import load_config
    from delphi.session import get_spark_session
    from delphi.runner import run_expectations
    from delphi.renderers.detect import detect_renderer
    from delphi.renderers.terminal import render_terminal
    from delphi.renderers.ci import render_json, render_junit_xml
    from delphi.dsl.yaml_loader import load_yaml_checks

    config = load_config(config_path=P("delphi.toml"))

    if confidence is not None:
        config.default_confidence = confidence
    if sample_ceiling is not None:
        config.sample_ceiling = sample_ceiling
    if evidence_rows is not None:
        config.evidence_rows = evidence_rows
    if no_evidence:
        config.evidence_rows = 0
    if time_column is not None:
        config.time_column = time_column

    spark = get_spark_session(config, profile=profile)

    target = P(path)
    all_results = []

    if target.suffix in (".yaml", ".yml"):
        yaml_str = target.read_text()
        check_set = load_yaml_checks(yaml_str)
        results = run_expectations(spark, check_set.table, check_set.expectations, config, test_name=target.stem)
        all_results.extend(results)
    elif target.is_dir():
        for py_file in sorted(target.rglob("test_*.py")):
            _run_python_tests(py_file, spark, config, all_results)
        for yaml_file in sorted(target.rglob("*.yaml")):
            yaml_str = yaml_file.read_text()
            check_set = load_yaml_checks(yaml_str)
            results = run_expectations(spark, check_set.table, check_set.expectations, config, test_name=yaml_file.stem)
            all_results.extend(results)
    elif target.suffix == ".py":
        _run_python_tests(target, spark, config, all_results)

    renderer = output or detect_renderer()
    result_dicts = [_result_to_dict(r) for r in all_results]

    if renderer == "terminal":
        render_terminal(result_dicts)
    elif renderer == "json":
        click.echo(render_json(result_dicts))
    elif renderer == "ci":
        click.echo(render_json(result_dicts))
        junit = render_junit_xml(result_dicts)
        P("delphi-results.xml").write_text(junit)
        click.echo("JUnit XML written to delphi-results.xml")

    has_failures = any(r.status in ("fail", "error") for r in all_results)
    sys.exit(1 if has_failures else 0)


@main.command()
@click.argument("table")
@click.option("--profile", default=None, help="Named connection profile.")
def inspect(table, profile):
    """Show table profile from Delta pre-scan (no sampling)."""
    from pathlib import Path as P
    from rich.console import Console
    from rich.table import Table as RichTable

    from delphi.config import load_config
    from delphi.session import get_spark_session
    from delphi.engine.prescan import prescan_table
    from delphi.detect.time_column import detect_time_column

    config = load_config(config_path=P("delphi.toml"))
    spark = get_spark_session(config, profile=profile)
    result = prescan_table(spark, table)
    time_col = detect_time_column(result, config.time_column_names)

    con = Console()
    con.print(f"\n[bold]{table}[/bold]")
    con.print(f"  Rows: {result.row_count:,}")
    con.print(f"  Files: {result.num_files:,}")
    con.print(f"  Size: {result.size_bytes / 1_000_000:.1f} MB")
    con.print(f"  Partitions: {', '.join(result.partition_columns) or 'none'}")
    con.print(f"  Clustering: {', '.join(result.clustering_columns) or 'none'}")
    con.print(f"  Detected time column: {time_col or 'none'}")

    col_table = RichTable(title="Columns")
    col_table.add_column("Name")
    col_table.add_column("Type")
    for col_info in result.columns.values():
        col_table.add_row(col_info.name, col_info.dtype)
    con.print(col_table)


def _run_python_tests(py_file, spark, config, all_results):
    """Import a Python file and run any @datatest-decorated functions."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    from delphi.runner import run_expectations
    for name in dir(module):
        obj = getattr(module, name)
        if callable(obj) and hasattr(obj, "_delphi_table"):
            ds = obj()
            results = run_expectations(spark, ds.table, ds.expectations, config, test_name=name)
            all_results.extend(results)


def _result_to_dict(r) -> dict:
    return {
        "test_name": r.test_name,
        "table": r.table,
        "status": r.status,
        "confidence_result": r.confidence_result,
        "threshold": r.threshold,
        "duration_ms": r.duration_ms,
        "evidence": r.evidence,
        "error": r.error,
        "suggestion": r.suggestion,
    }
