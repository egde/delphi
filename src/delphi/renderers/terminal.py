"""Rich terminal renderer for test results."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table
from rich.text import Text


def render_terminal(results: list[dict], console: Console | None = None, total_ms: int = 0) -> str:
    """Render test results as a rich terminal table. Returns rendered string."""
    if console is None:
        console = Console(record=True)

    table = Table(title="Delphi Test Results", show_lines=True)
    table.add_column("Status", width=12)
    table.add_column("Test", min_width=20)
    table.add_column("Observed", justify="right")
    table.add_column("Threshold", justify="right")
    table.add_column("CI", justify="right")
    table.add_column("Confidence", justify="right")
    table.add_column("Sample", justify="right")
    table.add_column("Time", justify="right")

    for r in results:
        status = r.get("status", "unknown")
        cr = r.get("confidence_result")

        if status == "pass":
            status_text = Text("PASS", style="bold green")
        elif status == "fail":
            status_text = Text("FAIL", style="bold red")
        elif status == "error":
            status_text = Text("ERROR", style="bold red")
        elif status == "inconclusive":
            status_text = Text("INCONCLUSIVE", style="bold yellow")
        else:
            status_text = Text(status.upper())

        observed = f"{cr.observed:.4f}" if cr else "-"
        threshold = str(r.get("threshold", "-"))
        ci = f"[{cr.ci_lower:.4f}, {cr.ci_upper:.4f}]" if cr else "-"
        confidence = f"{cr.confidence:.0%}" if cr else "-"
        sample = f"{cr.sample_size:,}" if cr else "-"
        duration = f"{r.get('duration_ms', 0)}ms"

        table.add_row(
            status_text, r.get("test_name", "?"),
            observed, threshold, ci, confidence, sample, duration,
        )

    console.print(table)

    for r in results:
        if r.get("error"):
            console.print(f"\n  [red]ERROR:[/red] {r['error']}")
            if r.get("suggestion"):
                console.print(f"  [yellow]->[/yellow] {r['suggestion']}")

    for r in results:
        if r.get("status") == "fail" and r.get("evidence"):
            console.print(f"\n  Evidence for {r.get('test_name', '?')}:")
            ev_table = Table(show_lines=True)
            evidence = r["evidence"]
            if evidence:
                for col_name in evidence[0]:
                    ev_table.add_column(col_name)
                for row in evidence:
                    ev_table.add_row(*[str(v) for v in row.values()])
                console.print(ev_table)

    # Summary footer
    passed = sum(1 for r in results if r.get("status") == "pass")
    failed = sum(1 for r in results if r.get("status") == "fail")
    errors = sum(1 for r in results if r.get("status") in ("error", "inconclusive"))
    total_secs = total_ms / 1000

    parts = []
    if passed:
        parts.append(f"[green]{passed} passed[/green]")
    if failed:
        parts.append(f"[red]{failed} failed[/red]")
    if errors:
        parts.append(f"[red]{errors} errors[/red]")

    summary = ", ".join(parts) + f" in {total_secs:.1f}s"
    console.print(f"\n{summary}")

    return console.export_text()
