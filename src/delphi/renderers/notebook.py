"""Plotly notebook renderer for inline display in Databricks notebooks."""

from __future__ import annotations

import json


def render_notebook(results: list[dict], total_ms: int = 0) -> str:
    """Render test results as HTML with plotly charts for notebook display.

    Returns an HTML string that can be displayed with displayHTML() in Databricks
    or IPython.display.HTML in Jupyter.
    """
    passed = sum(1 for r in results if r.get("status") == "pass")
    failed = sum(1 for r in results if r.get("status") == "fail")
    errors = sum(1 for r in results if r.get("status") in ("error", "inconclusive"))
    total_secs = total_ms / 1000

    # Build summary table rows
    table_rows = ""
    for r in results:
        status = r.get("status", "unknown")
        cr = r.get("confidence_result")

        color = {"pass": "#2ecc71", "fail": "#e74c3c", "error": "#e74c3c", "inconclusive": "#f39c12"}.get(status, "#95a5a6")
        status_badge = f'<span style="color:{color};font-weight:bold">{status.upper()}</span>'

        observed = f"{cr.observed:.4f}" if cr else "-"
        threshold = r.get("threshold", "-")
        ci = f"[{cr.ci_lower:.4f}, {cr.ci_upper:.4f}]" if cr else "-"
        confidence = f"{cr.confidence:.0%}" if cr else "-"
        sample = f"{cr.sample_size:,}" if cr else "-"
        duration = f"{r.get('duration_ms', 0)}ms"

        table_rows += f"""
        <tr>
            <td>{status_badge}</td>
            <td>{r.get('test_name', '?')}</td>
            <td style="text-align:right">{observed}</td>
            <td style="text-align:right">{threshold}</td>
            <td style="text-align:right">{ci}</td>
            <td style="text-align:right">{confidence}</td>
            <td style="text-align:right">{sample}</td>
            <td style="text-align:right">{duration}</td>
        </tr>"""

    # Build confidence interval chart data
    chart_data = []
    for r in results:
        cr = r.get("confidence_result")
        if cr and r.get("status") in ("pass", "fail"):
            chart_data.append({
                "test": r.get("test_name", "?"),
                "observed": cr.observed,
                "ci_lower": cr.ci_lower,
                "ci_upper": cr.ci_upper,
                "status": r.get("status"),
            })

    chart_json = json.dumps(chart_data)

    # Summary line
    summary_parts = []
    if passed:
        summary_parts.append(f'<span style="color:#2ecc71">{passed} passed</span>')
    if failed:
        summary_parts.append(f'<span style="color:#e74c3c">{failed} failed</span>')
    if errors:
        summary_parts.append(f'<span style="color:#e74c3c">{errors} errors</span>')
    summary = ", ".join(summary_parts) + f" in {total_secs:.1f}s"

    html = f"""
    <html>
    <head>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            .delphi-table {{ border-collapse: collapse; width: 100%; font-family: monospace; font-size: 13px; }}
            .delphi-table th, .delphi-table td {{ border: 1px solid #ddd; padding: 6px 10px; }}
            .delphi-table th {{ background: #f5f5f5; text-align: left; }}
            .delphi-summary {{ font-family: monospace; font-size: 14px; padding: 10px 0; }}
        </style>
    </head>
    <body>
        <h3>Delphi Test Results</h3>
        <table class="delphi-table">
            <tr>
                <th>Status</th><th>Test</th><th>Observed</th><th>Threshold</th>
                <th>CI</th><th>Confidence</th><th>Sample</th><th>Time</th>
            </tr>
            {table_rows}
        </table>

        <div id="delphi-ci-chart" style="width:100%;height:300px;margin-top:20px;"></div>

        <script>
            var data = {chart_json};
            if (data.length > 0) {{
                var traces = [{{
                    y: data.map(d => d.test),
                    x: data.map(d => d.observed),
                    error_x: {{
                        type: 'data',
                        symmetric: false,
                        array: data.map(d => d.ci_upper - d.observed),
                        arrayminus: data.map(d => d.observed - d.ci_lower),
                    }},
                    type: 'scatter',
                    mode: 'markers',
                    marker: {{
                        color: data.map(d => d.status === 'pass' ? '#2ecc71' : '#e74c3c'),
                        size: 10,
                    }},
                    name: 'Observed (with CI)',
                }}];
                var layout = {{
                    title: 'Confidence Intervals',
                    xaxis: {{ title: 'Value' }},
                    margin: {{ l: 200 }},
                    height: Math.max(200, data.length * 40 + 100),
                }};
                Plotly.newPlot('delphi-ci-chart', traces, layout);
            }}
        </script>

        <div class="delphi-summary">{summary}</div>
    </body>
    </html>
    """
    return html
