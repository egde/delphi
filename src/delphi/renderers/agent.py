"""Structured dict renderer for programmatic/agentic consumption."""

from __future__ import annotations


def render_agent(results: list[dict]) -> list[dict]:
    """Convert results to clean structured dicts for programmatic use."""
    output = []
    for r in results:
        cr = r.get("confidence_result")
        entry = {
            "test": r.get("test_name", ""),
            "table": r.get("table", ""),
            "status": r.get("status", "unknown"),
            "confidence": cr.confidence if cr else None,
            "observed": cr.observed if cr else None,
            "threshold": r.get("threshold"),
            "sample_size": cr.sample_size if cr else None,
            "method": cr.method if cr else None,
            "ci_lower": cr.ci_lower if cr else None,
            "ci_upper": cr.ci_upper if cr else None,
            "duration_ms": r.get("duration_ms", 0),
            "error": r.get("error"),
            "suggestion": r.get("suggestion"),
            "evidence": r.get("evidence", []),
        }
        output.append(entry)
    return output
