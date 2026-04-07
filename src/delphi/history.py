"""Run history persistence and drift detection."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


def save_run(results: list, history_path: str = ".delphi/history.jsonl") -> str:
    """Append test results to history file. Returns run_id."""
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now(timezone.utc).isoformat()

    with open(path, "a") as f:
        for r in results:
            cr = r.confidence_result
            entry = {
                "run_id": run_id,
                "timestamp": timestamp,
                "test_name": r.test_name,
                "table": r.table,
                "status": r.status,
                "observed": cr.observed if cr else None,
                "ci_lower": cr.ci_lower if cr else None,
                "ci_upper": cr.ci_upper if cr else None,
                "confidence": cr.confidence if cr else None,
                "method": cr.method if cr else None,
                "sample_size": cr.sample_size if cr else None,
                "threshold": r.threshold,
                "duration_ms": r.duration_ms,
            }
            f.write(json.dumps(entry) + "\n")

    return run_id


def load_baseline(
    test_name: str, table: str, history_path: str = ".delphi/history.jsonl"
) -> dict | None:
    """Load the most recent passing result for a test.

    Scans the history file in reverse order (most recent first).
    """
    path = Path(history_path)
    if not path.exists():
        return None

    lines = path.read_text().strip().split("\n")
    for line in reversed(lines):
        if not line.strip():
            continue
        entry = json.loads(line)
        if (
            entry.get("test_name") == test_name
            and entry.get("table") == table
            and entry.get("status") == "pass"
        ):
            return entry

    return None


def load_history(
    test_name: str, table: str, history_path: str = ".delphi/history.jsonl", limit: int = 20
) -> list[dict]:
    """Load recent history entries for a test."""
    path = Path(history_path)
    if not path.exists():
        return []

    entries = []
    for line in path.read_text().strip().split("\n"):
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("test_name") == test_name and entry.get("table") == table:
            entries.append(entry)

    return entries[-limit:]


def detect_drift(
    current_observed: float,
    baseline: dict,
    threshold: float = 2.0,
) -> dict | None:
    """Detect drift between current result and baseline.

    Returns drift info if the shift exceeds `threshold` standard errors,
    or None if no significant drift.

    A rough standard error is estimated from the baseline CI width.
    """
    bl_observed = baseline.get("observed")
    bl_lower = baseline.get("ci_lower")
    bl_upper = baseline.get("ci_upper")

    if bl_observed is None or bl_lower is None or bl_upper is None:
        return None

    # Estimate SE from CI width (95% CI ~ 2*1.96*SE)
    ci_width = bl_upper - bl_lower
    if ci_width <= 0:
        return None

    se = ci_width / (2 * 1.96)
    shift = current_observed - bl_observed
    magnitude = abs(shift) / se if se > 0 else 0

    if magnitude < threshold:
        return None

    return {
        "direction": "up" if shift > 0 else "down",
        "baseline_observed": bl_observed,
        "current_observed": current_observed,
        "shift_magnitude": round(magnitude, 2),
        "shift_value": round(shift, 6),
    }
