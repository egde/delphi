"""CI renderers — JSON report and JUnit XML."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET


def render_json(results: list[dict]) -> str:
    """Render results as JSON array."""
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
            "error_type": r.get("status") if r.get("error") else None,
            "message": r.get("error"),
            "suggestion": r.get("suggestion"),
            "evidence": r.get("evidence", []),
        }
        output.append(entry)
    return json.dumps(output, indent=2, default=str)


def render_junit_xml(results: list[dict]) -> str:
    """Render results as JUnit XML."""
    testsuites = ET.Element("testsuites")
    testsuite = ET.SubElement(testsuites, "testsuite", name="delphi", tests=str(len(results)))

    failures = 0
    errors = 0

    for r in results:
        tc = ET.SubElement(testsuite, "testcase",
            name=r.get("test_name", ""),
            classname=r.get("table", ""),
            time=str(r.get("duration_ms", 0) / 1000),
        )

        status = r.get("status", "unknown")
        if status == "fail":
            failures += 1
            cr = r.get("confidence_result")
            msg = f"observed={cr.observed:.4f} CI=[{cr.ci_lower:.4f}, {cr.ci_upper:.4f}]" if cr else ""
            failure = ET.SubElement(tc, "failure", message=msg)
            failure.text = msg
        elif status in ("error", "inconclusive"):
            errors += 1
            error = ET.SubElement(tc, "error",
                message=r.get("error", ""),
                type=status,
            )
            if r.get("suggestion"):
                error.text = r["suggestion"]

    testsuite.set("failures", str(failures))
    testsuite.set("errors", str(errors))

    return ET.tostring(testsuites, encoding="unicode", xml_declaration=True)
