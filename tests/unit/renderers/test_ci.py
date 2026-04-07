import json
import xml.etree.ElementTree as ET
from delphi.renderers.ci import render_json, render_junit_xml
from delphi.confidence.result import ConfidenceResult


def _result(passed=True, status="pass"):
    return {
        "test_name": "test_nulls",
        "table": "catalog.schema.t",
        "status": status,
        "confidence_result": ConfidenceResult(
            observed=0.003, ci_lower=0.001, ci_upper=0.005,
            confidence=0.95, method="wilson", sample_size=50000, passed=passed,
        ),
        "threshold": "< 0.01",
        "duration_ms": 1840,
        "evidence": [],
        "error": None,
        "suggestion": None,
    }


def test_render_json_structure():
    output = render_json([_result()])
    data = json.loads(output)
    assert isinstance(data, list)
    assert data[0]["test"] == "test_nulls"
    assert data[0]["status"] == "pass"
    assert data[0]["confidence"] == 0.95


def test_render_json_error():
    r = _result(status="error")
    r["error"] = "Column not found"
    r["suggestion"] = "Did you mean X?"
    output = render_json([r])
    data = json.loads(output)
    assert data[0]["error_type"] == "error"
    assert "Column not found" in data[0]["message"]


def test_render_junit_xml_valid():
    output = render_junit_xml([_result()])
    root = ET.fromstring(output)
    assert root.tag == "testsuites"
    tc = root.find(".//testcase")
    assert tc is not None
    assert tc.attrib["name"] == "test_nulls"


def test_render_junit_xml_failure():
    output = render_junit_xml([_result(passed=False, status="fail")])
    root = ET.fromstring(output)
    failure = root.find(".//failure")
    assert failure is not None
