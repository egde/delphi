from delphi.dsl.yaml_loader import load_yaml_checks

BASIC_YAML = """
table: catalog.schema.revenue
checks:
  - column: revenue
    null_rate: "< 0.01"
  - column: revenue
    mean: "between 1000 and 5000"
  - column: customer_id
    uniqueness: "> 0.99"
"""

COMPARISON_YAML = """
table: catalog.schema.output
compare_to: catalog.schema.expected
comparisons:
  - column: revenue
    mean_diff: "< 0.05"
  - row_count_ratio: "between 0.99 and 1.01"
"""


def test_load_basic_checks():
    result = load_yaml_checks(BASIC_YAML)
    assert result.table == "catalog.schema.revenue"
    assert len(result.expectations) == 3
    exp = result.expectations[0]
    assert exp.column == "revenue"
    assert exp.metric == "null_rate"
    assert exp.threshold == 0.01
    assert exp.direction == "below"


def test_load_between_check():
    result = load_yaml_checks(BASIC_YAML)
    exp = result.expectations[1]
    assert exp.metric == "mean"
    assert exp.threshold_low == 1000
    assert exp.threshold_high == 5000
    assert exp.direction == "between"


def test_load_comparison_checks():
    result = load_yaml_checks(COMPARISON_YAML)
    assert result.compare_to == "catalog.schema.expected"
    assert len(result.expectations) == 2
    assert result.expectations[0].metric == "mean_diff"


def test_default_confidence():
    result = load_yaml_checks(BASIC_YAML)
    for exp in result.expectations:
        assert exp.confidence == 0.95


def test_explicit_confidence():
    yaml_str = """
table: t
checks:
  - column: x
    null_rate: "< 0.01"
    confidence: 0.99
"""
    result = load_yaml_checks(yaml_str)
    assert result.expectations[0].confidence == 0.99


def test_time_column_in_yaml():
    yaml_str = """
table: t
time_column: updated_at
checks:
  - column: x
    null_rate: "< 0.01"
"""
    result = load_yaml_checks(yaml_str)
    assert result.time_column == "updated_at"


def test_time_column_absent_defaults_none():
    result = load_yaml_checks(BASIC_YAML)
    assert result.time_column is None
