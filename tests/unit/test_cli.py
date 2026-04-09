from click.testing import CliRunner
from delphi.cli import main


def test_version():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "0.5.1" in result.output


def test_setup_help():
    runner = CliRunner()
    result = runner.invoke(main, ["setup", "--help"])
    assert result.exit_code == 0
    assert "--profile" in result.output
    assert "--verify" in result.output


def test_run_help():
    runner = CliRunner()
    result = runner.invoke(main, ["run", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output
    assert "--evidence-rows" in result.output
    assert "--no-evidence" in result.output
    assert "--confidence" in result.output


def test_inspect_help():
    runner = CliRunner()
    result = runner.invoke(main, ["inspect", "--help"])
    assert result.exit_code == 0
