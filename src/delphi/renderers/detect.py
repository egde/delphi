"""Auto-detect runtime environment for renderer selection."""

from __future__ import annotations

import os


def detect_renderer() -> str:
    """Detect which renderer to use based on environment."""
    try:
        from dbruntime import dbutils  # noqa: F401
        return "notebook"
    except ImportError:
        pass

    ci_vars = ["CI", "GITHUB_ACTIONS", "JENKINS_URL", "GITLAB_CI", "CIRCLECI"]
    if any(os.environ.get(v) for v in ci_vars):
        return "ci"

    return "terminal"
