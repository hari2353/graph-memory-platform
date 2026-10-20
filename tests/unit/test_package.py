"""Smoke: the package imports and reports its version."""

import graph_memory


def test_package_imports_and_reports_version() -> None:
    assert graph_memory.__version__
