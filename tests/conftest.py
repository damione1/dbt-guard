"""Shared pytest fixtures for dbt-guard tests."""

from __future__ import annotations

from pathlib import Path

import pytest

# Absolute paths to fixture directories so tests work regardless of cwd
FIXTURES_DIR = Path(__file__).parent / "fixtures"
MANIFESTS_DIR = FIXTURES_DIR / "manifests"
SQL_DIR = FIXTURES_DIR / "sql"


@pytest.fixture()
def base_manifest_dir() -> Path:
    """Directory containing the base manifest.json fixture."""
    return MANIFESTS_DIR / "base"


@pytest.fixture()
def current_manifest_dir() -> Path:
    """Directory containing the current manifest.json fixture."""
    return MANIFESTS_DIR / "current"


@pytest.fixture()
def simple_select_sql() -> str:
    return (SQL_DIR / "simple_select.sql").read_text(encoding="utf-8")


@pytest.fixture()
def cte_chain_sql() -> str:
    return (SQL_DIR / "cte_chain.sql").read_text(encoding="utf-8")


@pytest.fixture()
def star_select_sql() -> str:
    return (SQL_DIR / "star_select.sql").read_text(encoding="utf-8")
