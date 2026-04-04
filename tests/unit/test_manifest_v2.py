"""Unit tests for v0.2 manifest loading: sources, exposures, snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_guard.manifest import ManifestData, load_manifest
from dbt_guard.models import ExposureInfo, ModelColumns

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
V2_BASE = FIXTURES_DIR / "manifests" / "v2" / "base"
V2_CURRENT = FIXTURES_DIR / "manifests" / "v2" / "current"


class TestSourceParsing:
    def test_sources_empty_by_default(self) -> None:
        data = load_manifest(V2_BASE)
        assert data.sources == {}

    def test_sources_loaded_when_flag_set(self) -> None:
        data = load_manifest(V2_BASE, include_sources=True)
        assert len(data.sources) == 2
        assert "source.test_pkg.raw_db.users" in data.sources
        assert "source.test_pkg.raw_db.orders" in data.sources

    def test_source_has_correct_columns(self) -> None:
        data = load_manifest(V2_BASE, include_sources=True)
        users = data.sources["source.test_pkg.raw_db.users"]
        assert isinstance(users, ModelColumns)
        assert users.resource_type == "source"
        assert "id" in users.columns
        assert "email" in users.columns
        assert users.columns["id"].data_type == "integer"

    def test_source_no_compiled_sql(self) -> None:
        data = load_manifest(V2_BASE, include_sources=True)
        for src in data.sources.values():
            assert src._compiled_sql is None
            assert src.has_compiled_sql is False

    def test_source_child_map_edges(self) -> None:
        data = load_manifest(V2_BASE, include_sources=True)
        # source -> model edge should be present
        assert "source.test_pkg.raw_db.users" in data.child_map
        assert "model.test_pkg.model_a" in data.child_map["source.test_pkg.raw_db.users"]


class TestUndocumentedSources:
    def test_no_warnings_by_default(self) -> None:
        data = load_manifest(V2_BASE, include_sources=True)
        assert data.undocumented_sources == []

    def test_undocumented_detected_when_flag_set(self) -> None:
        data = load_manifest(
            V2_BASE,
            include_sources=True,
            warn_undocumented_sources=True,
        )
        assert "source.test_pkg.raw_db.orders" in data.undocumented_sources

    def test_documented_source_not_flagged(self) -> None:
        data = load_manifest(
            V2_BASE,
            include_sources=True,
            warn_undocumented_sources=True,
        )
        assert "source.test_pkg.raw_db.users" not in data.undocumented_sources


class TestExposureParsing:
    def test_exposures_empty_by_default(self) -> None:
        data = load_manifest(V2_BASE)
        assert data.exposures == {}

    def test_exposures_loaded_when_flag_set(self) -> None:
        data = load_manifest(V2_BASE, include_exposures=True)
        assert len(data.exposures) == 2
        assert "exposure.test_pkg.user_dashboard" in data.exposures

    def test_exposure_metadata(self) -> None:
        data = load_manifest(V2_BASE, include_exposures=True)
        dashboard = data.exposures["exposure.test_pkg.user_dashboard"]
        assert isinstance(dashboard, ExposureInfo)
        assert dashboard.name == "user_dashboard"
        assert dashboard.type == "dashboard"
        assert dashboard.owner_name == "Alice Smith"
        assert dashboard.owner_email == "alice@example.com"
        assert dashboard.url == "https://bi.example.com/dashboards/users"

    def test_exposure_depends_on_nodes(self) -> None:
        data = load_manifest(V2_BASE, include_exposures=True)
        dashboard = data.exposures["exposure.test_pkg.user_dashboard"]
        assert "model.test_pkg.model_a" in dashboard.depends_on_nodes
        assert "model.test_pkg.model_b" in dashboard.depends_on_nodes


class TestSnapshotParsing:
    def test_snapshots_excluded_by_default(self) -> None:
        data = load_manifest(V2_BASE)
        assert "snapshot.test_pkg.snap_users" not in data.models

    def test_snapshots_included_when_flag_set(self) -> None:
        data = load_manifest(V2_BASE, include_snapshots=True)
        assert "snapshot.test_pkg.snap_users" in data.models

    def test_snapshot_has_correct_resource_type(self) -> None:
        data = load_manifest(V2_BASE, include_snapshots=True)
        snap = data.models["snapshot.test_pkg.snap_users"]
        assert snap.resource_type == "snapshot"

    def test_snapshot_columns(self) -> None:
        data = load_manifest(V2_BASE, include_snapshots=True)
        snap = data.models["snapshot.test_pkg.snap_users"]
        assert "id" in snap.columns
        assert "dbt_valid_from" in snap.columns


class TestManifestDataReturnType:
    def test_returns_manifest_data(self) -> None:
        data = load_manifest(V2_BASE)
        assert isinstance(data, ManifestData)

    def test_models_populated(self) -> None:
        data = load_manifest(V2_BASE)
        assert len(data.models) >= 3  # model_a, model_b, model_c

    def test_backward_compat_model_content(self) -> None:
        """Models loaded from v2 fixtures should have same structure as v1."""
        data = load_manifest(V2_BASE)
        model_a = data.models["model.test_pkg.model_a"]
        assert model_a.model_name == "model_a"
        assert "id" in model_a.columns
        assert model_a.columns["id"].data_type == "integer"
