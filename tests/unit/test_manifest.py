"""Unit tests for dbt_guard.manifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_guard.exceptions import ManifestNotFoundError, ManifestParseError
from dbt_guard.manifest import ManifestData, load_manifest
from dbt_guard.models import ExposureInfo, ModelColumns

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
FULL_BASE = FIXTURES_DIR / "manifests" / "full" / "base"
FULL_CURRENT = FIXTURES_DIR / "manifests" / "full" / "current"


# ---------------------------------------------------------------------------
# Core manifest loading (models, seeds, child_map)
# ---------------------------------------------------------------------------


class TestLoadManifest:
    def test_loads_valid_manifest(self, base_manifest_dir: Path) -> None:
        data = load_manifest(base_manifest_dir)
        models = data.models
        assert len(models) == 4  # model_a, model_b, model_c, model_d
        assert "model.test_pkg.model_a" in models

    def test_returns_model_columns_type(self, base_manifest_dir: Path) -> None:
        models = load_manifest(base_manifest_dir).models
        model_a = models["model.test_pkg.model_a"]
        assert isinstance(model_a, ModelColumns)
        assert model_a.model_id == "model.test_pkg.model_a"
        assert model_a.model_name == "model_a"

    def test_columns_loaded_correctly(self, base_manifest_dir: Path) -> None:
        models = load_manifest(base_manifest_dir).models
        model_a = models["model.test_pkg.model_a"]
        assert set(model_a.columns.keys()) == {"id", "name", "email", "status"}

    def test_column_names_are_lowercased(self, base_manifest_dir: Path) -> None:
        models = load_manifest(base_manifest_dir).models
        model_a = models["model.test_pkg.model_a"]
        for col_name in model_a.columns:
            assert col_name == col_name.lower()

    def test_column_data_types_preserved(self, base_manifest_dir: Path) -> None:
        models = load_manifest(base_manifest_dir).models
        model_a = models["model.test_pkg.model_a"]
        assert model_a.columns["id"].data_type == "integer"
        assert model_a.columns["email"].data_type == "varchar"

    def test_child_map_excludes_test_nodes(self, base_manifest_dir: Path) -> None:
        child_map = load_manifest(base_manifest_dir).child_map
        children = child_map.get("model.test_pkg.model_a", [])
        assert "model.test_pkg.model_b" in children
        for child in children:
            assert not child.startswith("test."), (
                f"Test node {child!r} should not appear in child_map"
            )

    def test_child_map_structure(self, base_manifest_dir: Path) -> None:
        child_map = load_manifest(base_manifest_dir).child_map
        assert "model.test_pkg.model_b" in child_map.get("model.test_pkg.model_a", [])
        assert "model.test_pkg.model_c" in child_map.get("model.test_pkg.model_b", [])

    def test_independent_model_has_no_children(self, base_manifest_dir: Path) -> None:
        child_map = load_manifest(base_manifest_dir).child_map
        assert child_map.get("model.test_pkg.model_d", []) == []

    def test_missing_manifest_raises_error(self, tmp_path: Path) -> None:
        with pytest.raises(ManifestNotFoundError, match="manifest.json not found"):
            load_manifest(tmp_path)

    def test_invalid_json_raises_error(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text("not valid json", encoding="utf-8")
        with pytest.raises(ManifestParseError, match="Invalid JSON"):
            load_manifest(tmp_path)

    def test_test_nodes_excluded_from_models(self, base_manifest_dir: Path) -> None:
        models = load_manifest(base_manifest_dir).models
        for uid in models:
            assert not uid.startswith("test."), (
                f"Test node {uid!r} should not appear in models dict"
            )

    def test_no_compiled_sql_without_files(self, base_manifest_dir: Path) -> None:
        """Compiled SQL should be None when no compiled/ directory exists."""
        models = load_manifest(base_manifest_dir).models
        for model in models.values():
            assert model.compiled_sql is None
            assert model.has_compiled_sql is False

    def test_empty_columns_dict(self, tmp_path: Path) -> None:
        """Model with no documented columns should have empty columns dict."""
        manifest = {
            "nodes": {
                "model.pkg.empty": {
                    "unique_id": "model.pkg.empty",
                    "name": "empty",
                    "resource_type": "model",
                    "package_name": "pkg",
                    "original_file_path": "models/empty.sql",
                    "compiled_path": None,
                    "compiled_code": "",
                    "depends_on": {"nodes": []},
                    "columns": {},
                    "description": "",
                    "tags": [],
                }
            },
            "sources": {},
            "child_map": {},
            "parent_map": {},
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        models = load_manifest(tmp_path).models
        assert models["model.pkg.empty"].columns == {}

    def test_data_type_none_when_empty_string(self, tmp_path: Path) -> None:
        """Empty string data_type should be normalised to None."""
        manifest = {
            "nodes": {
                "model.pkg.m": {
                    "unique_id": "model.pkg.m",
                    "name": "m",
                    "resource_type": "model",
                    "package_name": "pkg",
                    "original_file_path": "models/m.sql",
                    "compiled_path": None,
                    "compiled_code": "",
                    "depends_on": {"nodes": []},
                    "columns": {"id": {"name": "id", "data_type": ""}},
                    "description": "",
                    "tags": [],
                }
            },
            "sources": {},
            "child_map": {},
            "parent_map": {},
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        models = load_manifest(tmp_path).models
        assert models["model.pkg.m"].columns["id"].data_type is None


# ---------------------------------------------------------------------------
# Sources, exposures, snapshots
# ---------------------------------------------------------------------------


class TestSourceParsing:
    def test_sources_empty_by_default(self) -> None:
        data = load_manifest(FULL_BASE)
        assert data.sources == {}

    def test_sources_loaded_when_flag_set(self) -> None:
        data = load_manifest(FULL_BASE, include_sources=True)
        assert len(data.sources) == 2
        assert "source.test_pkg.raw_db.users" in data.sources
        assert "source.test_pkg.raw_db.orders" in data.sources

    def test_source_has_correct_columns(self) -> None:
        data = load_manifest(FULL_BASE, include_sources=True)
        users = data.sources["source.test_pkg.raw_db.users"]
        assert isinstance(users, ModelColumns)
        assert users.resource_type == "source"
        assert "id" in users.columns
        assert "email" in users.columns
        assert users.columns["id"].data_type == "integer"

    def test_source_no_compiled_sql(self) -> None:
        data = load_manifest(FULL_BASE, include_sources=True)
        for src in data.sources.values():
            assert src.compiled_sql is None
            assert src.has_compiled_sql is False

    def test_source_child_map_edges(self) -> None:
        data = load_manifest(FULL_BASE, include_sources=True)
        assert "source.test_pkg.raw_db.users" in data.child_map
        assert "model.test_pkg.model_a" in data.child_map["source.test_pkg.raw_db.users"]


class TestUndocumentedSources:
    def test_no_warnings_by_default(self) -> None:
        data = load_manifest(FULL_BASE, include_sources=True)
        assert data.undocumented_sources == []

    def test_undocumented_detected_when_flag_set(self) -> None:
        data = load_manifest(
            FULL_BASE,
            include_sources=True,
            warn_undocumented_sources=True,
        )
        assert "source.test_pkg.raw_db.orders" in data.undocumented_sources

    def test_documented_source_not_flagged(self) -> None:
        data = load_manifest(
            FULL_BASE,
            include_sources=True,
            warn_undocumented_sources=True,
        )
        assert "source.test_pkg.raw_db.users" not in data.undocumented_sources


class TestExposureParsing:
    def test_exposures_empty_by_default(self) -> None:
        data = load_manifest(FULL_BASE)
        assert data.exposures == {}

    def test_exposures_loaded_when_flag_set(self) -> None:
        data = load_manifest(FULL_BASE, include_exposures=True)
        assert len(data.exposures) == 2
        assert "exposure.test_pkg.user_dashboard" in data.exposures

    def test_exposure_metadata(self) -> None:
        data = load_manifest(FULL_BASE, include_exposures=True)
        dashboard = data.exposures["exposure.test_pkg.user_dashboard"]
        assert isinstance(dashboard, ExposureInfo)
        assert dashboard.name == "user_dashboard"
        assert dashboard.type == "dashboard"
        assert dashboard.owner_name == "Alice Smith"
        assert dashboard.owner_email == "alice@example.com"
        assert dashboard.url == "https://bi.example.com/dashboards/users"

    def test_exposure_depends_on_nodes(self) -> None:
        data = load_manifest(FULL_BASE, include_exposures=True)
        dashboard = data.exposures["exposure.test_pkg.user_dashboard"]
        assert "model.test_pkg.model_a" in dashboard.depends_on_nodes
        assert "model.test_pkg.model_b" in dashboard.depends_on_nodes


class TestSnapshotParsing:
    def test_snapshots_excluded_by_default(self) -> None:
        data = load_manifest(FULL_BASE)
        assert "snapshot.test_pkg.snap_users" not in data.models

    def test_snapshots_included_when_flag_set(self) -> None:
        data = load_manifest(FULL_BASE, include_snapshots=True)
        assert "snapshot.test_pkg.snap_users" in data.models

    def test_snapshot_has_correct_resource_type(self) -> None:
        data = load_manifest(FULL_BASE, include_snapshots=True)
        snap = data.models["snapshot.test_pkg.snap_users"]
        assert snap.resource_type == "snapshot"

    def test_snapshot_columns(self) -> None:
        data = load_manifest(FULL_BASE, include_snapshots=True)
        snap = data.models["snapshot.test_pkg.snap_users"]
        assert "id" in snap.columns
        assert "dbt_valid_from" in snap.columns


class TestManifestDataReturnType:
    def test_returns_manifest_data(self) -> None:
        data = load_manifest(FULL_BASE)
        assert isinstance(data, ManifestData)

    def test_models_populated(self) -> None:
        data = load_manifest(FULL_BASE)
        assert len(data.models) >= 3  # model_a, model_b, model_c

    def test_model_content(self) -> None:
        """Full fixture models have the same structure as basic ones."""
        data = load_manifest(FULL_BASE)
        model_a = data.models["model.test_pkg.model_a"]
        assert model_a.model_name == "model_a"
        assert "id" in model_a.columns
        assert model_a.columns["id"].data_type == "integer"
