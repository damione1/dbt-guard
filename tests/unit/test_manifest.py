"""Unit tests for dbt_guard.manifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_guard.exceptions import ManifestNotFoundError, ManifestParseError
from dbt_guard.manifest import load_manifest
from dbt_guard.models import ColumnInfo, ModelColumns


class TestLoadManifest:
    def test_loads_valid_manifest(self, base_manifest_dir: Path) -> None:
        data = load_manifest(base_manifest_dir)
        models, child_map = data.models, data.child_map
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
        # Should have id, name, email, status
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
        # model_a's child_map should contain model_b but NOT the test node
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
        # model_d is independent and has no model children
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
            # No compiled/ directory in fixtures, so SQL should be absent
            assert model._compiled_sql is None
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
