"""Unit tests for dbt_guard.differ."""

from __future__ import annotations

import pytest

from dbt_guard.differ import diff_models, _diff_model_columns
from dbt_guard.models import ColumnChange, ColumnInfo, ModelColumns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_model(uid: str, name: str, cols: dict) -> ModelColumns:
    """Build a ModelColumns with the given columns dict.

    *cols* maps column_name -> data_type (str or None).
    """
    return ModelColumns(
        model_id=uid,
        model_name=name,
        columns={
            k.lower(): ColumnInfo(name=k.lower(), data_type=v)
            for k, v in cols.items()
        },
    )


MODEL_ID = "model.pkg.test_model"
MODEL_NAME = "test_model"


# ---------------------------------------------------------------------------
# Tests: removed columns
# ---------------------------------------------------------------------------


class TestRemovedColumns:
    def test_removed_column_is_breaking(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": "integer", "email": "varchar"})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": "integer"})
        changes = _diff_model_columns(base, current)
        removed = [c for c in changes if c.change_type == "removed"]
        assert len(removed) == 1
        assert removed[0].column_name == "email"
        assert removed[0].is_breaking is True

    def test_multiple_removed_columns(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": None, "a": None, "b": None, "c": None})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": None})
        changes = _diff_model_columns(base, current)
        removed_names = {c.column_name for c in changes if c.change_type == "removed"}
        assert removed_names == {"a", "b", "c"}
        for c in changes:
            if c.change_type == "removed":
                assert c.is_breaking is True

    def test_removed_carries_model_metadata(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"email": "varchar"})
        current = make_model(MODEL_ID, MODEL_NAME, {})
        # With 0 added + 1 removed (not a rename scenario since adds=0)
        changes = _diff_model_columns(base, current)
        removed = [c for c in changes if c.change_type == "removed"]
        assert removed[0].model_id == MODEL_ID
        assert removed[0].model_name == MODEL_NAME


# ---------------------------------------------------------------------------
# Tests: added columns
# ---------------------------------------------------------------------------


class TestAddedColumns:
    def test_added_column_is_non_breaking(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": "integer"})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": "integer", "created_at": "timestamp"})
        changes = _diff_model_columns(base, current)
        added = [c for c in changes if c.change_type == "added"]
        assert len(added) == 1
        assert added[0].column_name == "created_at"
        assert added[0].is_breaking is False

    def test_multiple_added_columns(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": None})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": None, "x": None, "y": None})
        changes = _diff_model_columns(base, current)
        added_names = {c.column_name for c in changes if c.change_type == "added"}
        assert added_names == {"x", "y"}


# ---------------------------------------------------------------------------
# Tests: type changes
# ---------------------------------------------------------------------------


class TestTypeChanges:
    def test_type_change_is_breaking(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"amount": "integer"})
        current = make_model(MODEL_ID, MODEL_NAME, {"amount": "varchar"})
        changes = _diff_model_columns(base, current)
        type_changes = [c for c in changes if c.change_type == "type_changed"]
        assert len(type_changes) == 1
        assert type_changes[0].column_name == "amount"
        assert type_changes[0].old_value == "integer"
        assert type_changes[0].new_value == "varchar"
        assert type_changes[0].is_breaking is True

    def test_type_change_requires_both_sides_documented(self) -> None:
        # If either side has data_type=None, no type_changed event is emitted
        base = make_model(MODEL_ID, MODEL_NAME, {"amount": None})
        current = make_model(MODEL_ID, MODEL_NAME, {"amount": "varchar"})
        changes = _diff_model_columns(base, current)
        assert not any(c.change_type == "type_changed" for c in changes)

    def test_same_type_no_change(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"amount": "numeric"})
        current = make_model(MODEL_ID, MODEL_NAME, {"amount": "numeric"})
        changes = _diff_model_columns(base, current)
        assert changes == []


# ---------------------------------------------------------------------------
# Tests: rename detection
# ---------------------------------------------------------------------------


class TestRenameDetection:
    def test_single_removed_single_added_same_type_is_rename(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": None, "old_name": "varchar"})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": None, "new_name": "varchar"})
        changes = _diff_model_columns(base, current)
        renamed = [c for c in changes if c.change_type == "renamed"]
        assert len(renamed) == 1
        assert renamed[0].column_name == "old_name"
        assert renamed[0].old_value == "old_name"
        assert renamed[0].new_value == "new_name"
        assert renamed[0].is_breaking is True

    def test_rename_with_both_none_types(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"old_col": None})
        current = make_model(MODEL_ID, MODEL_NAME, {"new_col": None})
        changes = _diff_model_columns(base, current)
        renamed = [c for c in changes if c.change_type == "renamed"]
        assert len(renamed) == 1

    def test_type_mismatch_not_treated_as_rename(self) -> None:
        # Different types → separate removed + added (not a rename)
        base = make_model(MODEL_ID, MODEL_NAME, {"old_col": "integer"})
        current = make_model(MODEL_ID, MODEL_NAME, {"new_col": "varchar"})
        changes = _diff_model_columns(base, current)
        assert not any(c.change_type == "renamed" for c in changes)
        removed = [c for c in changes if c.change_type == "removed"]
        added = [c for c in changes if c.change_type == "added"]
        assert len(removed) == 1
        assert len(added) == 1

    def test_multiple_removed_added_not_treated_as_rename(self) -> None:
        # Ambiguous: 2 removed, 2 added — not a rename
        base = make_model(MODEL_ID, MODEL_NAME, {"a": None, "b": None})
        current = make_model(MODEL_ID, MODEL_NAME, {"c": None, "d": None})
        changes = _diff_model_columns(base, current)
        assert not any(c.change_type == "renamed" for c in changes)


# ---------------------------------------------------------------------------
# Tests: no changes
# ---------------------------------------------------------------------------


class TestNoChanges:
    def test_identical_models_produce_no_changes(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {"id": "integer", "name": "varchar"})
        current = make_model(MODEL_ID, MODEL_NAME, {"id": "integer", "name": "varchar"})
        changes = _diff_model_columns(base, current)
        assert changes == []

    def test_empty_models_produce_no_changes(self) -> None:
        base = make_model(MODEL_ID, MODEL_NAME, {})
        current = make_model(MODEL_ID, MODEL_NAME, {})
        changes = _diff_model_columns(base, current)
        assert changes == []


# ---------------------------------------------------------------------------
# Tests: diff_models (multi-model)
# ---------------------------------------------------------------------------


class TestDiffModels:
    def test_only_common_models_are_diffed(self) -> None:
        base = {
            "model.pkg.a": make_model("model.pkg.a", "a", {"col1": None}),
        }
        current = {
            "model.pkg.a": make_model("model.pkg.a", "a", {}),     # col1 removed
            "model.pkg.b": make_model("model.pkg.b", "b", {"col1": None}),  # new model
        }
        changes = diff_models(base, current)
        # model_b is new in current — not diffed, no change reported for it
        model_ids = {c.model_id for c in changes}
        assert "model.pkg.b" not in model_ids
        # model_a col1 was removed
        assert any(c.change_type == "removed" and c.model_name == "a" for c in changes)

    def test_fixture_manifests(self, base_manifest_dir, current_manifest_dir) -> None:
        from dbt_guard.manifest import load_manifest

        base_models, _ = load_manifest(base_manifest_dir)
        current_models, _ = load_manifest(current_manifest_dir)
        changes = diff_models(base_models, current_models)

        # model_a: email removed (breaking) + created_at added (non-breaking)
        breaking = [c for c in changes if c.is_breaking]
        non_breaking = [c for c in changes if not c.is_breaking]

        removed_email = [
            c for c in breaking if c.change_type == "removed" and c.column_name == "email"
        ]
        assert len(removed_email) == 1, "email removal should be detected"

        added_created_at = [
            c for c in non_breaking if c.change_type == "added" and c.column_name == "created_at"
        ]
        assert len(added_created_at) == 1, "created_at addition should be detected"
