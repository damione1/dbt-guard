"""Unit tests for dbt_guard.reporter."""

from __future__ import annotations

import json

import pytest

from dbt_guard.models import ColumnChange, DiffReport, ImpactedModel
from dbt_guard.reporter import format_report


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_report(
    breaking=None,
    non_breaking=None,
    impacted=None,
) -> DiffReport:
    return DiffReport(
        base_path="/ci/base/target",
        current_path="/ci/current/target",
        breaking_changes=breaking or [],
        non_breaking_changes=non_breaking or [],
        impacted_models=impacted or [],
    )


def _removed(model_name: str, col: str, model_id: str = "model.pkg.m") -> ColumnChange:
    return ColumnChange(
        change_type="removed",
        model_id=model_id,
        model_name=model_name,
        column_name=col,
        is_breaking=True,
    )


def _added(model_name: str, col: str, model_id: str = "model.pkg.m") -> ColumnChange:
    return ColumnChange(
        change_type="added",
        model_id=model_id,
        model_name=model_name,
        column_name=col,
        is_breaking=False,
    )


def _renamed(model_name: str, old: str, new: str, model_id: str = "model.pkg.m") -> ColumnChange:
    return ColumnChange(
        change_type="renamed",
        model_id=model_id,
        model_name=model_name,
        column_name=old,
        old_value=old,
        new_value=new,
        is_breaking=True,
    )


def _type_changed(
    model_name: str, col: str, old: str, new: str, model_id: str = "model.pkg.m"
) -> ColumnChange:
    return ColumnChange(
        change_type="type_changed",
        model_id=model_id,
        model_name=model_name,
        column_name=col,
        old_value=old,
        new_value=new,
        is_breaking=True,
    )


def _impacted(name: str, dist: int) -> ImpactedModel:
    return ImpactedModel(model_id=f"model.pkg.{name}", model_name=name, distance=dist)


# ---------------------------------------------------------------------------
# Text format
# ---------------------------------------------------------------------------


class TestTextFormat:
    def test_pass_message_when_no_breaking(self) -> None:
        report = _make_report()
        output = format_report(report, "text")
        assert "PASS" in output
        assert "no breaking changes" in output.lower()

    def test_fail_message_when_breaking(self) -> None:
        report = _make_report(breaking=[_removed("my_model", "email")])
        output = format_report(report, "text")
        assert "FAIL" in output
        assert "breaking changes detected" in output.lower()

    def test_removed_column_shown(self) -> None:
        report = _make_report(breaking=[_removed("my_model", "email")])
        output = format_report(report, "text")
        assert "REMOVED" in output
        assert "email" in output
        assert "my_model" in output

    def test_renamed_column_shown(self) -> None:
        report = _make_report(breaking=[_renamed("m", "old_col", "new_col")])
        output = format_report(report, "text")
        assert "RENAMED" in output
        assert "old_col" in output
        assert "new_col" in output

    def test_type_change_shown(self) -> None:
        report = _make_report(breaking=[_type_changed("m", "amount", "integer", "varchar")])
        output = format_report(report, "text")
        assert "TYPE" in output
        assert "integer" in output
        assert "varchar" in output

    def test_added_column_shown_in_non_breaking(self) -> None:
        report = _make_report(non_breaking=[_added("m", "created_at")])
        output = format_report(report, "text")
        assert "NON-BREAKING" in output
        assert "ADDED" in output
        assert "created_at" in output

    def test_impacted_models_shown(self) -> None:
        report = _make_report(
            breaking=[_removed("m", "col")],
            impacted=[_impacted("downstream", 1)],
        )
        output = format_report(report, "text")
        assert "DOWNSTREAM IMPACT" in output
        assert "downstream" in output
        assert "distance: 1" in output

    def test_paths_included(self) -> None:
        report = _make_report()
        output = format_report(report, "text")
        assert "/ci/base/target" in output
        assert "/ci/current/target" in output


# ---------------------------------------------------------------------------
# JSON format
# ---------------------------------------------------------------------------


class TestJsonFormat:
    def test_produces_valid_json(self) -> None:
        report = _make_report(breaking=[_removed("m", "col")])
        output = format_report(report, "json")
        parsed = json.loads(output)  # should not raise
        assert isinstance(parsed, dict)

    def test_breaking_change_in_json(self) -> None:
        report = _make_report(breaking=[_removed("my_model", "email")])
        parsed = json.loads(format_report(report, "json"))
        assert parsed["has_breaking_changes"] is True
        assert parsed["summary"]["breaking"] == 1
        bc = parsed["breaking_changes"][0]
        assert bc["model"] == "my_model"
        assert bc["column"] == "email"
        assert bc["type"] == "removed"

    def test_no_breaking_in_json(self) -> None:
        report = _make_report(non_breaking=[_added("m", "new_col")])
        parsed = json.loads(format_report(report, "json"))
        assert parsed["has_breaking_changes"] is False
        assert parsed["summary"]["breaking"] == 0
        assert parsed["summary"]["non_breaking"] == 1

    def test_impacted_models_in_json(self) -> None:
        report = _make_report(
            breaking=[_removed("m", "col")],
            impacted=[_impacted("ds", 2)],
        )
        parsed = json.loads(format_report(report, "json"))
        assert parsed["summary"]["impacted_models"] == 1
        assert parsed["impacted_models"][0]["distance"] == 2

    def test_rename_details_in_json(self) -> None:
        report = _make_report(breaking=[_renamed("m", "old", "new")])
        parsed = json.loads(format_report(report, "json"))
        bc = parsed["breaking_changes"][0]
        assert bc["type"] == "renamed"
        assert bc["old_value"] == "old"
        assert bc["new_value"] == "new"


# ---------------------------------------------------------------------------
# GitHub Actions format
# ---------------------------------------------------------------------------


class TestGithubFormat:
    def test_error_annotation_for_breaking(self) -> None:
        report = _make_report(breaking=[_removed("my_model", "email")])
        output = format_report(report, "github")
        assert "::error::" in output
        assert "email" in output
        assert "my_model" in output

    def test_notice_when_no_breaking(self) -> None:
        report = _make_report()
        output = format_report(report, "github")
        assert "::notice::" in output
        assert "::error::" not in output

    def test_multiple_breaking_produce_multiple_annotations(self) -> None:
        report = _make_report(
            breaking=[
                _removed("m", "col_a", "model.pkg.m"),
                _removed("m", "col_b", "model.pkg.m"),
            ]
        )
        output = format_report(report, "github")
        assert output.count("::error::") == 2

    def test_renamed_annotation(self) -> None:
        report = _make_report(breaking=[_renamed("m", "old_col", "new_col")])
        output = format_report(report, "github")
        assert "::error::" in output
        assert "renamed" in output.lower()
        assert "old_col" in output
        assert "new_col" in output

    def test_type_changed_annotation(self) -> None:
        report = _make_report(breaking=[_type_changed("m", "amount", "int", "varchar")])
        output = format_report(report, "github")
        assert "::error::" in output
        assert "type changed" in output.lower() or "int" in output

    def test_impacted_count_in_annotation(self) -> None:
        report = _make_report(
            breaking=[_removed("m", "col")],
            impacted=[_impacted("ds1", 1), _impacted("ds2", 2)],
        )
        output = format_report(report, "github")
        assert "2" in output  # 2 impacted models mentioned


# ---------------------------------------------------------------------------
# Default format fallback
# ---------------------------------------------------------------------------


class TestDefaultFormat:
    def test_unknown_format_falls_back_to_text(self) -> None:
        report = _make_report()
        output = format_report(report, "unknown_format")
        # Falls back to text format
        assert "dbt-guard" in output
