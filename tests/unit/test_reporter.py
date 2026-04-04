"""Unit tests for dbt_guard.reporter."""

from __future__ import annotations

import json

from dbt_guard.models import (
    ColumnChange,
    ColumnLineageImpact,
    ColumnLineageLink,
    DiffReport,
    ImpactedColumn,
    ImpactedExposure,
    ImpactedModel,
)
from dbt_guard.reporter import format_report

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_report(
    breaking=None,
    non_breaking=None,
    impacted=None,
    **kwargs,
) -> DiffReport:
    defaults = dict(
        base_path="/ci/base/target",
        current_path="/ci/current/target",
        breaking_changes=breaking or [],
        non_breaking_changes=non_breaking or [],
        impacted_models=impacted or [],
    )
    defaults.update(kwargs)
    return DiffReport(**defaults)


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
# Text format — sources, exposures, lineage sections
# ---------------------------------------------------------------------------


class TestTextFormatSources:
    def test_source_breaking_changes_section(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
        )
        text = format_report(report, "text")
        assert "SOURCE CHANGES" in text
        assert "BREAKING" in text
        assert "email" in text

    def test_source_non_breaking_section(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="added",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="phone",
                    is_breaking=False,
                )
            ],
        )
        text = format_report(report, "text")
        assert "NON-BREAKING" in text
        assert "phone" in text

    def test_column_lineage_impact_section(self) -> None:
        report = _make_report(
            column_lineage_impacts=[
                ColumnLineageImpact(
                    model_id="model.pkg.b",
                    model_name="model_b",
                    impacted_columns=[
                        ImpactedColumn(
                            column_name="email",
                            reason="references removed column email from model_a",
                            chain=[
                                ColumnLineageLink("model.pkg.a", "model_a", "email"),
                                ColumnLineageLink("model.pkg.b", "model_b", "email"),
                            ],
                        )
                    ],
                    cleared=False,
                ),
            ],
        )
        text = format_report(report, "text")
        assert "COLUMN LINEAGE IMPACT" in text
        assert "model_b" in text
        assert "email" in text
        assert "chain:" in text

    def test_cleared_models_section(self) -> None:
        report = _make_report(cleared_models=["model_c"])
        text = format_report(report, "text")
        assert "CLEARED MODELS" in text
        assert "model_c" in text

    def test_exposure_impact_section(self) -> None:
        report = _make_report(
            impacted_exposures=[
                ImpactedExposure(
                    exposure_id="exposure.pkg.dash",
                    name="user_dashboard",
                    type="dashboard",
                    owner_name="Alice",
                    owner_email="alice@example.com",
                    url="https://bi.example.com/dash",
                    impacted_models=["model_a"],
                    impacted_columns={"model_a": ["email"]},
                )
            ],
        )
        text = format_report(report, "text")
        assert "EXPOSURE IMPACT" in text
        assert "user_dashboard" in text
        assert "Alice" in text
        assert "model_a" in text

    def test_undocumented_sources_section(self) -> None:
        report = _make_report(
            undocumented_sources=["source.pkg.raw.orders"]
        )
        text = format_report(report, "text")
        assert "WARNINGS" in text
        assert "source.pkg.raw.orders" in text

    def test_no_optional_sections_when_empty(self) -> None:
        report = _make_report()
        text = format_report(report, "text")
        assert "SOURCE CHANGES" not in text
        assert "COLUMN LINEAGE" not in text
        assert "CLEARED MODELS" not in text
        assert "EXPOSURE IMPACT" not in text
        assert "WARNINGS" not in text

    def test_has_breaking_changes_includes_source_changes(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
        )
        assert report.has_breaking_changes is True
        text = format_report(report, "text")
        assert "FAIL" in text


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
# JSON format — sources, exposures, lineage keys
# ---------------------------------------------------------------------------


class TestJsonFormatExtended:
    def test_json_has_extended_keys(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
            cleared_models=["model_c"],
            impacted_exposures=[
                ImpactedExposure(
                    exposure_id="exposure.pkg.dash",
                    name="dashboard",
                    type="dashboard",
                    impacted_models=["model_a"],
                )
            ],
            undocumented_sources=["source.pkg.raw.orders"],
        )
        parsed = json.loads(format_report(report, "json"))
        assert "source_changes" in parsed
        assert "column_lineage_impact" in parsed
        assert "cleared_models" in parsed
        assert "exposure_impact" in parsed
        assert "undocumented_sources" in parsed

    def test_json_summary_extended_keys(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
        )
        parsed = json.loads(format_report(report, "json"))
        assert parsed["summary"]["sources_changed"] == 1
        assert parsed["summary"]["models_cleared"] == 0
        assert parsed["summary"]["exposures_impacted"] == 0

    def test_json_defaults_when_no_optional_data(self) -> None:
        """JSON output has all keys with empty defaults when no optional data is present."""
        report = _make_report()
        parsed = json.loads(format_report(report, "json"))
        assert "breaking_changes" in parsed
        assert "non_breaking_changes" in parsed
        assert "impacted_models" in parsed
        assert parsed["source_changes"] == []
        assert parsed["cleared_models"] == []


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
# GitHub Actions format — sources, exposures
# ---------------------------------------------------------------------------


class TestGithubFormatExtended:
    def test_source_breaking_error_annotation(self) -> None:
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
        )
        text = format_report(report, "github")
        assert "::error::" in text
        assert "Source column" in text
        assert "email" in text

    def test_exposure_warning_annotation(self) -> None:
        report = _make_report(
            breaking_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="model.pkg.a",
                    model_name="model_a",
                    column_name="email",
                    is_breaking=True,
                )
            ],
            impacted_exposures=[
                ImpactedExposure(
                    exposure_id="exposure.pkg.dash",
                    name="dashboard",
                    type="dashboard",
                    owner_name="Alice",
                    impacted_models=["model_a"],
                )
            ],
        )
        text = format_report(report, "github")
        assert "::warning::" in text
        assert "dashboard" in text
        assert "Alice" in text

    def test_cleared_notice_annotation(self) -> None:
        report = _make_report(cleared_models=["model_c"])
        text = format_report(report, "github")
        assert "::notice::" in text
        assert "model_c" in text
        assert "cleared" in text

    def test_no_notice_when_source_breaking(self) -> None:
        """When source breaking changes exist, no 'no breaking changes' notice."""
        report = _make_report(
            source_changes=[
                ColumnChange(
                    change_type="removed",
                    model_id="source.pkg.raw.users",
                    model_name="users",
                    column_name="email",
                    is_breaking=True,
                )
            ],
        )
        text = format_report(report, "github")
        assert "no breaking column changes detected" not in text


# ---------------------------------------------------------------------------
# Default format fallback
# ---------------------------------------------------------------------------


class TestDefaultFormat:
    def test_unknown_format_falls_back_to_text(self) -> None:
        report = _make_report()
        output = format_report(report, "unknown_format")
        # Falls back to text format
        assert "dbt-guard" in output
