"""Unit tests for v0.2 reporter sections."""

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


def _base_report(**kwargs) -> DiffReport:
    defaults = dict(
        base_path="/base",
        current_path="/current",
        breaking_changes=[],
        non_breaking_changes=[],
        impacted_models=[],
    )
    defaults.update(kwargs)
    return DiffReport(**defaults)


class TestTextFormatV2:
    def test_source_breaking_changes_section(self) -> None:
        report = _base_report(
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
        report = _base_report(
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
        report = _base_report(
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
        report = _base_report(cleared_models=["model_c"])
        text = format_report(report, "text")
        assert "CLEARED MODELS" in text
        assert "model_c" in text

    def test_exposure_impact_section(self) -> None:
        report = _base_report(
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
        report = _base_report(
            undocumented_sources=["source.pkg.raw.orders"]
        )
        text = format_report(report, "text")
        assert "WARNINGS" in text
        assert "source.pkg.raw.orders" in text

    def test_no_v2_sections_when_empty(self) -> None:
        report = _base_report()
        text = format_report(report, "text")
        assert "SOURCE CHANGES" not in text
        assert "COLUMN LINEAGE" not in text
        assert "CLEARED MODELS" not in text
        assert "EXPOSURE IMPACT" not in text
        assert "WARNINGS" not in text

    def test_has_breaking_changes_includes_source_changes(self) -> None:
        report = _base_report(
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


class TestJsonFormatV2:
    def test_json_has_v2_keys(self) -> None:
        report = _base_report(
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

    def test_json_summary_v2_keys(self) -> None:
        report = _base_report(
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

    def test_json_backward_compat(self) -> None:
        """JSON output still has original keys when no v0.2 data."""
        report = _base_report()
        parsed = json.loads(format_report(report, "json"))
        assert "breaking_changes" in parsed
        assert "non_breaking_changes" in parsed
        assert "impacted_models" in parsed
        assert parsed["source_changes"] == []
        assert parsed["cleared_models"] == []


class TestGithubFormatV2:
    def test_source_breaking_error_annotation(self) -> None:
        report = _base_report(
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
        report = _base_report(
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
        report = _base_report(cleared_models=["model_c"])
        text = format_report(report, "github")
        assert "::notice::" in text
        assert "model_c" in text
        assert "cleared" in text

    def test_no_notice_when_source_breaking(self) -> None:
        """When source breaking changes exist, no 'no breaking changes' notice."""
        report = _base_report(
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
