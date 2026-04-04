"""Unit tests for find_impacted_exposures."""

from __future__ import annotations

from dbt_guard.impact import find_impacted_exposures
from dbt_guard.models import (
    ColumnChange,
    ColumnLineageImpact,
    ExposureInfo,
    ImpactedColumn,
    ImpactedExposure,
)


def _make_exposure(
    name: str,
    depends_on: list,
    type_: str = "dashboard",
    owner_name: str = "Owner",
) -> ExposureInfo:
    return ExposureInfo(
        exposure_id=f"exposure.pkg.{name}",
        name=name,
        type=type_,
        owner_name=owner_name,
        depends_on_nodes=depends_on,
    )


class TestFindImpactedExposures:
    def test_exposure_depending_on_changed_model(self) -> None:
        exposures = {
            "exposure.pkg.dash": _make_exposure(
                "dash", ["model.pkg.model_a"]
            ),
        }
        result = find_impacted_exposures(
            impacted_model_ids=set(),
            changed_model_ids={"model.pkg.model_a"},
            exposures=exposures,
        )
        assert len(result) == 1
        assert result[0].name == "dash"
        assert "model_a" in result[0].impacted_models

    def test_exposure_depending_on_impacted_model(self) -> None:
        exposures = {
            "exposure.pkg.dash": _make_exposure(
                "dash", ["model.pkg.model_b"]
            ),
        }
        result = find_impacted_exposures(
            impacted_model_ids={"model.pkg.model_b"},
            changed_model_ids={"model.pkg.model_a"},
            exposures=exposures,
        )
        assert len(result) == 1
        assert "model_b" in result[0].impacted_models

    def test_exposure_not_affected(self) -> None:
        exposures = {
            "exposure.pkg.dash": _make_exposure(
                "dash", ["model.pkg.model_z"]
            ),
        }
        result = find_impacted_exposures(
            impacted_model_ids=set(),
            changed_model_ids={"model.pkg.model_a"},
            exposures=exposures,
        )
        assert result == []

    def test_empty_exposures(self) -> None:
        result = find_impacted_exposures(
            impacted_model_ids=set(),
            changed_model_ids={"model.pkg.model_a"},
            exposures={},
        )
        assert result == []

    def test_column_info_from_breaking_changes(self) -> None:
        exposures = {
            "exposure.pkg.dash": _make_exposure(
                "dash", ["model.pkg.model_a"]
            ),
        }
        breaking = [
            ColumnChange(
                change_type="removed",
                model_id="model.pkg.model_a",
                model_name="model_a",
                column_name="email",
            )
        ]
        result = find_impacted_exposures(
            impacted_model_ids=set(),
            changed_model_ids={"model.pkg.model_a"},
            exposures=exposures,
            breaking_changes=breaking,
        )
        assert len(result) == 1
        assert "model_a" in result[0].impacted_columns
        assert "email" in result[0].impacted_columns["model_a"]

    def test_multiple_exposures_mixed(self) -> None:
        exposures = {
            "exposure.pkg.affected": _make_exposure(
                "affected", ["model.pkg.model_a"]
            ),
            "exposure.pkg.safe": _make_exposure(
                "safe", ["model.pkg.model_z"]
            ),
        }
        result = find_impacted_exposures(
            impacted_model_ids=set(),
            changed_model_ids={"model.pkg.model_a"},
            exposures=exposures,
        )
        assert len(result) == 1
        assert result[0].name == "affected"

    def test_exposure_with_lineage_impacts(self) -> None:
        exposures = {
            "exposure.pkg.dash": _make_exposure(
                "dash", ["model.pkg.model_b"]
            ),
        }
        lineage_impacts = [
            ColumnLineageImpact(
                model_id="model.pkg.model_b",
                model_name="model_b",
                impacted_columns=[
                    ImpactedColumn(column_name="email", reason="references email from model_a")
                ],
                cleared=False,
            )
        ]
        result = find_impacted_exposures(
            impacted_model_ids={"model.pkg.model_b"},
            changed_model_ids=set(),
            exposures=exposures,
            column_lineage_impacts=lineage_impacts,
        )
        assert len(result) == 1
        assert "model_b" in result[0].impacted_columns
        assert "email" in result[0].impacted_columns["model_b"]
