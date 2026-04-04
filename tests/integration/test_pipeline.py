"""End-to-end pipeline tests using on-disk fixtures with compiled SQL.

These tests exercise the full flow:

    manifest loading → SQL enrichment → column diff → BFS impact
    → column lineage resolution (via compiled SQL) → exposure analysis

The ``full/`` fixtures contain:

  base:    model_a has columns (id, name, email, status)
  current: model_a has columns (id, name, status, created_at) — email REMOVED

  model_b compiled SQL: SELECT a.id, a.name AS user_name, a.email, a.status
                        → DOES reference email → should be IMPACTED

  model_c compiled SQL: SELECT a.id, a.status
                        → does NOT reference email → should be CLEARED

  source raw_db.users: email removed in current → source breaking change

  exposure user_dashboard: depends on model_a + model_b → should be impacted
  exposure stable_report:  depends on model_c only     → should NOT be impacted
"""

from __future__ import annotations

from pathlib import Path

from dbt_guard.cli import PipelineConfig, run_pipeline
from dbt_guard.models import DiffReport

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
FULL_BASE = FIXTURES_DIR / "manifests" / "full" / "base"
FULL_CURRENT = FIXTURES_DIR / "manifests" / "full" / "current"


def _run(**overrides) -> DiffReport:
    """Run the pipeline with full fixtures and optional overrides."""
    defaults = dict(
        base=FULL_BASE,
        current=FULL_CURRENT,
    )
    defaults.update(overrides)
    config = PipelineConfig(**defaults)
    return run_pipeline(config)


# ---------------------------------------------------------------------------
# Model-level diff (no lineage)
# ---------------------------------------------------------------------------


class TestModelDiff:
    def test_detects_email_removal_as_breaking(self) -> None:
        report = _run()
        breaking_cols = {(c.model_name, c.column_name) for c in report.breaking_changes}
        assert ("model_a", "email") in breaking_cols

    def test_detects_created_at_addition_as_non_breaking(self) -> None:
        report = _run()
        added_cols = {(c.model_name, c.column_name) for c in report.non_breaking_changes}
        assert ("model_a", "created_at") in added_cols

    def test_total_changes_includes_all(self) -> None:
        report = _run()
        assert report.total_changes >= 2  # at least 1 breaking + 1 non-breaking


# ---------------------------------------------------------------------------
# BFS impact (model-level, no column lineage)
# ---------------------------------------------------------------------------


class TestBfsImpact:
    def test_model_b_impacted_at_distance_one(self) -> None:
        report = _run()
        impacted = {m.model_name: m.distance for m in report.impacted_models}
        assert impacted["model_b"] == 1

    def test_model_c_impacted_at_distance_one(self) -> None:
        """Without --column-lineage, model_c is a false positive."""
        report = _run()
        impacted = {m.model_name for m in report.impacted_models}
        assert "model_c" in impacted

    def test_no_impact_skips_bfs(self) -> None:
        report = _run(no_impact=True)
        assert report.impacted_models == []


# ---------------------------------------------------------------------------
# Column lineage resolution (the core feature)
# ---------------------------------------------------------------------------


class TestColumnLineage:
    def test_model_b_impacted_because_it_references_email(self) -> None:
        """model_b SQL: SELECT a.id, a.name AS user_name, a.email, a.status
        → references email → must be impacted."""
        report = _run(column_lineage=True)
        impacted_names = {m.model_name for m in report.impacted_models}
        assert "model_b" in impacted_names

    def test_model_c_cleared_because_it_does_not_reference_email(self) -> None:
        """model_c SQL: SELECT a.id, a.status
        → does NOT reference email → must be cleared (false positive eliminated)."""
        report = _run(column_lineage=True)
        assert "model_c" in report.cleared_models

    def test_cleared_model_removed_from_impacted_list(self) -> None:
        report = _run(column_lineage=True)
        impacted_names = {m.model_name for m in report.impacted_models}
        assert "model_c" not in impacted_names

    def test_lineage_impact_detail_for_model_b(self) -> None:
        """Column lineage should report which columns in model_b are affected."""
        report = _run(column_lineage=True)
        b_impacts = [
            i for i in report.column_lineage_impacts
            if i.model_id == "model.test_pkg.model_b" and not i.cleared
        ]
        assert len(b_impacts) == 1
        col_names = {ic.column_name for ic in b_impacts[0].impacted_columns}
        assert "email" in col_names

    def test_lineage_impact_marks_model_c_as_cleared(self) -> None:
        report = _run(column_lineage=True)
        c_impacts = [
            i for i in report.column_lineage_impacts
            if i.model_id == "model.test_pkg.model_c"
        ]
        assert len(c_impacts) == 1
        assert c_impacts[0].cleared is True
        assert c_impacts[0].impacted_columns == []


# ---------------------------------------------------------------------------
# Source changes
# ---------------------------------------------------------------------------


class TestSourceChanges:
    def test_source_email_removal_detected(self) -> None:
        report = _run(include_sources=True)
        breaking_src = [c for c in report.source_changes if c.is_breaking]
        src_cols = {(c.model_name, c.column_name) for c in breaking_src}
        assert ("users", "email") in src_cols

    def test_source_changes_counted_in_total(self) -> None:
        report = _run(include_sources=True)
        assert len(report.source_changes) >= 1
        assert report.total_changes >= len(report.breaking_changes) + len(report.non_breaking_changes) + len(report.source_changes)

    def test_source_breaking_triggers_has_breaking(self) -> None:
        """Source breaking changes should trigger has_breaking_changes even without model changes."""
        report = _run(include_sources=True)
        assert report.has_breaking_changes is True


# ---------------------------------------------------------------------------
# Exposure impact
# ---------------------------------------------------------------------------


class TestExposureImpact:
    def test_user_dashboard_impacted(self) -> None:
        """user_dashboard depends on model_a + model_b → should be impacted."""
        report = _run(include_exposures=True)
        exp_names = {e.name for e in report.impacted_exposures}
        assert "user_dashboard" in exp_names

    def test_user_dashboard_lists_affected_models(self) -> None:
        report = _run(include_exposures=True)
        dashboard = next(e for e in report.impacted_exposures if e.name == "user_dashboard")
        assert "model_a" in dashboard.impacted_models

    def test_stable_report_not_impacted_with_lineage(self) -> None:
        """stable_report depends only on model_c.
        With --column-lineage, model_c is cleared → stable_report should not be impacted."""
        report = _run(include_exposures=True, column_lineage=True)
        exp_names = {e.name for e in report.impacted_exposures}
        assert "stable_report" not in exp_names


# ---------------------------------------------------------------------------
# Undocumented sources
# ---------------------------------------------------------------------------


class TestUndocumentedSources:
    def test_orders_flagged_as_undocumented(self) -> None:
        report = _run(include_sources=True, warn_undocumented_sources=True)
        assert "source.test_pkg.raw_db.orders" in report.undocumented_sources

    def test_users_not_flagged(self) -> None:
        report = _run(include_sources=True, warn_undocumented_sources=True)
        assert "source.test_pkg.raw_db.users" not in report.undocumented_sources


# ---------------------------------------------------------------------------
# Full pipeline — all features combined
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_all_features_together(self) -> None:
        """Exercise every feature in a single run and verify the complete picture."""
        report = _run(
            include_sources=True,
            include_exposures=True,
            include_snapshots=True,
            column_lineage=True,
            warn_undocumented_sources=True,
        )

        # Breaking change detected
        assert report.has_breaking_changes is True
        breaking_cols = {c.column_name for c in report.breaking_changes}
        assert "email" in breaking_cols

        # Non-breaking change detected
        added_cols = {c.column_name for c in report.non_breaking_changes}
        assert "created_at" in added_cols

        # Source change detected
        src_breaking = [c for c in report.source_changes if c.is_breaking]
        assert any(c.column_name == "email" for c in src_breaking)

        # Column lineage: model_b impacted, model_c cleared
        impacted_names = {m.model_name for m in report.impacted_models}
        assert "model_b" in impacted_names
        assert "model_c" not in impacted_names
        assert "model_c" in report.cleared_models

        # Exposure: user_dashboard impacted, stable_report not
        exp_names = {e.name for e in report.impacted_exposures}
        assert "user_dashboard" in exp_names
        assert "stable_report" not in exp_names

        # Undocumented source warning
        assert "source.test_pkg.raw_db.orders" in report.undocumented_sources
