"""Integration tests for the dbt-guard CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_guard.cli import main

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
BASE_DIR = FIXTURES_DIR / "manifests" / "base"
CURRENT_DIR = FIXTURES_DIR / "manifests" / "current"
FULL_BASE = FIXTURES_DIR / "manifests" / "full" / "base"
FULL_CURRENT = FIXTURES_DIR / "manifests" / "full" / "current"


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# Basic diff command
# ---------------------------------------------------------------------------


class TestDiffCommand:
    def test_exits_one_with_breaking_changes(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["diff", "--base", str(BASE_DIR), "--current", str(CURRENT_DIR)],
        )
        assert result.exit_code == 1, f"Expected exit 1, got {result.exit_code}.\nOutput:\n{result.output}"

    def test_output_contains_breaking_summary(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["diff", "--base", str(BASE_DIR), "--current", str(CURRENT_DIR)],
        )
        assert "BREAKING" in result.output or "breaking" in result.output.lower()

    def test_removed_email_appears_in_output(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["diff", "--base", str(BASE_DIR), "--current", str(CURRENT_DIR)],
        )
        assert "email" in result.output

    def test_fail_message_in_output(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            ["diff", "--base", str(BASE_DIR), "--current", str(CURRENT_DIR)],
        )
        assert "FAIL" in result.output


# ---------------------------------------------------------------------------
# --format json
# ---------------------------------------------------------------------------


class TestJsonFormat:
    def test_json_format_produces_valid_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)

    def test_json_has_breaking_changes_true(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is True

    def test_json_breaking_changes_list(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert len(parsed["breaking_changes"]) >= 1
        col_names = [bc["column"] for bc in parsed["breaking_changes"]]
        assert "email" in col_names

    def test_json_non_breaking_changes(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        non_breaking_cols = [c["column"] for c in parsed["non_breaking_changes"]]
        assert "created_at" in non_breaking_cols


# ---------------------------------------------------------------------------
# --format github
# ---------------------------------------------------------------------------


class TestGithubFormat:
    def test_github_format_error_annotations(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "github",
            ],
        )
        assert "::error::" in result.output

    def test_github_format_no_notice_when_breaking(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "github",
            ],
        )
        assert "::notice::" not in result.output


# ---------------------------------------------------------------------------
# --fail-on
# ---------------------------------------------------------------------------


class TestFailOn:
    def test_fail_on_never_exits_zero(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--fail-on", "never",
            ],
        )
        assert result.exit_code == 0

    def test_fail_on_breaking_exits_one(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--fail-on", "breaking",
            ],
        )
        assert result.exit_code == 1

    def test_fail_on_any_exits_one_with_non_breaking(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--fail-on", "any",
            ],
        )
        assert result.exit_code == 1

    def test_identical_manifests_fail_on_breaking_exits_zero(
        self, runner: CliRunner
    ) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(BASE_DIR),
                "--fail-on", "breaking",
            ],
        )
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# --no-impact
# ---------------------------------------------------------------------------


class TestNoImpact:
    def test_no_impact_flag_skips_impact_section(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--no-impact",
            ],
        )
        assert "DOWNSTREAM IMPACT" not in result.output

    def test_without_flag_impact_is_shown(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
            ],
        )
        assert "DOWNSTREAM IMPACT" in result.output


# ---------------------------------------------------------------------------
# Missing manifest
# ---------------------------------------------------------------------------


class TestMissingManifest:
    def test_missing_manifest_exits_two(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(tmp_path),
                "--current", str(CURRENT_DIR),
            ],
        )
        assert result.exit_code == 2

    def test_missing_manifest_shows_error_message(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(tmp_path),
                "--current", str(CURRENT_DIR),
            ],
        )
        assert "Error" in (result.output + (result.output or ""))


# ---------------------------------------------------------------------------
# --select filter
# ---------------------------------------------------------------------------


class TestSelectFilter:
    def test_select_limits_to_specific_model(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--select", "model_d",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is False

    def test_select_model_a_finds_changes(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--select", "model_a",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is True


# ---------------------------------------------------------------------------
# --quiet
# ---------------------------------------------------------------------------


class TestQuiet:
    def test_quiet_outputs_one_line(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--quiet",
            ],
        )
        lines = [l for l in result.output.strip().splitlines() if l.strip()]  # noqa: E741
        assert len(lines) == 1

    def test_quiet_line_contains_status(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--quiet",
            ],
        )
        assert "FAIL" in result.output or "PASS" in result.output


# ---------------------------------------------------------------------------
# --output to file
# ---------------------------------------------------------------------------


class TestOutputFile:
    def test_output_written_to_file(self, runner: CliRunner, tmp_path: Path) -> None:
        out_file = tmp_path / "report.txt"
        runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--output", str(out_file),
            ],
        )
        assert out_file.exists()
        content = out_file.read_text(encoding="utf-8")
        assert "dbt-guard" in content

    def test_json_written_to_file(self, runner: CliRunner, tmp_path: Path) -> None:
        out_file = tmp_path / "report.json"
        runner.invoke(
            main,
            [
                "diff",
                "--base", str(BASE_DIR),
                "--current", str(CURRENT_DIR),
                "--format", "json",
                "--output", str(out_file),
            ],
        )
        content = out_file.read_text(encoding="utf-8")
        parsed = json.loads(content)
        assert "breaking_changes" in parsed


# ---------------------------------------------------------------------------
# Default flags on full fixtures (sources/exposures/snapshots excluded)
# ---------------------------------------------------------------------------


class TestDefaultFlagsFullFixtures:
    def test_default_invocation_excludes_optional_features(self, runner: CliRunner) -> None:
        """Default invocation on full fixtures ignores sources/exposures/snapshots."""
        result = runner.invoke(
            main,
            ["diff", "--base", str(FULL_BASE), "--current", str(FULL_CURRENT)],
        )
        assert result.exit_code == 1
        assert "SOURCE CHANGES" not in result.output
        assert "EXPOSURE IMPACT" not in result.output


# ---------------------------------------------------------------------------
# --include-sources
# ---------------------------------------------------------------------------


class TestIncludeSources:
    def test_source_changes_in_text(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
            ],
        )
        assert "SOURCE CHANGES" in result.output
        assert "email" in result.output

    def test_source_changes_in_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["summary"]["sources_changed"] >= 1
        source_cols = [c["column"] for c in parsed["source_changes"]]
        assert "email" in source_cols


# ---------------------------------------------------------------------------
# --include-exposures
# ---------------------------------------------------------------------------


class TestIncludeExposures:
    def test_exposure_impact_in_text(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-exposures",
            ],
        )
        assert "EXPOSURE IMPACT" in result.output
        assert "user_dashboard" in result.output

    def test_exposure_impact_in_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-exposures",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["summary"]["exposures_impacted"] >= 1
        exp_names = [e["name"] for e in parsed["exposure_impact"]]
        assert "user_dashboard" in exp_names


# ---------------------------------------------------------------------------
# --include-snapshots
# ---------------------------------------------------------------------------


class TestIncludeSnapshots:
    def test_snapshot_included_in_diff(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-snapshots",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        model_ids = [c["model_id"] for c in parsed["breaking_changes"]]
        assert "model.test_pkg.model_a" in model_ids


# ---------------------------------------------------------------------------
# --column-lineage
# ---------------------------------------------------------------------------


class TestColumnLineage:
    def test_column_lineage_in_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--column-lineage",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert "column_lineage_impact" in parsed
        assert isinstance(parsed["column_lineage_impact"], list)

    def test_strict_lineage_requires_column_lineage(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--strict-lineage",
            ],
        )
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# --warn-undocumented-sources
# ---------------------------------------------------------------------------


class TestWarnUndocumentedSources:
    def test_undocumented_warning_in_text(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--warn-undocumented-sources",
            ],
        )
        assert "WARNINGS" in result.output
        assert "orders" in result.output

    def test_undocumented_in_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--warn-undocumented-sources",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert len(parsed["undocumented_sources"]) >= 1


# ---------------------------------------------------------------------------
# GitHub format with sources/exposures
# ---------------------------------------------------------------------------


class TestGithubFormatSources:
    def test_source_error_annotations(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--format", "github",
            ],
        )
        assert "::error::" in result.output

    def test_exposure_warning_annotations(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-exposures",
                "--format", "github",
            ],
        )
        assert "::warning::" in result.output


# ---------------------------------------------------------------------------
# Full pipeline (all flags combined)
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_all_flags_json(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--include-exposures",
                "--include-snapshots",
                "--column-lineage",
                "--warn-undocumented-sources",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is True
        assert parsed["summary"]["sources_changed"] >= 1
        assert parsed["summary"]["exposures_impacted"] >= 1
        assert isinstance(parsed["column_lineage_impact"], list)
        assert isinstance(parsed["undocumented_sources"], list)

    def test_all_flags_text(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(FULL_BASE),
                "--current", str(FULL_CURRENT),
                "--include-sources",
                "--include-exposures",
                "--include-snapshots",
                "--column-lineage",
                "--warn-undocumented-sources",
            ],
        )
        assert result.exit_code == 1
        assert "BREAKING CHANGES" in result.output
        assert "SOURCE CHANGES" in result.output
        assert "EXPOSURE IMPACT" in result.output
