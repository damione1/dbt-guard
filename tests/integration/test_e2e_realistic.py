"""End-to-end tests with synthetic manifest data for a fictional e-commerce project.

These fixtures simulate a generic dbt pattern:
  source(raw.orders) → stg_orders → int_order_summary → mart_revenue
                                                        ↗
  source(raw.users) → stg_users → int_user_metrics ----
                                                        ↘
                                                         mart_user_dashboard (exposure)

The base manifest has a `phone` column on stg_users that gets removed in current.
Column lineage should clear int_order_summary (doesn't reference `phone`) but
mark int_user_metrics as impacted (references `phone`).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_guard.cli import main


def _write_manifest(tmp: Path, manifest: dict, compiled_sql: dict | None = None) -> None:
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    if compiled_sql:
        for rel_path, sql in compiled_sql.items():
            full_path = tmp / "compiled" / rel_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(sql, encoding="utf-8")


BASE_MANIFEST = {
    "metadata": {"dbt_schema_version": "v12", "dbt_version": "1.9.0"},
    "nodes": {
        "model.shop.stg_orders": {
            "unique_id": "model.shop.stg_orders",
            "name": "stg_orders",
            "resource_type": "model",
            "package_name": "shop",
            "original_file_path": "models/staging/stg_orders.sql",
            "depends_on": {"nodes": ["source.shop.raw.orders"]},
            "columns": {
                "order_id": {"name": "order_id", "data_type": "integer"},
                "user_id": {"name": "user_id", "data_type": "integer"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "status": {"name": "status", "data_type": "varchar"},
            },
        },
        "model.shop.stg_users": {
            "unique_id": "model.shop.stg_users",
            "name": "stg_users",
            "resource_type": "model",
            "package_name": "shop",
            "original_file_path": "models/staging/stg_users.sql",
            "depends_on": {"nodes": ["source.shop.raw.users"]},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "integer"},
                "name": {"name": "name", "data_type": "varchar"},
                "email": {"name": "email", "data_type": "varchar"},
                "phone": {"name": "phone", "data_type": "varchar"},
                "created_at": {"name": "created_at", "data_type": "timestamp"},
            },
        },
        "model.shop.int_order_summary": {
            "unique_id": "model.shop.int_order_summary",
            "name": "int_order_summary",
            "resource_type": "model",
            "package_name": "shop",
            "original_file_path": "models/intermediate/int_order_summary.sql",
            "depends_on": {"nodes": ["model.shop.stg_orders", "model.shop.stg_users"]},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "integer"},
                "user_name": {"name": "user_name", "data_type": "varchar"},
                "total_orders": {"name": "total_orders", "data_type": "integer"},
                "total_amount": {"name": "total_amount", "data_type": "numeric"},
            },
        },
        "model.shop.int_user_metrics": {
            "unique_id": "model.shop.int_user_metrics",
            "name": "int_user_metrics",
            "resource_type": "model",
            "package_name": "shop",
            "original_file_path": "models/intermediate/int_user_metrics.sql",
            "depends_on": {"nodes": ["model.shop.stg_users"]},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "integer"},
                "name": {"name": "name", "data_type": "varchar"},
                "phone": {"name": "phone", "data_type": "varchar"},
                "days_since_signup": {"name": "days_since_signup", "data_type": "integer"},
            },
        },
        "model.shop.mart_revenue": {
            "unique_id": "model.shop.mart_revenue",
            "name": "mart_revenue",
            "resource_type": "model",
            "package_name": "shop",
            "original_file_path": "models/marts/mart_revenue.sql",
            "depends_on": {"nodes": ["model.shop.int_order_summary"]},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "integer"},
                "total_revenue": {"name": "total_revenue", "data_type": "numeric"},
            },
        },
    },
    "sources": {
        "source.shop.raw.orders": {
            "unique_id": "source.shop.raw.orders",
            "name": "orders",
            "resource_type": "source",
            "source_name": "raw",
            "depends_on": {"nodes": []},
            "columns": {
                "order_id": {"name": "order_id", "data_type": "integer"},
                "user_id": {"name": "user_id", "data_type": "integer"},
                "amount": {"name": "amount", "data_type": "numeric"},
                "status": {"name": "status", "data_type": "varchar"},
            },
        },
        "source.shop.raw.users": {
            "unique_id": "source.shop.raw.users",
            "name": "users",
            "resource_type": "source",
            "source_name": "raw",
            "depends_on": {"nodes": []},
            "columns": {
                "user_id": {"name": "user_id", "data_type": "integer"},
                "name": {"name": "name", "data_type": "varchar"},
                "email": {"name": "email", "data_type": "varchar"},
                "phone": {"name": "phone", "data_type": "varchar"},
                "created_at": {"name": "created_at", "data_type": "timestamp"},
            },
        },
    },
    "exposures": {
        "exposure.shop.user_dashboard": {
            "unique_id": "exposure.shop.user_dashboard",
            "name": "user_dashboard",
            "type": "dashboard",
            "owner": {"name": "Analytics Team", "email": "analytics@example.com"},
            "url": "https://bi.example.com/user-dash",
            "depends_on": {
                "nodes": ["model.shop.int_user_metrics", "model.shop.mart_revenue"],
            },
        },
    },
    "child_map": {
        "source.shop.raw.orders": ["model.shop.stg_orders"],
        "source.shop.raw.users": ["model.shop.stg_users"],
        "model.shop.stg_orders": ["model.shop.int_order_summary"],
        "model.shop.stg_users": ["model.shop.int_order_summary", "model.shop.int_user_metrics"],
        "model.shop.int_order_summary": ["model.shop.mart_revenue"],
        "model.shop.int_user_metrics": [],
        "model.shop.mart_revenue": [],
    },
}

# Current manifest: phone column removed from stg_users
CURRENT_MANIFEST = json.loads(json.dumps(BASE_MANIFEST))
del CURRENT_MANIFEST["nodes"]["model.shop.stg_users"]["columns"]["phone"]
del CURRENT_MANIFEST["sources"]["source.shop.raw.users"]["columns"]["phone"]
# Also remove phone from int_user_metrics (it references it)
del CURRENT_MANIFEST["nodes"]["model.shop.int_user_metrics"]["columns"]["phone"]
CURRENT_MANIFEST["nodes"]["model.shop.int_user_metrics"]["columns"]["email"] = {
    "name": "email", "data_type": "varchar",
}

# Compiled SQL for downstream models
COMPILED_SQL = {
    "shop/models/intermediate/int_order_summary.sql": (
        "SELECT\n"
        "    u.user_id,\n"
        "    u.name AS user_name,\n"
        "    COUNT(o.order_id) AS total_orders,\n"
        "    SUM(o.amount) AS total_amount\n"
        "FROM stg_orders o\n"
        "JOIN stg_users u ON o.user_id = u.user_id\n"
        "GROUP BY u.user_id, u.name\n"
    ),
    "shop/models/intermediate/int_user_metrics.sql": (
        "SELECT\n"
        "    user_id,\n"
        "    name,\n"
        "    phone,\n"
        "    DATEDIFF(DAY, created_at, CURRENT_DATE) AS days_since_signup\n"
        "FROM stg_users\n"
    ),
    "shop/models/marts/mart_revenue.sql": (
        "SELECT\n"
        "    user_id,\n"
        "    total_amount AS total_revenue\n"
        "FROM int_order_summary\n"
    ),
}


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestRealisticE2E:
    def test_default_flags_detects_breaking(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Without v0.2 flags, detect the phone column removal as breaking."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            ["diff", "--base", str(base), "--current", str(current), "--format", "json"],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is True
        breaking_cols = [c["column"] for c in parsed["breaking_changes"]]
        assert "phone" in breaking_cols

    def test_model_level_impact_shows_false_positives(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Without --column-lineage, both int_order_summary and int_user_metrics are impacted."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            ["diff", "--base", str(base), "--current", str(current), "--format", "json"],
        )
        parsed = json.loads(result.output)
        impacted_names = [m["model_name"] for m in parsed["impacted_models"]]
        # Both are downstream of stg_users via BFS
        assert "int_order_summary" in impacted_names
        assert "int_user_metrics" in impacted_names

    def test_column_lineage_clears_false_positive(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """With --column-lineage, int_order_summary is cleared (doesn't use phone)."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--column-lineage",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        # int_order_summary should be cleared
        assert "int_order_summary" in parsed["cleared_models"]
        # int_user_metrics should still be impacted (references phone)
        impacted_names = [m["model_name"] for m in parsed["impacted_models"]]
        assert "int_user_metrics" in impacted_names
        # int_order_summary should NOT be in impacted list
        assert "int_order_summary" not in impacted_names

    def test_source_diff_detects_phone_removal(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--include-sources detects phone removal from raw.users source."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--include-sources",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        source_cols = [c["column"] for c in parsed["source_changes"] if c["is_breaking"]]
        assert "phone" in source_cols

    def test_exposure_impact_detected(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--include-exposures detects that user_dashboard is affected."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--include-exposures",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        exp_names = [e["name"] for e in parsed["exposure_impact"]]
        assert "user_dashboard" in exp_names

    def test_full_pipeline_all_flags(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Full pipeline with all v0.2 flags — everything works together."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--include-sources",
                "--include-exposures",
                "--column-lineage",
                "--warn-undocumented-sources",
                "--format", "json",
            ],
        )
        parsed = json.loads(result.output)
        assert parsed["has_breaking_changes"] is True

        # Breaking changes
        breaking_cols = {c["column"] for c in parsed["breaking_changes"]}
        assert "phone" in breaking_cols

        # Source changes
        assert parsed["summary"]["sources_changed"] >= 1

        # Column lineage: int_order_summary cleared
        assert "int_order_summary" in parsed["cleared_models"]

        # Exposure impact: user_dashboard affected
        assert parsed["summary"]["exposures_impacted"] >= 1

    def test_full_pipeline_text_output(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Full pipeline text output is human-readable and complete."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--include-sources",
                "--include-exposures",
                "--column-lineage",
            ],
        )
        assert result.exit_code == 1
        assert "BREAKING CHANGES" in result.output
        assert "phone" in result.output
        assert "SOURCE CHANGES" in result.output
        assert "CLEARED MODELS" in result.output
        assert "int_order_summary" in result.output
        assert "EXPOSURE IMPACT" in result.output
        assert "user_dashboard" in result.output

    def test_github_format_full_pipeline(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """GitHub Actions format includes all annotation types."""
        base = tmp_path / "base"
        current = tmp_path / "current"
        _write_manifest(base, BASE_MANIFEST)
        _write_manifest(current, CURRENT_MANIFEST, COMPILED_SQL)

        result = runner.invoke(
            main,
            [
                "diff",
                "--base", str(base),
                "--current", str(current),
                "--include-sources",
                "--include-exposures",
                "--column-lineage",
                "--format", "github",
            ],
        )
        assert "::error::" in result.output  # Breaking changes
        assert "::warning::" in result.output  # Exposure impact
        assert "::notice::" in result.output  # Cleared models
