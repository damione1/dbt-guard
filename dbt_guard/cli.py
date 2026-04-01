"""Command-line interface for dbt-guard."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Tuple

import click

from . import __version__
from .differ import diff_models
from .exceptions import DbtGuardError
from .impact import find_impacted_models
from .lineage import extract_columns_from_sql
from .manifest import load_manifest
from .models import ColumnInfo, DiffReport
from .reporter import format_report

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version=__version__, prog_name="dbt-guard")
@click.option("--debug", is_flag=True, hidden=True, help="Enable debug logging.")
@click.pass_context
def main(ctx: click.Context, debug: bool) -> None:
    """dbt-guard: column-level lineage breaking change detection for dbt Core."""
    level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug


@main.command()
@click.option(
    "--base",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing the base manifest.json (e.g. target/ from the main branch).",
)
@click.option(
    "--current",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Directory containing the current manifest.json (e.g. target/ from the PR branch).",
)
@click.option(
    "--dialect",
    default="default",
    type=click.Choice(
        ["default", "snowflake", "bigquery", "databricks", "redshift", "trino"],
        case_sensitive=False,
    ),
    show_default=True,
    help="SQL dialect for SQLGlot parsing.",
)
@click.option(
    "--format",
    "fmt",
    default="text",
    type=click.Choice(["text", "json", "github"], case_sensitive=False),
    show_default=True,
    help="Output format.",
)
@click.option(
    "--fail-on",
    "fail_on",
    default="breaking",
    type=click.Choice(["breaking", "any", "never"], case_sensitive=False),
    show_default=True,
    help=(
        "When to exit with a non-zero code. "
        "'breaking' (default): on breaking changes only. "
        "'any': on any column change. "
        "'never': always exit 0."
    ),
)
@click.option(
    "--no-impact",
    is_flag=True,
    default=False,
    help="Skip downstream impact analysis (faster for large DAGs).",
)
@click.option(
    "--max-depth",
    default=10,
    type=click.IntRange(1, 50),
    show_default=True,
    help="Maximum DAG hops for downstream impact traversal.",
)
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    default=None,
    help="Write the report to a file instead of stdout.",
)
@click.option(
    "--select",
    "select_models",
    multiple=True,
    metavar="MODEL",
    help="Limit the diff to specific model names (repeatable). "
    "Example: --select DT_PAYMENTS --select DT_CLIENTS",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Print only a one-line summary instead of the full report.",
)
def diff(
    base: Path,
    current: Path,
    dialect: str,
    fmt: str,
    fail_on: str,
    no_impact: bool,
    max_depth: int,
    output: Optional[Path],
    select_models: Tuple[str, ...],
    quiet: bool,
) -> None:
    """Diff column-level lineage between two dbt manifest states."""
    try:
        sql_dialect = None if dialect == "default" else dialect

        # Load both manifests
        base_models, _base_child_map = load_manifest(base)
        current_models, curr_child_map = load_manifest(current)

        # Enrich column lists from compiled SQL where available
        _enrich_from_sql(base_models, sql_dialect)
        _enrich_from_sql(current_models, sql_dialect)

        # Apply --select filter (filter by model name, not unique_id)
        if select_models:
            select_set = set(select_models)
            base_models = {k: v for k, v in base_models.items() if v.model_name in select_set}
            current_models = {
                k: v for k, v in current_models.items() if v.model_name in select_set
            }

        # Compute column diff
        all_changes = diff_models(base_models, current_models)
        breaking = [c for c in all_changes if c.is_breaking]
        non_breaking = [c for c in all_changes if not c.is_breaking]

        # Downstream impact analysis
        impacted = []
        if not no_impact and breaking:
            changed_ids = list({c.model_id for c in breaking})
            impacted = find_impacted_models(changed_ids, curr_child_map, max_depth)

        report = DiffReport(
            base_path=str(base),
            current_path=str(current),
            breaking_changes=breaking,
            non_breaking_changes=non_breaking,
            impacted_models=impacted,
        )

        formatted = format_report(report, fmt)

        # Output
        if output:
            output.write_text(formatted, encoding="utf-8")
            if not quiet:
                click.echo(f"Report written to {output}")
        else:
            if quiet:
                status = "FAIL" if report.has_breaking_changes else "PASS"
                click.echo(
                    f"{status}: {len(breaking)} breaking, "
                    f"{len(non_breaking)} non-breaking, "
                    f"{len(impacted)} impacted"
                )
            else:
                click.echo(formatted)

        # Exit code
        if fail_on == "never":
            sys.exit(0)
        elif fail_on == "any" and report.total_changes > 0:
            sys.exit(1)
        elif fail_on == "breaking" and report.has_breaking_changes:
            sys.exit(1)
        sys.exit(0)

    except DbtGuardError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"Unexpected error: {exc}", err=True)
        if logger.isEnabledFor(logging.DEBUG):
            import traceback

            traceback.print_exc()
        sys.exit(2)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _enrich_from_sql(
    models: dict,
    dialect: Optional[str],
) -> None:
    """Enrich model column dictionaries from compiled SQL when available.

    If SQLGlot successfully extracts a column list from the compiled SQL, we
    merge it with the documented columns: SQL-derived columns without any
    schema.yml entry get a bare :class:`ColumnInfo` with ``data_type=None``.
    SQL-derived columns that ARE documented keep their ``data_type``.

    This means undocumented columns appear in diffs, which is the whole point
    of the tool.
    """
    for model in models.values():
        sql: Optional[str] = model._compiled_sql
        if not sql:
            continue

        dialect_arg = dialect or "default"
        sql_cols = extract_columns_from_sql(sql, dialect_arg)
        if not sql_cols:
            continue

        # Merge: preserve documented metadata, add undocumented entries
        merged = {}
        for col_name in sql_cols:
            if col_name in model.columns:
                merged[col_name] = model.columns[col_name]
            else:
                merged[col_name] = ColumnInfo(name=col_name, data_type=None)

        model.columns = merged
        model.has_compiled_sql = True
