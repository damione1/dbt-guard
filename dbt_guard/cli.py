"""Command-line interface for dbt-guard."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import click

from . import __version__
from .differ import diff_models
from .exceptions import DbtGuardError
from .impact import find_impacted_exposures, find_impacted_models
from .lineage import extract_columns_from_sql
from .manifest import load_manifest
from .models import ColumnChange, ColumnInfo, ColumnLineageImpact, DiffReport
from .reporter import format_report
from .resolver import resolve_column_lineage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline logic (testable without CLI)
# ---------------------------------------------------------------------------


@dataclass
class PipelineConfig:
    """Typed configuration for the diff pipeline."""

    base: Path
    current: Path
    dialect: Optional[str] = None
    no_impact: bool = False
    max_depth: int = 10
    select_models: Tuple[str, ...] = ()
    include_sources: bool = False
    include_exposures: bool = False
    include_snapshots: bool = False
    column_lineage: bool = False
    strict_lineage: bool = False
    warn_undocumented_sources: bool = False


def run_pipeline(config: PipelineConfig) -> DiffReport:
    """Execute the diff pipeline and return a :class:`DiffReport`.

    This is the core orchestration function, separated from CLI concerns
    so it can be called programmatically and tested independently.
    """
    # Load both manifests
    base_data = load_manifest(
        config.base,
        include_sources=config.include_sources,
        include_snapshots=config.include_snapshots,
        include_exposures=config.include_exposures,
        warn_undocumented_sources=config.warn_undocumented_sources,
    )
    current_data = load_manifest(
        config.current,
        include_sources=config.include_sources,
        include_snapshots=config.include_snapshots,
        include_exposures=config.include_exposures,
        warn_undocumented_sources=config.warn_undocumented_sources,
    )

    base_models = base_data.models
    current_models = current_data.models
    curr_child_map = current_data.child_map

    # Enrich column lists from compiled SQL where available
    _enrich_from_sql(base_models, config.dialect)
    _enrich_from_sql(current_models, config.dialect)

    # Apply --select filter (filter by model name, not unique_id)
    if config.select_models:
        select_set = set(config.select_models)
        base_models = {k: v for k, v in base_models.items() if v.model_name in select_set}
        current_models = {
            k: v for k, v in current_models.items() if v.model_name in select_set
        }

    # Compute column diff for models
    all_changes = diff_models(base_models, current_models)
    breaking = [c for c in all_changes if c.is_breaking]
    non_breaking = [c for c in all_changes if not c.is_breaking]

    # Compute source diff
    source_changes: List[ColumnChange] = []
    if config.include_sources:
        source_changes = diff_models(base_data.sources, current_data.sources)

    # Downstream impact analysis
    impacted = []
    breaking_source_changes = [c for c in source_changes if c.is_breaking]
    seed_ids = list({c.model_id for c in breaking})
    # Include source breaking changes in BFS seed set
    if config.include_sources and breaking_source_changes:
        seed_ids.extend({c.model_id for c in breaking_source_changes})
        seed_ids = list(set(seed_ids))
    if not config.no_impact and seed_ids:
        impacted = find_impacted_models(
            seed_ids, curr_child_map, config.max_depth, all_models=current_data.models
        )

    # Column-level lineage resolution
    column_lineage_impacts: List[ColumnLineageImpact] = []
    cleared_models: List[str] = []
    if config.column_lineage and breaking:
        changed_cols: Dict[str, Set[str]] = {}
        for c in breaking:
            changed_cols.setdefault(c.model_id, set()).add(c.column_name)

        column_lineage_impacts, cleared_ids = resolve_column_lineage(
            changed_columns=changed_cols,
            child_map=curr_child_map,
            all_models=current_data.models,
            dialect=config.dialect,
            max_depth=config.max_depth,
            strict=config.strict_lineage,
        )

        # Remove cleared models from impacted list
        cleared_set = set(cleared_ids)
        cleared_models = [
            m.model_name for m in impacted if m.model_id in cleared_set
        ]
        impacted = [m for m in impacted if m.model_id not in cleared_set]

    # Exposure impact analysis
    impacted_exposures = []
    if config.include_exposures and current_data.exposures:
        impacted_model_ids = {m.model_id for m in impacted}
        changed_model_ids = {c.model_id for c in breaking}
        impacted_exposures = find_impacted_exposures(
            impacted_model_ids=impacted_model_ids,
            changed_model_ids=changed_model_ids,
            exposures=current_data.exposures,
            breaking_changes=breaking,
            column_lineage_impacts=column_lineage_impacts or None,
            all_models=current_data.models,
        )

    # Undocumented sources (merge from both manifests)
    undocumented = sorted(
        set(base_data.undocumented_sources) | set(current_data.undocumented_sources)
    )

    return DiffReport(
        base_path=str(config.base),
        current_path=str(config.current),
        breaking_changes=breaking,
        non_breaking_changes=non_breaking,
        impacted_models=impacted,
        source_changes=source_changes,
        column_lineage_impacts=column_lineage_impacts,
        cleared_models=cleared_models,
        impacted_exposures=impacted_exposures,
        undocumented_sources=undocumented,
    )


# ---------------------------------------------------------------------------
# CLI entry points
# ---------------------------------------------------------------------------


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
@click.option(
    "--include-sources",
    is_flag=True,
    default=False,
    help="Include dbt sources in the diff analysis.",
)
@click.option(
    "--include-exposures",
    is_flag=True,
    default=False,
    help="Include dbt exposures in impact analysis.",
)
@click.option(
    "--include-snapshots",
    is_flag=True,
    default=False,
    help="Include dbt snapshots in the diff analysis.",
)
@click.option(
    "--column-lineage",
    is_flag=True,
    default=False,
    help="Enable column-level lineage resolution to reduce false positives.",
)
@click.option(
    "--strict-lineage",
    is_flag=True,
    default=False,
    help="Fail if compiled SQL is missing for any impacted model (requires --column-lineage).",
)
@click.option(
    "--warn-undocumented-sources",
    is_flag=True,
    default=False,
    help="Warn about sources with no documented columns.",
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
    include_sources: bool,
    include_exposures: bool,
    include_snapshots: bool,
    column_lineage: bool,
    strict_lineage: bool,
    warn_undocumented_sources: bool,
) -> None:
    """Diff column-level lineage between two dbt manifest states."""
    try:
        if strict_lineage and not column_lineage:
            raise click.UsageError("--strict-lineage requires --column-lineage")

        config = PipelineConfig(
            base=base,
            current=current,
            dialect=None if dialect == "default" else dialect,
            no_impact=no_impact,
            max_depth=max_depth,
            select_models=select_models,
            include_sources=include_sources,
            include_exposures=include_exposures,
            include_snapshots=include_snapshots,
            column_lineage=column_lineage,
            strict_lineage=strict_lineage,
            warn_undocumented_sources=warn_undocumented_sources,
        )
        report = run_pipeline(config)
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
                    f"{status}: {len(report.breaking_changes)} breaking, "
                    f"{len(report.non_breaking_changes)} non-breaking, "
                    f"{len(report.impacted_models)} impacted"
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
        sql: Optional[str] = model.compiled_sql
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
