"""Output formatters for dbt-guard diff reports.

Supports three formats:

- ``text``   — human-readable console output (default)
- ``json``   — machine-readable JSON (CI artifact, Slack bots, etc.)
- ``github`` — GitHub Actions annotation format (``::error::`` / ``::notice::``)
"""

from __future__ import annotations

import json as json_module
from typing import Dict, List

from .models import ColumnChange, DiffReport


def format_report(report: DiffReport, fmt: str = "text") -> str:
    """Render *report* in the requested format.

    Parameters
    ----------
    report:
        The completed diff report.
    fmt:
        One of ``"text"``, ``"json"``, or ``"github"``.

    Returns
    -------
    Formatted string ready for printing or writing to a file.
    """
    if fmt == "json":
        return _format_json(report)
    if fmt == "github":
        return _format_github(report)
    return _format_text(report)


# ---------------------------------------------------------------------------
# Text format
# ---------------------------------------------------------------------------


def _format_text(report: DiffReport) -> str:
    lines: List[str] = []
    lines.append("dbt-guard column lineage diff")
    lines.append("=" * 40)
    lines.append(f"Base:    {report.base_path}")
    lines.append(f"Current: {report.current_path}")
    lines.append("")

    if report.breaking_changes:
        lines.append(f"BREAKING CHANGES ({len(report.breaking_changes)})")
        lines.append("-" * 40)
        for model_name, changes in _group_by_model(report.breaking_changes):
            lines.append(f"\n{model_name}")
            for c in changes:
                if c.change_type == "removed":
                    lines.append(f"  REMOVED   {c.column_name}")
                elif c.change_type == "renamed":
                    lines.append(f"  RENAMED   {c.column_name} -> {c.new_value}")
                elif c.change_type == "type_changed":
                    lines.append(
                        f"  TYPE      {c.column_name}: {c.old_value} -> {c.new_value}"
                    )
    else:
        lines.append("No breaking changes detected.")

    if report.non_breaking_changes:
        lines.append(f"\nNON-BREAKING CHANGES ({len(report.non_breaking_changes)})")
        lines.append("-" * 40)
        for model_name, changes in _group_by_model(report.non_breaking_changes):
            lines.append(f"\n{model_name}")
            for c in changes:
                lines.append(f"  ADDED     {c.column_name}")

    if report.impacted_models:
        lines.append(f"\nDOWNSTREAM IMPACT ({len(report.impacted_models)} models)")
        lines.append("-" * 40)
        for m in report.impacted_models:
            lines.append(f"  {m.model_name} (distance: {m.distance})")

    # --- v0.2 sections (only when data exists) ---

    if report.source_changes:
        breaking_src = [c for c in report.source_changes if c.is_breaking]
        non_breaking_src = [c for c in report.source_changes if not c.is_breaking]

        if breaking_src:
            lines.append(f"\nSOURCE CHANGES — BREAKING ({len(breaking_src)})")
            lines.append("-" * 40)
            for model_name, changes in _group_by_model(breaking_src):
                lines.append(f"\n{model_name}")
                for c in changes:
                    if c.change_type == "removed":
                        lines.append(f"  REMOVED   {c.column_name}")
                    elif c.change_type == "renamed":
                        lines.append(f"  RENAMED   {c.column_name} -> {c.new_value}")
                    elif c.change_type == "type_changed":
                        lines.append(
                            f"  TYPE      {c.column_name}: {c.old_value} -> {c.new_value}"
                        )

        if non_breaking_src:
            lines.append(f"\nSOURCE CHANGES — NON-BREAKING ({len(non_breaking_src)})")
            lines.append("-" * 40)
            for model_name, changes in _group_by_model(non_breaking_src):
                lines.append(f"\n{model_name}")
                for c in changes:
                    lines.append(f"  ADDED     {c.column_name}")

    if report.column_lineage_impacts:
        impacted_entries = [i for i in report.column_lineage_impacts if not i.cleared]
        if impacted_entries:
            lines.append(f"\nCOLUMN LINEAGE IMPACT ({len(impacted_entries)} models)")
            lines.append("-" * 40)
            for impact in impacted_entries:
                lines.append(f"\n{impact.model_name}")
                for ic in impact.impacted_columns:
                    lines.append(f"  {ic.column_name}: {ic.reason}")
                    if ic.chain:
                        chain_str = " -> ".join(
                            f"{link.model_name}.{link.column_name}"
                            for link in ic.chain
                        )
                        lines.append(f"    chain: {chain_str}")

    if report.cleared_models:
        lines.append(f"\nCLEARED MODELS ({len(report.cleared_models)})")
        lines.append("-" * 40)
        for name in report.cleared_models:
            lines.append(f"  {name} (no column-level dependency on changed columns)")

    if report.impacted_exposures:
        lines.append(f"\nEXPOSURE IMPACT ({len(report.impacted_exposures)} exposures)")
        lines.append("-" * 40)
        for exp in report.impacted_exposures:
            lines.append(f"\n{exp.name} ({exp.type})")
            if exp.owner_name or exp.owner_email:
                owner_parts = [p for p in [exp.owner_name, exp.owner_email] if p]
                lines.append(f"  Owner: {', '.join(owner_parts)}")
            if exp.url:
                lines.append(f"  URL: {exp.url}")
            lines.append(f"  Impacted models: {', '.join(exp.impacted_models)}")
            if exp.impacted_columns:
                for model_name, cols in sorted(exp.impacted_columns.items()):
                    lines.append(f"    {model_name}: {', '.join(cols)}")

    if report.undocumented_sources:
        lines.append(f"\nWARNINGS ({len(report.undocumented_sources)})")
        lines.append("-" * 40)
        for src_id in report.undocumented_sources:
            lines.append(f"  Undocumented source: {src_id}")

    lines.append("")
    if report.has_breaking_changes:
        lines.append("Result: FAIL — breaking changes detected")
    else:
        lines.append("Result: PASS — no breaking changes")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON format
# ---------------------------------------------------------------------------


def _format_json(report: DiffReport) -> str:
    data = {
        "base": report.base_path,
        "current": report.current_path,
        "has_breaking_changes": report.has_breaking_changes,
        "summary": {
            "breaking": len(report.breaking_changes),
            "non_breaking": len(report.non_breaking_changes),
            "impacted_models": len(report.impacted_models),
            "sources_changed": len(report.source_changes),
            "models_cleared": len(report.cleared_models),
            "exposures_impacted": len(report.impacted_exposures),
        },
        "breaking_changes": [
            {
                "model": c.model_name,
                "model_id": c.model_id,
                "type": c.change_type,
                "column": c.column_name,
                "old_value": c.old_value,
                "new_value": c.new_value,
            }
            for c in report.breaking_changes
        ],
        "non_breaking_changes": [
            {
                "model": c.model_name,
                "type": c.change_type,
                "column": c.column_name,
            }
            for c in report.non_breaking_changes
        ],
        "impacted_models": [
            {
                "model_id": m.model_id,
                "model_name": m.model_name,
                "distance": m.distance,
            }
            for m in report.impacted_models
        ],
        "source_changes": [
            {
                "source": c.model_name,
                "source_id": c.model_id,
                "type": c.change_type,
                "column": c.column_name,
                "old_value": c.old_value,
                "new_value": c.new_value,
                "is_breaking": c.is_breaking,
            }
            for c in report.source_changes
        ],
        "column_lineage_impact": [
            {
                "model_id": impact.model_id,
                "model_name": impact.model_name,
                "cleared": impact.cleared,
                "impacted_columns": [
                    {
                        "column": ic.column_name,
                        "reason": ic.reason,
                        "chain": [
                            {
                                "model_id": link.model_id,
                                "model_name": link.model_name,
                                "column": link.column_name,
                            }
                            for link in ic.chain
                        ],
                    }
                    for ic in impact.impacted_columns
                ],
            }
            for impact in report.column_lineage_impacts
        ],
        "cleared_models": report.cleared_models,
        "exposure_impact": [
            {
                "exposure_id": exp.exposure_id,
                "name": exp.name,
                "type": exp.type,
                "owner_name": exp.owner_name,
                "owner_email": exp.owner_email,
                "url": exp.url,
                "impacted_models": exp.impacted_models,
                "impacted_columns": exp.impacted_columns,
            }
            for exp in report.impacted_exposures
        ],
        "undocumented_sources": report.undocumented_sources,
    }
    return json_module.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# GitHub Actions format
# ---------------------------------------------------------------------------


def _format_github(report: DiffReport) -> str:
    """Emit GitHub Actions workflow commands.

    Each breaking change becomes an ``::error::`` annotation.
    When there are no breaking changes, a ``::notice::`` is emitted.
    """
    lines: List[str] = []
    n_impacted = len(report.impacted_models)

    for c in report.breaking_changes:
        if c.change_type == "removed":
            msg = f"Column '{c.column_name}' removed from {c.model_name}"
        elif c.change_type == "renamed":
            msg = f"Column '{c.column_name}' renamed to '{c.new_value}' in {c.model_name}"
        elif c.change_type == "type_changed":
            msg = (
                f"Column '{c.column_name}' type changed "
                f"{c.old_value} -> {c.new_value} in {c.model_name}"
            )
        else:
            msg = f"Breaking change in {c.model_name}: {c.column_name}"

        if n_impacted:
            msg += f" ({n_impacted} downstream model(s) potentially affected)"

        lines.append(f"::error::{msg}")

    # Source breaking changes
    for c in report.source_changes:
        if c.is_breaking:
            if c.change_type == "removed":
                msg = f"Source column '{c.column_name}' removed from {c.model_name}"
            elif c.change_type == "type_changed":
                msg = (
                    f"Source column '{c.column_name}' type changed "
                    f"{c.old_value} -> {c.new_value} in {c.model_name}"
                )
            else:
                msg = f"Source breaking change in {c.model_name}: {c.column_name}"
            lines.append(f"::error::{msg}")

    # Exposure impact warnings
    for exp in report.impacted_exposures:
        msg = (
            f"Exposure '{exp.name}' ({exp.type}) affected — "
            f"impacted models: {', '.join(exp.impacted_models)}"
        )
        if exp.owner_name:
            msg += f" (owner: {exp.owner_name})"
        lines.append(f"::warning::{msg}")

    # Cleared models notices
    for name in report.cleared_models:
        lines.append(
            f"::notice::Model '{name}' cleared by column-level lineage analysis"
        )

    if not report.breaking_changes and not any(
        c.is_breaking for c in report.source_changes
    ):
        lines.append("::notice::dbt-guard: no breaking column changes detected")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _group_by_model(
    changes: List[ColumnChange],
) -> List[tuple]:
    """Return changes grouped by model_name, sorted alphabetically."""
    grouped: Dict[str, List[ColumnChange]] = {}
    for c in changes:
        grouped.setdefault(c.model_name, []).append(c)
    return sorted(grouped.items())
