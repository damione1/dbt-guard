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

    if not report.breaking_changes:
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
