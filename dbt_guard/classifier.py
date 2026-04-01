"""Breaking-change classification helpers.

The main classification logic lives in :mod:`dbt_guard.differ` (where
``is_breaking`` is set on each :class:`ColumnChange`).  This module provides
higher-level helpers for categorising an entire report, which are used by the
CLI and reporter.
"""

from __future__ import annotations

from typing import List, Tuple

from .models import ColumnChange


def partition_changes(
    changes: List[ColumnChange],
) -> Tuple[List[ColumnChange], List[ColumnChange]]:
    """Split *changes* into (breaking, non_breaking) lists."""
    breaking = [c for c in changes if c.is_breaking]
    non_breaking = [c for c in changes if not c.is_breaking]
    return breaking, non_breaking


def is_breaking(change: ColumnChange) -> bool:
    """Return whether *change* is considered a breaking change.

    Centralised so that future classification rules can be added in one place.

    Rules:
    - ``removed``     → breaking (consumers will fail)
    - ``renamed``     → breaking (consumer column references break)
    - ``type_changed``→ breaking (implicit casts may fail downstream)
    - ``added``       → non-breaking (additive, no consumer breaks)
    """
    return change.change_type in ("removed", "renamed", "type_changed")
