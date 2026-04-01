"""Column-level diff between two manifest states."""

from __future__ import annotations

from typing import Dict, List, Set

from .models import ColumnChange, ModelColumns


def diff_models(
    base_models: Dict[str, ModelColumns],
    current_models: Dict[str, ModelColumns],
) -> List[ColumnChange]:
    """Compare column inventories across two manifest states.

    Only models present in **both** manifests are diffed.  Models that are
    newly added in *current* are non-breaking additions; models removed from
    *current* are ignored (the pipeline may have been split, etc.).

    Returns a list of :class:`ColumnChange` objects, breaking changes first.
    """
    changes: List[ColumnChange] = []
    common_ids = set(base_models.keys()) & set(current_models.keys())

    for model_id in sorted(common_ids):
        base = base_models[model_id]
        current = current_models[model_id]
        changes.extend(_diff_model_columns(base, current))

    return changes


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _diff_model_columns(
    base: ModelColumns,
    current: ModelColumns,
) -> List[ColumnChange]:
    """Diff columns for a single model and return detected changes."""
    base_cols: Set[str] = set(base.columns.keys())
    current_cols: Set[str] = set(current.columns.keys())

    removed: Set[str] = base_cols - current_cols
    added: Set[str] = current_cols - base_cols
    common: Set[str] = base_cols & current_cols

    changes: List[ColumnChange] = []

    # --- Rename detection --------------------------------------------------
    # Heuristic: if exactly 1 column was removed AND 1 was added in the same
    # model, and their data types match (or both are undocumented), we treat
    # that as a rename rather than an independent removal + addition.
    unmatched_removed: Set[str] = set(removed)
    unmatched_added: Set[str] = set(added)

    if len(removed) == 1 and len(added) == 1:
        old_name = next(iter(removed))
        new_name = next(iter(added))
        old_col = base.columns[old_name]
        new_col = current.columns[new_name]
        # Types must match (including both-None) for us to call this a rename
        if old_col.data_type == new_col.data_type:
            changes.append(
                ColumnChange(
                    change_type="renamed",
                    model_id=base.model_id,
                    model_name=base.model_name,
                    column_name=old_name,
                    old_value=old_name,
                    new_value=new_name,
                    is_breaking=True,
                )
            )
            unmatched_removed.discard(old_name)
            unmatched_added.discard(new_name)

    # --- Removed columns (breaking) ----------------------------------------
    for col_name in sorted(unmatched_removed):
        changes.append(
            ColumnChange(
                change_type="removed",
                model_id=base.model_id,
                model_name=base.model_name,
                column_name=col_name,
                is_breaking=True,
            )
        )

    # --- Added columns (non-breaking) --------------------------------------
    for col_name in sorted(unmatched_added):
        changes.append(
            ColumnChange(
                change_type="added",
                model_id=base.model_id,
                model_name=base.model_name,
                column_name=col_name,
                is_breaking=False,
            )
        )

    # --- Type changes (breaking, only when documented on both sides) -------
    for col_name in sorted(common):
        base_col = base.columns[col_name]
        curr_col = current.columns[col_name]
        if (
            base_col.data_type
            and curr_col.data_type
            and base_col.data_type != curr_col.data_type
        ):
            changes.append(
                ColumnChange(
                    change_type="type_changed",
                    model_id=base.model_id,
                    model_name=base.model_name,
                    column_name=col_name,
                    old_value=base_col.data_type,
                    new_value=curr_col.data_type,
                    is_breaking=True,
                )
            )

    return changes
