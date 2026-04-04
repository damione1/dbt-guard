"""Downstream impact analysis via BFS over the dbt DAG child_map."""

from __future__ import annotations

from collections import deque
from typing import Dict, List, Optional, Set

from .models import (
    ColumnChange,
    ColumnLineageImpact,
    ExposureInfo,
    ImpactedExposure,
    ImpactedModel,
    ModelColumns,
)


def find_impacted_models(
    changed_model_ids: List[str],
    child_map: Dict[str, List[str]],
    max_depth: int = 10,
    all_models: Optional[Dict[str, "ModelColumns"]] = None,
) -> List[ImpactedModel]:
    """Return all downstream models reachable from *changed_model_ids*.

    Uses breadth-first search over *child_map*.  Each returned
    :class:`ImpactedModel` carries the shortest BFS distance (hop count) from
    any directly-changed model.

    Parameters
    ----------
    changed_model_ids:
        The set of model unique_ids that have breaking column changes.
    child_map:
        Adjacency list from the manifest: ``{parent_id: [child_id, ...]}``.
        Must already be filtered to model/seed nodes only (tests excluded).
    max_depth:
        Maximum BFS depth.  Nodes beyond this hop count are not reported.

    Returns
    -------
    List of :class:`ImpactedModel`, ordered by (distance, model_id).
    The directly-changed models themselves are **not** included.
    """
    changed_set: Set[str] = set(changed_model_ids)

    # visited maps model_id -> minimum BFS distance from any changed model
    visited: Dict[str, int] = {}
    queue: deque = deque()

    # Seed the BFS with direct children of all changed models
    for model_id in changed_model_ids:
        for child_id in child_map.get(model_id, []):
            if child_id not in changed_set and child_id not in visited:
                visited[child_id] = 1
                queue.append((child_id, 1))

    while queue:
        node_id, depth = queue.popleft()
        if depth >= max_depth:
            continue
        for child_id in child_map.get(node_id, []):
            if child_id not in visited and child_id not in changed_set:
                visited[child_id] = depth + 1
                queue.append((child_id, depth + 1))

    def _model_name(mid: str) -> str:
        if all_models and mid in all_models:
            return all_models[mid].model_name
        return mid.split(".")[-1]

    return [
        ImpactedModel(
            model_id=mid,
            model_name=_model_name(mid),
            distance=dist,
        )
        for mid, dist in sorted(visited.items(), key=lambda kv: (kv[1], kv[0]))
    ]


def find_impacted_exposures(
    impacted_model_ids: Set[str],
    changed_model_ids: Set[str],
    exposures: Dict[str, ExposureInfo],
    breaking_changes: Optional[List[ColumnChange]] = None,
    column_lineage_impacts: Optional[List[ColumnLineageImpact]] = None,
    all_models: Optional[Dict[str, ModelColumns]] = None,
) -> List[ImpactedExposure]:
    """Find exposures affected by breaking changes in their upstream models.

    For each exposure, checks whether any of its ``depends_on_nodes`` overlap
    with the set of changed or transitively impacted models. If so, builds an
    :class:`ImpactedExposure` with affected model names and (when available)
    impacted column names.
    """
    all_affected = impacted_model_ids | changed_model_ids

    # Build lookup: model_id -> model_name
    id_to_name: Dict[str, str] = {}
    for mid in all_affected:
        if all_models and mid in all_models:
            id_to_name[mid] = all_models[mid].model_name
        else:
            id_to_name[mid] = mid.split(".")[-1]

    # Build lookup: model_id -> list of breaking column names
    breaking_cols_by_model: Dict[str, List[str]] = {}
    if breaking_changes:
        for change in breaking_changes:
            breaking_cols_by_model.setdefault(change.model_id, []).append(
                change.column_name
            )

    # Build lookup from column lineage impacts
    lineage_cols_by_model: Dict[str, List[str]] = {}
    if column_lineage_impacts:
        for impact in column_lineage_impacts:
            if not impact.cleared:
                lineage_cols_by_model[impact.model_id] = [
                    ic.column_name for ic in impact.impacted_columns
                ]

    result: List[ImpactedExposure] = []

    for exp_id, exposure in sorted(exposures.items()):
        dep_nodes = set(exposure.depends_on_nodes)
        overlap = dep_nodes & all_affected
        if not overlap:
            continue

        impacted_model_names = sorted(id_to_name.get(mid, mid) for mid in overlap)

        # Gather column-level detail where available
        impacted_columns: Dict[str, List[str]] = {}
        for mid in overlap:
            model_name = id_to_name.get(mid, mid)
            cols: List[str] = []
            if mid in breaking_cols_by_model:
                cols.extend(breaking_cols_by_model[mid])
            if mid in lineage_cols_by_model:
                cols.extend(
                    c for c in lineage_cols_by_model[mid] if c not in cols
                )
            if cols:
                impacted_columns[model_name] = cols

        result.append(
            ImpactedExposure(
                exposure_id=exp_id,
                name=exposure.name,
                type=exposure.type,
                owner_name=exposure.owner_name,
                owner_email=exposure.owner_email,
                url=exposure.url,
                impacted_models=impacted_model_names,
                impacted_columns=impacted_columns,
            )
        )

    return result
