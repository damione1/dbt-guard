"""Downstream impact analysis via BFS over the dbt DAG child_map."""

from __future__ import annotations

from collections import deque
from typing import Dict, List, Set

from .models import ImpactedModel


def find_impacted_models(
    changed_model_ids: List[str],
    child_map: Dict[str, List[str]],
    max_depth: int = 10,
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

    return [
        ImpactedModel(
            model_id=mid,
            model_name=mid.split(".")[-1],
            distance=dist,
        )
        for mid, dist in sorted(visited.items(), key=lambda kv: (kv[1], kv[0]))
    ]
