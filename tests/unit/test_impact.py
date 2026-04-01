"""Unit tests for dbt_guard.impact."""

from __future__ import annotations

import pytest

from dbt_guard.impact import find_impacted_models
from dbt_guard.models import ImpactedModel

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

# Simple linear chain: A -> B -> C -> D
LINEAR_CHILD_MAP = {
    "model.pkg.A": ["model.pkg.B"],
    "model.pkg.B": ["model.pkg.C"],
    "model.pkg.C": ["model.pkg.D"],
    "model.pkg.D": [],
}

# Diamond DAG: A -> B, A -> C, B -> D, C -> D
DIAMOND_CHILD_MAP = {
    "model.pkg.A": ["model.pkg.B", "model.pkg.C"],
    "model.pkg.B": ["model.pkg.D"],
    "model.pkg.C": ["model.pkg.D"],
    "model.pkg.D": [],
}


# ---------------------------------------------------------------------------
# Tests: single hop
# ---------------------------------------------------------------------------


class TestSingleHop:
    def test_direct_child_at_distance_one(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP)
        distances = {m.model_id: m.distance for m in impacted}
        assert distances.get("model.pkg.B") == 1

    def test_changed_model_not_in_results(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP)
        ids = {m.model_id for m in impacted}
        assert "model.pkg.A" not in ids

    def test_model_name_derived_from_id(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP)
        model_b = next(m for m in impacted if m.model_id == "model.pkg.B")
        assert model_b.model_name == "B"


# ---------------------------------------------------------------------------
# Tests: two hops
# ---------------------------------------------------------------------------


class TestTwoHops:
    def test_two_hop_chain(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP)
        distances = {m.model_id: m.distance for m in impacted}
        assert distances.get("model.pkg.B") == 1
        assert distances.get("model.pkg.C") == 2
        assert distances.get("model.pkg.D") == 3

    def test_sorted_by_distance_then_id(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP)
        dist_list = [m.distance for m in impacted]
        assert dist_list == sorted(dist_list)


# ---------------------------------------------------------------------------
# Tests: max_depth
# ---------------------------------------------------------------------------


class TestMaxDepth:
    def test_max_depth_one_stops_at_first_hop(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP, max_depth=1)
        ids = {m.model_id for m in impacted}
        assert "model.pkg.B" in ids
        assert "model.pkg.C" not in ids
        assert "model.pkg.D" not in ids

    def test_max_depth_two(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], LINEAR_CHILD_MAP, max_depth=2)
        ids = {m.model_id for m in impacted}
        assert "model.pkg.B" in ids
        assert "model.pkg.C" in ids
        assert "model.pkg.D" not in ids


# ---------------------------------------------------------------------------
# Tests: diamond DAG (shortest path)
# ---------------------------------------------------------------------------


class TestDiamondDag:
    def test_diamond_d_at_distance_two(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], DIAMOND_CHILD_MAP)
        distances = {m.model_id: m.distance for m in impacted}
        # D is reachable via A->B->D (2) or A->C->D (2) — both give distance 2
        assert distances.get("model.pkg.D") == 2

    def test_diamond_no_duplicates(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], DIAMOND_CHILD_MAP)
        ids = [m.model_id for m in impacted]
        assert len(ids) == len(set(ids)), "Each model should appear at most once"


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_model_not_in_child_map_returns_empty(self) -> None:
        impacted = find_impacted_models(["model.pkg.unknown"], LINEAR_CHILD_MAP)
        assert impacted == []

    def test_empty_changed_list_returns_empty(self) -> None:
        impacted = find_impacted_models([], LINEAR_CHILD_MAP)
        assert impacted == []

    def test_empty_child_map_returns_empty(self) -> None:
        impacted = find_impacted_models(["model.pkg.A"], {})
        assert impacted == []

    def test_multiple_changed_models(self) -> None:
        # Both A and B changed; C, D should still be found but not A or B
        impacted = find_impacted_models(["model.pkg.A", "model.pkg.B"], LINEAR_CHILD_MAP)
        ids = {m.model_id for m in impacted}
        assert "model.pkg.A" not in ids
        assert "model.pkg.B" not in ids
        assert "model.pkg.C" in ids
        assert "model.pkg.D" in ids

    def test_leaf_node_returns_empty(self) -> None:
        impacted = find_impacted_models(["model.pkg.D"], LINEAR_CHILD_MAP)
        assert impacted == []
