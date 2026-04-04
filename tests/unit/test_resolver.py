"""Unit tests for dbt_guard.resolver — column-level lineage resolution."""

from __future__ import annotations

import pytest

from dbt_guard.exceptions import ColumnLineageError
from dbt_guard.models import ColumnInfo, ModelColumns
from dbt_guard.resolver import resolve_column_lineage


def _make_model(
    uid: str,
    name: str,
    cols: dict,
    compiled_sql: str | None = None,
) -> ModelColumns:
    return ModelColumns(
        model_id=uid,
        model_name=name,
        columns={
            k.lower(): ColumnInfo(name=k.lower(), data_type=v)
            for k, v in cols.items()
        },
        has_compiled_sql=compiled_sql is not None,
        _compiled_sql=compiled_sql,
    )


class TestResolveColumnLineage:
    def test_basic_trace_impacted(self) -> None:
        """model_b references model_a.email → should be impacted."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar", "status": "varchar"},
        )
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer", "email": "varchar"},
            compiled_sql="SELECT a.id, a.email FROM model_a a",
        )

        all_models = {"model.pkg.a": model_a, "model.pkg.b": model_b}
        child_map = {"model.pkg.a": ["model.pkg.b"]}

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        assert len(impacts) == 1
        assert impacts[0].model_id == "model.pkg.b"
        assert not impacts[0].cleared
        impacted_col_names = {ic.column_name for ic in impacts[0].impacted_columns}
        assert "email" in impacted_col_names
        assert cleared == []

    def test_clearing_false_positive(self) -> None:
        """model_c does NOT reference email → should be cleared."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar", "status": "varchar"},
        )
        model_c = _make_model(
            "model.pkg.c", "model_c",
            {"id": "integer", "status": "varchar"},
            compiled_sql="SELECT a.id, a.status FROM model_a a",
        )

        all_models = {"model.pkg.a": model_a, "model.pkg.c": model_c}
        child_map = {"model.pkg.a": ["model.pkg.c"]}

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        assert len(impacts) == 1
        assert impacts[0].model_id == "model.pkg.c"
        assert impacts[0].cleared is True
        assert impacts[0].impacted_columns == []
        assert "model.pkg.c" in cleared

    def test_missing_compiled_sql_fallback(self) -> None:
        """Without compiled SQL and strict=False → assume impacted."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar"},
        )
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer"},
            compiled_sql=None,
        )

        all_models = {"model.pkg.a": model_a, "model.pkg.b": model_b}
        child_map = {"model.pkg.a": ["model.pkg.b"]}

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
            strict=False,
        )

        assert len(impacts) == 1
        assert not impacts[0].cleared
        assert impacts[0].impacted_columns[0].column_name == "*"
        assert cleared == []

    def test_strict_mode_raises_on_missing_sql(self) -> None:
        """With strict=True and missing compiled SQL → raise."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar"},
        )
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer"},
            compiled_sql=None,
        )

        all_models = {"model.pkg.a": model_a, "model.pkg.b": model_b}
        child_map = {"model.pkg.a": ["model.pkg.b"]}

        with pytest.raises(ColumnLineageError, match="Compiled SQL missing"):
            resolve_column_lineage(
                changed_columns={"model.pkg.a": {"email"}},
                child_map=child_map,
                all_models=all_models,
                strict=True,
            )

    def test_no_children_produces_empty_results(self) -> None:
        """Changed model with no children → no impacts or cleared."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar"},
        )

        all_models = {"model.pkg.a": model_a}
        child_map: dict = {}

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        assert impacts == []
        assert cleared == []

    def test_multi_hop_propagation(self) -> None:
        """Changed column propagates through multiple hops: a→b→c."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar"},
        )
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer", "email": "varchar"},
            compiled_sql="SELECT a.id, a.email FROM model_a a",
        )
        model_c = _make_model(
            "model.pkg.c", "model_c",
            {"id": "integer", "email": "varchar"},
            compiled_sql="SELECT b.id, b.email FROM model_b b",
        )

        all_models = {
            "model.pkg.a": model_a,
            "model.pkg.b": model_b,
            "model.pkg.c": model_c,
        }
        child_map = {
            "model.pkg.a": ["model.pkg.b"],
            "model.pkg.b": ["model.pkg.c"],
        }

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        impacted_ids = {i.model_id for i in impacts if not i.cleared}
        assert "model.pkg.b" in impacted_ids
        assert "model.pkg.c" in impacted_ids
        assert cleared == []

    def test_cleared_model_stops_propagation(self) -> None:
        """If model_b doesn't reference the changed column, model_c (child of b) should not be visited."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar", "status": "varchar"},
        )
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer", "status": "varchar"},
            compiled_sql="SELECT a.id, a.status FROM model_a a",
        )
        model_c = _make_model(
            "model.pkg.c", "model_c",
            {"id": "integer", "status": "varchar"},
            compiled_sql="SELECT b.id, b.status FROM model_b b",
        )

        all_models = {
            "model.pkg.a": model_a,
            "model.pkg.b": model_b,
            "model.pkg.c": model_c,
        }
        child_map = {
            "model.pkg.a": ["model.pkg.b"],
            "model.pkg.b": ["model.pkg.c"],
        }

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        # model_b is cleared, model_c should not be in impacts
        assert "model.pkg.b" in cleared
        impacted_ids = {i.model_id for i in impacts}
        assert "model.pkg.c" not in impacted_ids

    def test_select_star_fallback(self) -> None:
        """SELECT * from a CTE is handled by the SQL parser."""
        model_a = _make_model(
            "model.pkg.a", "model_a",
            {"id": "integer", "email": "varchar"},
        )
        # SELECT * from a physical table won't resolve column names,
        # so this falls back to model-level impact
        model_b = _make_model(
            "model.pkg.b", "model_b",
            {"id": "integer", "email": "varchar"},
            compiled_sql="SELECT * FROM model_a",
        )

        all_models = {"model.pkg.a": model_a, "model.pkg.b": model_b}
        child_map = {"model.pkg.a": ["model.pkg.b"]}

        impacts, cleared = resolve_column_lineage(
            changed_columns={"model.pkg.a": {"email"}},
            child_map=child_map,
            all_models=all_models,
        )

        # Should have some impact result (either traced or fallback)
        assert len(impacts) >= 1
