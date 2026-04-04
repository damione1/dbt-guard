"""Column-level lineage resolution using sqlglot.lineage.

This module traces column-to-column dependencies through compiled SQL to determine
whether a downstream model actually references a changed upstream column. This
eliminates false positives from model-level BFS impact analysis.
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Dict, List, Optional, Set, Tuple

import sqlglot.lineage as sg_lineage

from .exceptions import ColumnLineageError
from .lineage import extract_columns_from_sql
from .models import (
    ColumnLineageImpact,
    ColumnLineageLink,
    ImpactedColumn,
    ModelColumns,
)

logger = logging.getLogger(__name__)


def resolve_column_lineage(
    changed_columns: Dict[str, Set[str]],  # model_id -> set of changed column names
    child_map: Dict[str, List[str]],
    all_models: Dict[str, ModelColumns],
    dialect: Optional[str] = None,
    max_depth: int = 10,
    strict: bool = False,
) -> Tuple[List[ColumnLineageImpact], List[str]]:
    """Resolve column-level lineage to determine actual downstream impact.

    Parameters
    ----------
    changed_columns:
        Mapping of model_id -> set of changed column names (from breaking changes).
    child_map:
        Adjacency list: ``{parent_id: [child_id, ...]}``.
    all_models:
        All models/seeds from the manifest.
    dialect:
        SQL dialect for sqlglot parsing (e.g. ``"snowflake"``).
    max_depth:
        Maximum BFS depth for column lineage propagation.
    strict:
        If True, raise :class:`ColumnLineageError` when compiled SQL is missing.
        If False, fall back to model-level impact (assume impacted).

    Returns
    -------
    Tuple of (column_lineage_impacts, cleared_model_ids).
    ``cleared_model_ids`` are models that were in the BFS impact set but have
    no actual column-level dependency on any changed column.
    """
    impacts: List[ColumnLineageImpact] = []
    cleared: List[str] = []

    # BFS queue: (model_id, depth)
    visited: Set[str] = set()
    queue: deque[Tuple[str, int]] = deque()

    # Seed with direct children of changed models
    changed_set = set(changed_columns.keys())
    for model_id in changed_columns:
        for child_id in child_map.get(model_id, []):
            if child_id not in changed_set and child_id not in visited:
                visited.add(child_id)
                queue.append((child_id, 1))

    # Track propagated changes: model_id -> set of affected output column names
    propagated: Dict[str, Set[str]] = dict(changed_columns)

    while queue:
        model_id, depth = queue.popleft()
        if depth > max_depth:
            continue

        model = all_models.get(model_id)
        if model is None:
            continue

        compiled_sql = model._compiled_sql
        if not compiled_sql:
            if strict:
                raise ColumnLineageError(
                    f"Compiled SQL missing for {model_id} and --strict-lineage is set"
                )
            # Fallback: assume impacted at model level, no column detail
            impacts.append(
                ColumnLineageImpact(
                    model_id=model_id,
                    model_name=model.model_name,
                    impacted_columns=[
                        ImpactedColumn(
                            column_name="*",
                            reason="compiled SQL unavailable, assuming full impact",
                        )
                    ],
                    cleared=False,
                )
            )
            # Propagate all changed columns from parents to children
            parent_changes: Set[str] = set()
            for parent_id in changed_columns:
                if model_id in child_map.get(parent_id, []):
                    parent_changes.update(propagated.get(parent_id, set()))
            if parent_changes:
                propagated[model_id] = parent_changes
            # Continue BFS for children
            for child_id in child_map.get(model_id, []):
                if child_id not in visited and child_id not in changed_set:
                    visited.add(child_id)
                    queue.append((child_id, depth + 1))
            continue

        # Build schema dict for sqlglot lineage
        schema = _build_schema_dict(model_id, child_map, all_models)

        # Get output columns of this model
        dialect_arg = dialect or "default"
        output_cols = extract_columns_from_sql(compiled_sql, dialect_arg)
        if not output_cols:
            output_cols = list(model.columns.keys())
        if not output_cols:
            # No columns known; fall back to model-level impact
            impacts.append(
                ColumnLineageImpact(
                    model_id=model_id,
                    model_name=model.model_name,
                    impacted_columns=[
                        ImpactedColumn(
                            column_name="*",
                            reason="no output columns detected, assuming full impact",
                        )
                    ],
                    cleared=False,
                )
            )
            continue

        # For each output column, check if it traces back to any changed column
        impacted_cols: List[ImpactedColumn] = []
        affected_output_cols: Set[str] = set()

        for out_col in output_cols:
            source_refs = _trace_column(out_col, compiled_sql, schema, dialect)
            if source_refs is None:
                # Tracing failed; conservatively mark as impacted
                impacted_cols.append(
                    ImpactedColumn(
                        column_name=out_col,
                        reason="lineage tracing failed, conservatively marking impacted",
                    )
                )
                affected_output_cols.add(out_col)
                continue

            # Check if any source ref matches a changed column
            for src_table, src_col in source_refs:
                # Resolve src_table to a model_id
                src_model_id = _resolve_table_to_model_id(
                    src_table, all_models
                )
                if src_model_id and src_col in propagated.get(src_model_id, set()):
                    chain = [
                        ColumnLineageLink(
                            model_id=src_model_id,
                            model_name=all_models[src_model_id].model_name
                            if src_model_id in all_models
                            else src_table,
                            column_name=src_col,
                        ),
                        ColumnLineageLink(
                            model_id=model_id,
                            model_name=model.model_name,
                            column_name=out_col,
                        ),
                    ]
                    impacted_cols.append(
                        ImpactedColumn(
                            column_name=out_col,
                            reason=f"references changed column {src_col} from {src_table}",
                            chain=chain,
                        )
                    )
                    affected_output_cols.add(out_col)
                    break  # One match is enough for this output column

        if impacted_cols:
            impacts.append(
                ColumnLineageImpact(
                    model_id=model_id,
                    model_name=model.model_name,
                    impacted_columns=impacted_cols,
                    cleared=False,
                )
            )
            # Propagate affected output columns as changed for downstream BFS
            propagated[model_id] = affected_output_cols
            for child_id in child_map.get(model_id, []):
                if child_id not in visited and child_id not in changed_set:
                    visited.add(child_id)
                    queue.append((child_id, depth + 1))
        else:
            # No output column traces to a changed column → cleared
            impacts.append(
                ColumnLineageImpact(
                    model_id=model_id,
                    model_name=model.model_name,
                    impacted_columns=[],
                    cleared=True,
                )
            )
            cleared.append(model_id)
            # Do NOT propagate to children — this model breaks the chain

    return impacts, cleared


def _build_schema_dict(
    model_id: str,
    child_map: Dict[str, List[str]],
    all_models: Dict[str, ModelColumns],
) -> Dict[str, Dict[str, str]]:
    """Build a schema dict for sqlglot lineage tracing.

    Maps table names to their column type definitions. Includes all models
    that could be upstream of *model_id* (i.e., any model whose child_map
    includes model_id).
    """
    schema: Dict[str, Dict[str, str]] = {}

    for mid, model in all_models.items():
        if not model.columns:
            continue
        col_types = {
            col_name: col_info.data_type or "VARCHAR"
            for col_name, col_info in model.columns.items()
        }
        # Register under model_name (how tables typically appear in SQL)
        schema[model.model_name.lower()] = col_types
        # Also register under common schema-qualified patterns
        parts = mid.split(".")
        if len(parts) >= 3:
            # e.g. "schema.model_name" pattern
            schema[f"{parts[-2]}.{parts[-1]}".lower()] = col_types

    return schema


def _trace_column(
    column: str,
    sql: str,
    schema: Dict[str, Dict[str, str]],
    dialect: Optional[str],
) -> Optional[Set[Tuple[str, str]]]:
    """Trace a single output column back to its source (table, column) pairs.

    Returns a set of (table_name, column_name) tuples, or None if tracing fails.
    """
    try:
        result = sg_lineage.lineage(
            column,
            sql,
            dialect=dialect,
            schema=schema,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("sqlglot lineage failed for column '%s': %s", column, exc)
        return None

    refs: Set[Tuple[str, str]] = set()
    _walk_lineage_node(result, refs)
    return refs if refs else None


def _walk_lineage_node(
    node: sg_lineage.Node,
    refs: Set[Tuple[str, str]],
) -> None:
    """Walk a sqlglot lineage node tree and collect source (table, column) refs."""
    if not node.downstream:
        # Leaf node — represents a source column reference.
        # node.name is e.g. "a.email" (alias.column) or just "email"
        # node.expression is typically a Table node for the source table
        name = node.name or ""

        # Resolve the actual table name from the expression (Table node)
        import sqlglot.expressions as exp

        expr = node.expression
        table_name = ""
        if isinstance(expr, exp.Table):
            table_name = expr.name or ""
        else:
            # Fallback: try the source attribute
            source = getattr(node, "source", None)
            if source and isinstance(source, exp.Table):
                table_name = source.name or ""

        # Extract column name from node.name (format: "alias.col" or "col")
        if "." in name:
            col_name = name.split(".", 1)[1]
        else:
            col_name = name

        if table_name and col_name:
            refs.add((table_name.lower(), col_name.lower()))
        return

    for child in node.downstream:
        _walk_lineage_node(child, refs)


def _resolve_table_to_model_id(
    table_name: str,
    all_models: Dict[str, ModelColumns],
) -> Optional[str]:
    """Resolve a table name (as it appears in SQL) to a model unique_id."""
    lower = table_name.lower()
    for mid, model in all_models.items():
        if model.model_name.lower() == lower:
            return mid
        # Check schema.model_name pattern
        parts = mid.split(".")
        if len(parts) >= 3 and f"{parts[-2]}.{parts[-1]}".lower() == lower:
            return mid
    return None
