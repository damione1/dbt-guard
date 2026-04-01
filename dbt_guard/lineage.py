"""SQL column extraction using SQLGlot.

This module determines the *output columns* of a compiled dbt model by parsing
its SQL with SQLGlot.  It handles:

- Simple ``SELECT col1, col2 FROM ...``
- CTE chains ending in ``SELECT ... FROM some_cte``
- ``SELECT * FROM some_cte`` — the star is expanded by tracing back through the
  CTE definition recursively.

If parsing fails for any reason the function returns ``None`` so the caller can
fall back to the documented columns from the manifest.

SQLGlot version note
--------------------
In SQLGlot >= 25, ``parse_one`` returns a ``Select`` node that embeds CTEs via
its ``.ctes`` property (a list of ``CTE`` objects).  The older pattern of
returning a top-level ``With`` node wrapping a ``Select`` is no longer the
common case.  This module handles both via :func:`_build_cte_lookup`.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import sqlglot
import sqlglot.expressions as exp
from sqlglot.expressions import Expression

logger = logging.getLogger(__name__)


def extract_columns_from_sql(
    sql: str,
    dialect: str = "default",
) -> Optional[List[str]]:
    """Return the list of output column names produced by *sql*.

    Parameters
    ----------
    sql:
        Compiled SQL text of a dbt model.
    dialect:
        SQLGlot dialect name (e.g. ``"snowflake"``, ``"bigquery"``).
        ``"default"`` means no dialect hint is applied.

    Returns
    -------
    List of lowercase column names, or ``None`` if parsing fails or produces
    no usable output (e.g. ``SELECT *`` from a physical table).
    """
    if not sql or not sql.strip():
        return None

    dialect_arg: Optional[str] = None if dialect == "default" else dialect

    try:
        statement = sqlglot.parse_one(
            sql,
            dialect=dialect_arg,
            error_level=sqlglot.ErrorLevel.WARN,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("sqlglot.parse_one raised %s: %s", type(exc).__name__, exc)
        return None

    if statement is None:
        return None

    # parse_one returns Expr; cast to Expression so helpers are happy with mypy
    stmt: Expression = statement  # type: ignore[assignment]

    # Build the CTE lookup from whatever top-level node sqlglot returned
    cte_lookup: Dict[str, exp.Expression] = _build_cte_lookup(stmt)

    # Locate the outermost SELECT
    outer_select = _find_outermost_select(stmt)
    if outer_select is None:
        logger.debug("No SELECT found in statement type=%s", type(statement).__name__)
        return None

    try:
        columns = _extract_from_select(outer_select, cte_lookup, depth=0)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Column extraction failed: %s", exc)
        return None

    if columns is None:
        return None

    # Deduplicate while preserving order, lowercase everything
    seen: set = set()
    result: List[str] = []
    for col in columns:
        key = col.lower()
        if key not in seen:
            seen.add(key)
            result.append(key)

    return result if result else None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_cte_lookup(statement: exp.Expression) -> Dict[str, exp.Expression]:
    """Extract all CTEs from *statement* into a ``{name: body_expr}`` dict.

    Handles both the modern SQLGlot representation (``Select.ctes``) and the
    older ``With`` wrapper node.
    """
    cte_lookup: Dict[str, exp.Expression] = {}

    # Modern SQLGlot: a Select carries CTEs in its .ctes property
    if isinstance(statement, exp.Select) and statement.ctes:
        for cte in statement.ctes:
            name = (cte.alias or "").lower()
            if name:
                cte_lookup[name] = cte.this
        return cte_lookup

    # Older / alternative: top-level With node wrapping a Select
    if isinstance(statement, exp.With):
        for cte in statement.expressions:
            if isinstance(cte, exp.CTE):
                name = (cte.alias or "").lower()
                if name:
                    cte_lookup[name] = cte.this
        return cte_lookup

    # Fallback: scan for any CTE nodes anywhere in the tree
    for cte in statement.find_all(exp.CTE):
        name = (cte.alias or "").lower()
        if name:
            cte_lookup[name] = cte.this

    return cte_lookup


def _find_outermost_select(statement: exp.Expression) -> Optional[exp.Select]:
    """Return the topmost SELECT expression in *statement*.

    For ``Select`` nodes (modern SQLGlot), the node itself *is* the outer
    SELECT — even when it carries CTEs.

    For ``With`` nodes (older pattern), the inner ``this`` is the SELECT.
    """
    if isinstance(statement, exp.Select):
        return statement
    if isinstance(statement, exp.With):
        inner = statement.this
        if isinstance(inner, exp.Select):
            return inner
        return _find_outermost_select(inner)
    # Try to find any Select as a fallback
    return statement.find(exp.Select)


def _extract_from_select(
    select: exp.Select,
    cte_lookup: Dict[str, exp.Expression],
    depth: int,
) -> Optional[List[str]]:
    """Extract column names from a SELECT node, resolving CTEs for ``*`` stars."""
    if depth > 20:
        logger.debug("Max CTE recursion depth reached")
        return None

    columns: List[str] = []

    for expr in select.expressions:
        if isinstance(expr, exp.Star):
            # SELECT * — resolve through the FROM source
            source_name = _primary_from_name(select)
            if source_name and source_name.lower() in cte_lookup:
                cte_body = cte_lookup[source_name.lower()]
                # The CTE body is a Select (possibly with its own CTEs)
                sub_select = _find_outermost_select(cte_body)
                if sub_select is None:
                    logger.debug("CTE body has no SELECT for star expansion")
                    return None
                resolved = _extract_from_select(sub_select, cte_lookup, depth + 1)
                if resolved is None:
                    return None
                columns.extend(resolved)
            else:
                logger.debug(
                    "Cannot resolve SELECT * from source '%s'", source_name
                )
                return None
        elif isinstance(expr, exp.Alias):
            alias = expr.alias
            if alias:
                columns.append(alias)
            else:
                inner_name = _expr_name(expr.this)
                if inner_name:
                    columns.append(inner_name)
        else:
            name = _expr_name(expr)
            if name:
                columns.append(name)
            else:
                logger.debug("Could not derive column name for expr type=%s", type(expr).__name__)

    return columns


def _primary_from_name(select: exp.Select) -> Optional[str]:
    """Return the primary table/alias name from a SELECT's FROM clause."""
    # In SQLGlot >= 25 the arg key is "from_" (with trailing underscore)
    from_clause = select.args.get("from_") or select.args.get("from")
    if from_clause is None:
        # Fall back to find() scan
        from_clause = select.find(exp.From)
    if from_clause is None:
        return None

    # Direct table reference
    table = from_clause.find(exp.Table)
    if table is not None:
        # Prefer alias over table name (e.g. FROM users u → "u")
        return (table.alias or table.name) or None

    # Subquery with alias
    subquery = from_clause.find(exp.Subquery)
    if subquery is not None and subquery.alias:
        return subquery.alias

    return None


def _expr_name(expr: exp.Expression) -> Optional[str]:
    """Best-effort: derive the output column name for a non-aliased expression."""
    if isinstance(expr, exp.Column):
        return expr.name or None
    if isinstance(expr, (exp.Anonymous, exp.Func)):
        return None
    if hasattr(expr, "name") and expr.name:
        return expr.name
    return None
