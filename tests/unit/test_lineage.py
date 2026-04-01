"""Unit tests for dbt_guard.lineage."""

from __future__ import annotations

import pytest

from dbt_guard.lineage import extract_columns_from_sql


class TestSimpleSelect:
    def test_extracts_explicit_columns(self, simple_select_sql: str) -> None:
        cols = extract_columns_from_sql(simple_select_sql)
        assert cols is not None
        assert cols == ["id", "name", "email", "created_at"]

    def test_result_is_lowercase(self) -> None:
        sql = "select ID, Name, EMAIL from users"
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        for col in cols:
            assert col == col.lower()

    def test_alias_used_as_column_name(self) -> None:
        sql = "select u.id as user_id, u.name as full_name from users u"
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        assert "user_id" in cols
        assert "full_name" in cols

    def test_expression_with_alias(self) -> None:
        sql = "select id, UPPER(name) as upper_name, count(*) as cnt from users"
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        assert "id" in cols
        assert "upper_name" in cols
        assert "cnt" in cols


class TestCteChain:
    def test_cte_chain_extracts_final_columns(self, cte_chain_sql: str) -> None:
        cols = extract_columns_from_sql(cte_chain_sql)
        assert cols is not None
        assert set(cols) == {"id", "name", "email", "status"}

    def test_single_cte_explicit_final_select(self) -> None:
        sql = """
        with base as (
            select id, name from users
        )
        select id, name from base
        """
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        assert cols == ["id", "name"]

    def test_cte_with_aliased_columns(self) -> None:
        sql = """
        with src as (
            select id, full_name as name from users
        )
        select id, name from src
        """
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        assert set(cols) == {"id", "name"}


class TestStarSelect:
    def test_star_resolved_through_cte(self, star_select_sql: str) -> None:
        cols = extract_columns_from_sql(star_select_sql)
        assert cols is not None
        assert set(cols) == {"id", "name", "email"}

    def test_star_from_explicit_table_returns_none(self) -> None:
        # SELECT * FROM a real table (not a CTE) — we cannot expand this
        sql = "select * from raw_users"
        cols = extract_columns_from_sql(sql)
        # We expect None since we can't resolve the columns of a physical table
        assert cols is None

    def test_nested_cte_star_resolution(self) -> None:
        sql = """
        with stage1 as (
            select a, b, c from source_table
        ),
        final as (
            select * from stage1
        )
        select * from final
        """
        cols = extract_columns_from_sql(sql)
        assert cols is not None
        assert set(cols) == {"a", "b", "c"}


class TestInvalidSQL:
    def test_invalid_sql_returns_none(self) -> None:
        sql = "this is not sql at all !!!@@@"
        cols = extract_columns_from_sql(sql)
        assert cols is None

    def test_empty_string_returns_none(self) -> None:
        cols = extract_columns_from_sql("")
        assert cols is None

    def test_whitespace_only_returns_none(self) -> None:
        cols = extract_columns_from_sql("   \n\t  ")
        assert cols is None

    def test_incomplete_sql_returns_none_or_list(self) -> None:
        sql = "select"
        result = extract_columns_from_sql(sql)
        # May return None or empty list — both are acceptable graceful degradations
        assert result is None or result == []

    def test_non_select_statement_returns_none_or_list(self) -> None:
        sql = "CREATE TABLE foo (id INT)"
        result = extract_columns_from_sql(sql)
        # CREATE TABLE has no output columns
        assert result is None or result == []


class TestDialects:
    def test_snowflake_dialect(self) -> None:
        sql = "select id, name, amount::float as amount_float from payments"
        cols = extract_columns_from_sql(sql, dialect="snowflake")
        assert cols is not None
        assert "id" in cols
        assert "name" in cols
        assert "amount_float" in cols

    def test_default_dialect_handles_standard_sql(self) -> None:
        sql = "select id, COALESCE(name, 'unknown') as name from users"
        cols = extract_columns_from_sql(sql, dialect="default")
        assert cols is not None
        assert "id" in cols
        assert "name" in cols
