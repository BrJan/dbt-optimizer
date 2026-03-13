"""Tests for rule-based SQL checks."""

import pytest
from pathlib import Path

from dbt_optimizer.models import DbtModel, Severity
from dbt_optimizer.rules.sql_rules import (
    SelectStarRule,
    CartesianJoinRule,
    DistinctOveruseRule,
    SubqueryInFromRule,
    FunctionOnFilterColumnRule,
    HardcodedDateRule,
    ModelComplexityRule,
    MissingIncrementalStrategyRule,
    MissingTestsRule,
    MissingDescriptionRule,
    UnionAllDeduplicationRule,
    RefInsteadOfTableRule,
)


def make_model(sql: str, name: str = "test_model", materialization: str = "view",
               has_tests: bool = True, has_description: bool = True) -> DbtModel:
    return DbtModel(
        name=name,
        path=Path(f"models/{name}.sql"),
        sql=sql,
        materialization=materialization,
        has_tests=has_tests,
        has_description=has_description,
    )


# ---------------------------------------------------------------------------
# OPT001 – SELECT *
# ---------------------------------------------------------------------------

class TestSelectStarRule:
    rule = SelectStarRule()

    def test_flags_select_star(self):
        model = make_model("SELECT * FROM {{ ref('orders') }}")
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].rule_id == "OPT001"
        assert results[0].severity == Severity.MEDIUM

    def test_flags_aliased_star(self):
        model = make_model("SELECT t.* FROM orders t")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_allows_count_star(self):
        model = make_model("SELECT COUNT(*) FROM {{ ref('orders') }}")
        results = self.rule.check(model)
        assert len(results) == 0

    def test_no_flag_explicit_columns(self):
        model = make_model("SELECT id, name FROM {{ ref('orders') }}")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT002 – Cartesian join
# ---------------------------------------------------------------------------

class TestCartesianJoinRule:
    rule = CartesianJoinRule()

    def test_flags_implicit_cross_join(self):
        model = make_model("SELECT * FROM orders o, customers c WHERE o.id = c.id")
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].severity == Severity.HIGH

    def test_no_flag_explicit_join(self):
        model = make_model("SELECT * FROM orders o JOIN customers c ON o.id = c.id")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT003 – DISTINCT overuse
# ---------------------------------------------------------------------------

class TestDistinctOveruseRule:
    rule = DistinctOveruseRule()

    def test_flags_select_distinct(self):
        model = make_model("SELECT DISTINCT id, name FROM orders")
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].severity == Severity.MEDIUM

    def test_no_flag_regular_select(self):
        model = make_model("SELECT id, name FROM orders")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT004 – Subquery in FROM
# ---------------------------------------------------------------------------

class TestSubqueryInFromRule:
    rule = SubqueryInFromRule()

    def test_flags_subquery_in_from(self):
        model = make_model("SELECT * FROM (SELECT id FROM orders) sub")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_flags_subquery_in_join(self):
        model = make_model("SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_no_flag_cte(self):
        model = make_model("WITH sub AS (SELECT id FROM orders) SELECT * FROM sub")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT005 – Function on filter column
# ---------------------------------------------------------------------------

class TestFunctionOnFilterColumnRule:
    rule = FunctionOnFilterColumnRule()

    def test_flags_date_function_in_where(self):
        model = make_model("SELECT id FROM orders WHERE DATE(created_at) = '2024-01-01'")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_flags_lower_in_where(self):
        model = make_model("SELECT id FROM customers WHERE LOWER(email) = 'test@example.com'")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_no_flag_clean_filter(self):
        model = make_model("SELECT id FROM orders WHERE created_at >= '2024-01-01'")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT006 – Hardcoded dates
# ---------------------------------------------------------------------------

class TestHardcodedDateRule:
    rule = HardcodedDateRule()

    def test_flags_hardcoded_date(self):
        model = make_model("SELECT * FROM orders WHERE order_date >= '2023-01-01'")
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].severity == Severity.LOW

    def test_no_flag_no_dates(self):
        model = make_model("SELECT * FROM orders WHERE amount > 100")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT008 – Missing incremental
# ---------------------------------------------------------------------------

class TestMissingIncrementalStrategyRule:
    rule = MissingIncrementalStrategyRule()

    def test_flags_large_table_with_timestamps(self):
        sql = "\n".join([
            "SELECT order_id, created_at, updated_at, amount",
            "FROM {{ ref('raw_orders') }}",
            "WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP)",
            "GROUP BY 1,2,3,4",
        ] * 15)  # Make it > 50 lines
        model = make_model(sql, materialization="table")
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].severity == Severity.MEDIUM

    def test_no_flag_incremental_model(self):
        sql = "\n".join(["SELECT id, created_at FROM orders"] * 60)
        model = make_model(sql, materialization="incremental")
        results = self.rule.check(model)
        assert len(results) == 0

    def test_no_flag_small_table(self):
        model = make_model("SELECT id, created_at FROM orders", materialization="table")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT009 – Missing tests
# ---------------------------------------------------------------------------

class TestMissingTestsRule:
    rule = MissingTestsRule()

    def test_flags_model_without_tests(self):
        model = make_model("SELECT id FROM orders", has_tests=False)
        results = self.rule.check(model)
        assert len(results) == 1

    def test_no_flag_model_with_tests(self):
        model = make_model("SELECT id FROM orders", has_tests=True)
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT012 – UNION without ALL
# ---------------------------------------------------------------------------

class TestUnionAllDeduplicationRule:
    rule = UnionAllDeduplicationRule()

    def test_flags_union_without_all(self):
        model = make_model("SELECT id FROM a UNION SELECT id FROM b")
        results = self.rule.check(model)
        assert len(results) == 1

    def test_no_flag_union_all(self):
        model = make_model("SELECT id FROM a UNION ALL SELECT id FROM b")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# OPT013 – Direct table reference
# ---------------------------------------------------------------------------

class TestRefInsteadOfTableRule:
    rule = RefInsteadOfTableRule()

    def test_flags_direct_schema_table_ref(self):
        model = make_model(
            "SELECT o.id FROM {{ ref('stg_orders') }} o JOIN analytics.public.customers c ON o.id = c.id"
        )
        results = self.rule.check(model)
        assert len(results) == 1
        assert results[0].severity == Severity.HIGH

    def test_no_flag_only_ref(self):
        model = make_model(
            "SELECT id FROM {{ ref('stg_orders') }} JOIN {{ ref('stg_customers') }} USING (customer_id)"
        )
        results = self.rule.check(model)
        assert len(results) == 0

    def test_no_flag_no_jinja(self):
        # Model that doesn't use any Jinja at all — may be intentional
        model = make_model("SELECT id FROM raw.orders")
        results = self.rule.check(model)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Integration: sample project fixture
# ---------------------------------------------------------------------------

class TestSampleProjectFixture:
    """End-to-end check that the fixture project loads and produces suggestions."""

    def test_project_loads_and_produces_suggestions(self):
        from dbt_optimizer.project import DbtProjectParser
        from dbt_optimizer.rules import ALL_RULES

        fixture_path = Path(__file__).parent / "fixtures" / "sample_project"
        parser = DbtProjectParser(fixture_path).load()
        models = parser.discover_models()

        assert len(models) >= 2

        all_suggestions = []
        for model in models:
            for rule in ALL_RULES:
                all_suggestions.extend(rule.check(model))

        # The fixture project is designed with intentional issues
        assert len(all_suggestions) > 0

        rule_ids = {s.rule_id for s in all_suggestions}
        # fct_orders.sql has cartesian join, UNION without ALL, direct table ref
        assert "OPT002" in rule_ids or "OPT012" in rule_ids or "OPT013" in rule_ids
