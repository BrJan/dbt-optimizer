"""Concrete rule-based SQL analysis checks."""

from __future__ import annotations

import re

from ..models import DbtModel, Severity, Suggestion
from .base import BaseRule

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    """Remove -- line comments and /* */ block comments."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql


def _strip_strings(sql: str) -> str:
    """Replace string literals with placeholders so we don't match inside them."""
    return re.sub(r"'[^']*'", "'__STR__'", sql)


def _normalize(sql: str) -> str:
    return _strip_strings(_strip_comments(sql))


def _suggestion(rule: BaseRule, model: DbtModel, severity: Severity, description: str,
                recommendation: str, context: str = "") -> Suggestion:
    return Suggestion(
        rule_id=rule.rule_id,
        title=rule.title,
        description=description,
        severity=severity,
        model_name=model.name,
        model_path=str(model.path),
        context=context,
        recommendation=recommendation,
        source="rule",
    )


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

class SelectStarRule(BaseRule):
    rule_id = "OPT001"
    title = "SELECT * usage detected"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        # Match SELECT * or SELECT alias.* but not COUNT(*)
        pattern = re.compile(r"\bSELECT\s+(?:\w+\.)?\*", re.IGNORECASE)
        if not pattern.search(sql):
            return []
        return [_suggestion(
            self, model, Severity.MEDIUM,
            "Using SELECT * selects all columns, including ones that may be unused. "
            "This can cause hidden breakages when upstream schemas change and hurts query performance "
            "on columnar warehouses by preventing column pruning.",
            "Explicitly list the columns you need. This improves performance, self-documents intent, "
            "and prevents unexpected column additions from propagating downstream.",
            context="SELECT *",
        )]


class CartesianJoinRule(BaseRule):
    rule_id = "OPT002"
    title = "Implicit cross join / cartesian product risk"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        # Detect comma-separated tables in FROM clause: FROM a, b
        # Heuristic: comma between identifiers/aliases after FROM that isn't inside ()
        pattern = re.compile(
            r"\bFROM\s+[\w.\"` ]+\s*,\s*[\w.\"` ]+",
            re.IGNORECASE,
        )
        if not pattern.search(sql):
            return []
        return [_suggestion(
            self, model, Severity.HIGH,
            "Comma-separated tables in FROM clause create an implicit CROSS JOIN. "
            "If a WHERE clause is missing or incorrect, this produces a cartesian product "
            "and can cause massive data explosions.",
            "Replace implicit joins with explicit JOIN … ON syntax to make join conditions "
            "visible, prevent accidental cross joins, and improve readability.",
        )]


class DistinctOveruseRule(BaseRule):
    rule_id = "OPT003"
    title = "SELECT DISTINCT may mask upstream data quality issues"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        if not re.search(r"\bSELECT\s+DISTINCT\b", sql, re.IGNORECASE):
            return []
        return [_suggestion(
            self, model, Severity.MEDIUM,
            "SELECT DISTINCT adds a de-duplication step that can be expensive on large datasets. "
            "More importantly, it often masks root-cause duplicates in upstream models or joins.",
            "Investigate *why* duplicates exist. Fix the upstream fan-out (e.g. many-to-many join, "
            "missing grain definition) rather than suppressing it with DISTINCT. "
            "If intentional, add a comment explaining why.",
        )]


class SubqueryInFromRule(BaseRule):
    rule_id = "OPT004"
    title = "Subquery in FROM clause instead of CTE"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        # Look for FROM ( SELECT or JOIN ( SELECT
        pattern = re.compile(r"\b(?:FROM|JOIN)\s*\(\s*SELECT\b", re.IGNORECASE)
        matches = pattern.findall(sql)
        if not matches:
            return []
        return [_suggestion(
            self, model, Severity.LOW,
            f"Found {len(matches)} subquery/subqueries nested inside FROM or JOIN. "
            "Inline subqueries are harder to read, harder to test independently, "
            "and some optimizers handle CTEs more efficiently.",
            "Refactor inline subqueries into named CTEs at the top of the model. "
            "This improves readability and allows you to add incremental logic or tests later.",
            context=f"{len(matches)} occurrence(s) of FROM/JOIN (SELECT ...)",
        )]


class FunctionOnFilterColumnRule(BaseRule):
    rule_id = "OPT005"
    title = "Function applied to filtered column (non-sargable predicate)"

    _PATTERNS = [
        re.compile(r"\bWHERE\b[^;]*\b(LOWER|UPPER|TRIM|DATE|CAST|TO_DATE|DATE_TRUNC)\s*\(", re.IGNORECASE),
        re.compile(r"\bAND\b[^;]*\b(LOWER|UPPER|TRIM|DATE|CAST|TO_DATE|DATE_TRUNC)\s*\(", re.IGNORECASE),
    ]

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        found: list[str] = []
        for pat in self._PATTERNS:
            for m in pat.finditer(sql):
                found.append(m.group(1).upper())
        if not found:
            return []
        funcs = ", ".join(sorted(set(found)))
        return [_suggestion(
            self, model, Severity.MEDIUM,
            f"Functions ({funcs}) applied to columns in WHERE/AND clauses prevent the warehouse "
            "from using partition pruning or clustering. These are non-sargable predicates.",
            "Move the transformation to the right-hand side of the comparison where possible. "
            "For example: instead of `WHERE DATE(created_at) = '2024-01-01'` use "
            "`WHERE created_at >= '2024-01-01' AND created_at < '2024-01-02'`.",
            context=f"Functions on filter columns: {funcs}",
        )]


class HardcodedDateRule(BaseRule):
    rule_id = "OPT006"
    title = "Hardcoded date or magic number literal"

    _DATE_PATTERN = re.compile(
        r"['\"](\d{4}-\d{2}-\d{2}|\d{4}/\d{2}/\d{2})['\"]",
        re.IGNORECASE,
    )

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _strip_comments(model.sql)  # keep strings here to find them
        matches = self._DATE_PATTERN.findall(sql)
        if not matches:
            return []
        sample = matches[:3]
        return [_suggestion(
            self, model, Severity.LOW,
            f"Found {len(matches)} hardcoded date literal(s): {sample}. "
            "Hardcoded dates make models brittle and require code changes to update.",
            "Use dbt variables (`{{ var('start_date') }}`), `current_date`, or a reference to "
            "a date spine / calendar model. For incremental models, rely on the incremental "
            "predicate instead of a hardcoded date.",
            context=str(sample),
        )]


class ModelComplexityRule(BaseRule):
    rule_id = "OPT007"
    title = "High model complexity"

    LINE_WARN = 200
    LINE_HIGH = 400
    CTE_WARN = 10

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        lines = model.line_count
        cte_count = len(re.findall(r"\bWITH\b|\),\s*\w+\s+AS\s*\(", sql, re.IGNORECASE))
        join_count = len(re.findall(r"\bJOIN\b", sql, re.IGNORECASE))

        suggestions: list[Suggestion] = []

        if lines >= self.LINE_HIGH:
            suggestions.append(_suggestion(
                self, model, Severity.MEDIUM,
                f"Model is {lines} lines long. Very large models are hard to test, debug, and review.",
                "Consider decomposing this model into intermediate staging or intermediate models. "
                "A good rule of thumb: if a model has more than one clear transformation concern, "
                "it can likely be split.",
                context=f"{lines} lines, ~{cte_count} CTEs, ~{join_count} JOINs",
            ))
        elif lines >= self.LINE_WARN:
            suggestions.append(_suggestion(
                self, model, Severity.LOW,
                f"Model is {lines} lines long — approaching complexity threshold.",
                "Review whether this model can be split into upstream intermediate models "
                "to keep concerns separated.",
                context=f"{lines} lines",
            ))

        if join_count >= 8:
            suggestions.append(_suggestion(
                self, model, Severity.MEDIUM,
                f"Model contains {join_count} JOIN operations, which can be expensive and hard to reason about.",
                "Consider whether some joins can be pushed into upstream staging models. "
                "High join counts often indicate a model is doing too much.",
                context=f"{join_count} JOINs",
            ))

        return suggestions


class MissingIncrementalStrategyRule(BaseRule):
    rule_id = "OPT008"
    title = "Large table materialized as 'table' — consider incremental"

    # Heuristics that suggest the model processes time-series or event data.
    # Each pattern that matches counts as one signal; we require >= 2.
    _TIME_PATTERNS = [
        re.compile(r"\b(created_at|updated_at|event_date|event_time|occurred_at|loaded_at)\b", re.IGNORECASE),
        re.compile(r"\bDATE_TRUNC\s*\(\s*['\"]?(day|week|month|year)['\"]?\s*,", re.IGNORECASE),
        re.compile(r"\bCURRENT_TIMESTAMP\b|\bGETDATE\s*\(\s*\)|\bNOW\s*\(\s*\)", re.IGNORECASE),
        re.compile(r"\bDATEADD\b|\bDATEDIFF\b|\bTIMESTAMPADD\b", re.IGNORECASE),
        re.compile(r"\b_at\b|\b_date\b|\b_time\b|\b_ts\b", re.IGNORECASE),
    ]

    def check(self, model: DbtModel) -> list[Suggestion]:
        if model.materialization != "table":
            return []
        if model.line_count < 50:
            return []

        sql = _normalize(model.sql)
        time_signals = sum(1 for p in self._TIME_PATTERNS if p.search(sql))
        if time_signals < 2:
            return []

        return [_suggestion(
            self, model, Severity.MEDIUM,
            f"Model '{model.name}' is materialized as `table` and contains time-series patterns "
            f"({time_signals} time-related signals detected). Full refreshes rebuild the entire table "
            "on every run, which is expensive for growing datasets.",
            "Evaluate using `materialized='incremental'` with an appropriate `unique_key` and "
            "`incremental_strategy`. Start with `strategy='delete+insert'` or `'merge'`. "
            "Add an `is_incremental()` filter on your timestamp column.",
        )]


class MissingTestsRule(BaseRule):
    rule_id = "OPT009"
    title = "Model has no dbt tests defined"

    def check(self, model: DbtModel) -> list[Suggestion]:
        if model.has_tests:
            return []
        return [_suggestion(
            self, model, Severity.MEDIUM,
            f"Model '{model.name}' has no dbt tests defined in a schema.yml file. "
            "Without tests, data quality regressions can go undetected.",
            "Add at minimum `unique` and `not_null` tests on the primary key column. "
            "Consider adding `accepted_values` tests for categorical columns and "
            "`relationships` tests for foreign keys.",
        )]


class MissingDescriptionRule(BaseRule):
    rule_id = "OPT010"
    title = "Model has no description"

    def check(self, model: DbtModel) -> list[Suggestion]:
        if model.has_description:
            return []
        return [_suggestion(
            self, model, Severity.INFO,
            f"Model '{model.name}' has no description in schema.yml. "
            "Undocumented models are hard for new team members to understand.",
            "Add a `description:` field to the model entry in schema.yml. "
            "Good descriptions answer: what does this model represent, what is the grain, "
            "and what are the key business rules applied?",
        )]


class MultipleAggregationsRule(BaseRule):
    rule_id = "OPT011"
    title = "Multiple aggregation levels in a single model"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        group_bys = re.findall(r"\bGROUP\s+BY\b", sql, re.IGNORECASE)
        if len(group_bys) < 2:
            return []
        return [_suggestion(
            self, model, Severity.LOW,
            f"Model contains {len(group_bys)} GROUP BY clauses, suggesting multiple aggregation levels. "
            "This often leads to fanout issues or convoluted SQL.",
            "Consider splitting into separate models per aggregation grain. "
            "This makes each model easier to test (e.g. uniqueness tests per grain) "
            "and reason about independently.",
            context=f"{len(group_bys)} GROUP BY clauses",
        )]


class UnionAllDeduplicationRule(BaseRule):
    rule_id = "OPT012"
    title = "UNION (without ALL) causes full de-duplication sort"

    def check(self, model: DbtModel) -> list[Suggestion]:
        sql = _normalize(model.sql)
        # UNION not followed by ALL
        pattern = re.compile(r"\bUNION\b(?!\s+ALL\b)", re.IGNORECASE)
        if not pattern.search(sql):
            return []
        return [_suggestion(
            self, model, Severity.LOW,
            "UNION without ALL performs a full sort + deduplication across both result sets, "
            "which is significantly more expensive than UNION ALL.",
            "Use `UNION ALL` if duplicates are not expected (or are acceptable). "
            "If you genuinely need deduplication, consider adding a CTE with ROW_NUMBER() "
            "to make the deduplication logic explicit and easier to reason about.",
        )]


class RefInsteadOfTableRule(BaseRule):
    rule_id = "OPT013"
    title = "Direct table reference instead of dbt ref() or source()"

    # Match schema.table patterns
    _TABLE_PATTERN = re.compile(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]+)\b",
    )

    _DBT_JINJA = re.compile(r"\{\{\s*(ref|source)\s*\(")

    def check(self, model: DbtModel) -> list[Suggestion]:
        # Only flag models that already use Jinja (i.e., are aware of dbt patterns)
        # but also have raw table references
        sql = _strip_comments(model.sql)
        if not self._DBT_JINJA.search(sql):
            # Model uses no jinja at all — might be intentional (macros, etc.)
            return []

        # Replace all {{ ... }} jinja blocks so we don't match inside ref()/source()
        cleaned = re.sub(r"\{\{[^}]+\}\}", " __DBT_REF__ ", sql)
        cleaned = _strip_strings(cleaned)

        matches = self._TABLE_PATTERN.findall(cleaned)
        # Filter out common false positives:
        # - single/double-char prefixes are almost certainly table aliases (o.id, t.col)
        # - known SQL function false positives
        false_positives = {"date_trunc", "date_part", "to_date", "current_date", "dbt_ref"}
        matches = [
            (a, b) for a, b in matches
            if len(a) >= 3
            and a.lower() not in false_positives
            and b.lower() not in false_positives
        ]

        if not matches:
            return []

        sample = [f"{a}.{b}" for a, b in matches[:3]]
        return [_suggestion(
            self, model, Severity.HIGH,
            f"Found {len(matches)} direct table reference(s) that bypass dbt's ref()/source() system: "
            f"{sample}. This breaks lineage tracking, prevents environment promotion, "
            "and skips dbt's dependency ordering.",
            "Wrap table references in `{{ ref('model_name') }}` for dbt models or "
            "`{{ source('schema', 'table') }}` for raw source tables. "
            "If these are external references outside dbt, define them as sources.",
            context=str(sample),
        )]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ALL_RULES: list[BaseRule] = [
    SelectStarRule(),
    CartesianJoinRule(),
    DistinctOveruseRule(),
    SubqueryInFromRule(),
    FunctionOnFilterColumnRule(),
    HardcodedDateRule(),
    ModelComplexityRule(),
    MissingIncrementalStrategyRule(),
    MissingTestsRule(),
    MissingDescriptionRule(),
    MultipleAggregationsRule(),
    UnionAllDeduplicationRule(),
    RefInsteadOfTableRule(),
]
