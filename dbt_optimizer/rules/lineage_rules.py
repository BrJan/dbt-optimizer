"""Lineage-aware rules — require MCP enrichment to produce suggestions.

These rules only fire when a model's ``upstream_models`` / ``downstream_models``
fields have been populated by :meth:`DbtProjectParser.enrich_models_from_mcp`.
If MCP data is absent the rules silently return no suggestions, so they are
safe to include in ``ALL_RULES`` regardless of whether MCP is configured.
"""

from __future__ import annotations

from ..models import DbtModel, Severity, Suggestion
from .base import BaseRule

# Threshold above which a view/ephemeral model is flagged as a fragile fan-out
_FANOUT_THRESHOLD = 5


class OrphanedModelRule(BaseRule):
    """LIN001 — Model with no downstream consumers.

    A model that nothing else depends on is a candidate for removal.
    Exposures are not tracked here, so this is a heuristic — the analyst
    should verify before dropping the model.
    """

    rule_id = "LIN001"
    title = "Orphaned model (no downstream consumers)"

    def check(self, model: DbtModel) -> list[Suggestion]:
        # Only fire when lineage was actually fetched
        if not model.upstream_models and not model.downstream_models:
            return []  # MCP data absent — skip

        if model.downstream_models:
            return []  # Has consumers — fine

        return [
            Suggestion(
                rule_id=self.rule_id,
                title=self.title,
                description=(
                    f"'{model.name}' has no downstream models referencing it. "
                    "It may be dead code or used only via an exposure/direct query."
                ),
                severity=Severity.LOW,
                model_name=model.name,
                model_path=str(model.path),
                recommendation=(
                    "Verify whether this model is referenced in an exposure, dashboard, "
                    "or BI tool. If not, consider removing it to reduce project complexity."
                ),
                source="rule",
            )
        ]


class FragileFanOutRule(BaseRule):
    """LIN002 — View/ephemeral model with many downstream consumers.

    When many models depend on a non-materialized model, every dependent
    query re-executes the view's SQL.  This multiplies computation and can
    hide expensive logic behind a lightweight-looking reference.
    """

    rule_id = "LIN002"
    title = "High-fan-out view — consider materializing as table"

    def check(self, model: DbtModel) -> list[Suggestion]:
        if not model.upstream_models and not model.downstream_models:
            return []  # MCP data absent

        if model.materialization not in ("view", "ephemeral"):
            return []

        fan_out = len(model.downstream_models)
        if fan_out < _FANOUT_THRESHOLD:
            return []

        return [
            Suggestion(
                rule_id=self.rule_id,
                title=self.title,
                description=(
                    f"'{model.name}' is materialized as '{model.materialization}' "
                    f"but has {fan_out} downstream models ({', '.join(model.downstream_models[:5])}"
                    + (f" … and {fan_out - 5} more" if fan_out > 5 else "")
                    + "). Each downstream query re-executes this model's SQL."
                ),
                severity=Severity.MEDIUM,
                model_name=model.name,
                model_path=str(model.path),
                recommendation=(
                    f"Materialize '{model.name}' as 'table' or 'incremental' to compute "
                    "it once and serve {fan_out} consumers from a pre-built result set."
                ),
                source="rule",
            )
        ]


LINEAGE_RULES: list[BaseRule] = [
    OrphanedModelRule(),
    FragileFanOutRule(),
]
