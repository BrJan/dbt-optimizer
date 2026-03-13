"""AI-powered SQL analysis using Claude."""

from __future__ import annotations

import json
import textwrap
from typing import TYPE_CHECKING

import anthropic

from .models import DbtModel, Severity, Suggestion

if TYPE_CHECKING:
    pass

# Max tokens of SQL to send per model (prevents token overflow on very large models)
_MAX_SQL_CHARS = 6000

_SYSTEM_PROMPT = textwrap.dedent("""\
    You are an expert dbt and SQL performance engineer. Your job is to review dbt SQL models
    and return actionable optimization suggestions.

    Focus on:
    - Query performance (partition pruning, clustering, join efficiency, aggregation strategy)
    - Data correctness risks (fanout from joins, grain mismatches, implicit type coercions)
    - dbt modeling best practices (appropriate materialization, layer separation, ref/source usage)
    - Maintainability (overly complex logic that should be split, missing documentation signals)

    Rules:
    - Only return suggestions that are concrete and actionable.
    - Do NOT repeat suggestions already covered by static rules (SELECT *, DISTINCT overuse,
      missing tests, hardcoded dates, direct table refs, UNION without ALL).
    - Be specific: reference the actual SQL pattern you're flagging.
    - Return between 1 and 5 suggestions per model. If the model is clean, return an empty list.
    - Severity must be one of: "high", "medium", "low", "info"

    Respond ONLY with a JSON array. Example:
    [
      {
        "title": "Unbounded LIKE scan on large column",
        "description": "The filter `WHERE email LIKE '%@%'` leads to a full column scan.",
        "severity": "medium",
        "recommendation": "If filtering for valid emails, consider a regex check or upstream validation model.",
        "context": "WHERE email LIKE '%@%'"
      }
    ]
""")


def _build_user_prompt(model: DbtModel) -> str:
    sql_source = model.effective_sql
    sql_snippet = sql_source[:_MAX_SQL_CHARS]
    truncated = len(sql_source) > _MAX_SQL_CHARS
    trunc_note = (
        f"\n... [truncated at {_MAX_SQL_CHARS} chars of {len(sql_source)} total]"
        if truncated else ""
    )
    sql_label = "Compiled SQL (Jinja resolved)" if model.compiled_sql else "SQL (raw, Jinja not resolved)"

    lineage_section = ""
    if model.upstream_models or model.downstream_models:
        up = ", ".join(model.upstream_models) if model.upstream_models else "none"
        dn = ", ".join(model.downstream_models) if model.downstream_models else "none"
        lineage_section = textwrap.dedent(f"""
        Lineage:
          Upstream models  : {up}
          Downstream models: {dn}
        """)

    return textwrap.dedent(f"""\
        Analyze this dbt model and return optimization suggestions as a JSON array.

        Model name: {model.name}
        Materialization: {model.materialization}
        Line count: {model.line_count}
        Has tests: {model.has_tests}
        Has description: {model.has_description}{lineage_section}
        {sql_label}:
        ```sql
        {sql_snippet}{trunc_note}
        ```
    """)


def _parse_response(response_text: str, model: DbtModel) -> list[Suggestion]:
    """Parse JSON array from Claude response."""
    # Strip markdown code fences if present
    text = response_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)

    try:
        items = json.loads(text)
    except json.JSONDecodeError:
        # Try to extract JSON array from the text
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if not match:
            return []
        try:
            items = json.loads(match.group(0))
        except json.JSONDecodeError:
            return []

    suggestions: list[Suggestion] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        severity_str = item.get("severity", "info").lower()
        try:
            severity = Severity(severity_str)
        except ValueError:
            severity = Severity.INFO

        suggestions.append(Suggestion(
            rule_id="AI",
            title=item.get("title", "AI Suggestion"),
            description=item.get("description", ""),
            severity=severity,
            model_name=model.name,
            model_path=str(model.path),
            context=item.get("context", ""),
            recommendation=item.get("recommendation", ""),
            source="ai",
        ))
    return suggestions


import re  # noqa: E402 (needed by _parse_response above)


class AIAnalyzer:
    """Uses Claude to perform deep SQL analysis on dbt models."""

    def __init__(self, api_key: str | None = None, model: str = "claude-sonnet-4-6") -> None:
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def analyze_model(self, dbt_model: DbtModel) -> list[Suggestion]:
        """Run AI analysis on a single model. Returns suggestions."""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": _build_user_prompt(dbt_model)}],
            )
            text = response.content[0].text
            return _parse_response(text, dbt_model)
        except anthropic.AuthenticationError:
            raise
        except Exception as e:
            # Gracefully degrade — return empty on transient errors
            return []

    def analyze_models(
        self,
        models: list[DbtModel],
        progress_callback=None,
    ) -> tuple[list[Suggestion], list[str]]:
        """Analyze a list of models. Returns (suggestions, errors)."""
        all_suggestions: list[Suggestion] = []
        errors: list[str] = []

        for i, model in enumerate(models):
            if progress_callback:
                progress_callback(i, len(models), model.name)
            try:
                suggestions = self.analyze_model(model)
                all_suggestions.extend(suggestions)
            except anthropic.AuthenticationError:
                errors.append("Invalid Anthropic API key. AI analysis skipped.")
                break
            except Exception as e:
                errors.append(f"AI analysis failed for {model.name}: {e}")

        return all_suggestions, errors
