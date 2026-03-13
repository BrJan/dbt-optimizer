"""Data models for dbt-optimizer."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class Severity(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


SEVERITY_ORDER = {Severity.HIGH: 0, Severity.MEDIUM: 1, Severity.LOW: 2, Severity.INFO: 3}


@dataclass
class DbtModel:
    name: str
    path: Path
    sql: str
    materialization: str = "view"
    schema_config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    # Resolved from schema.yml / properties files
    columns: list[str] = field(default_factory=list)
    has_tests: bool = False
    has_description: bool = False

    @property
    def line_count(self) -> int:
        return len(self.sql.splitlines())

    @property
    def relative_path(self) -> str:
        return str(self.path)


@dataclass
class Suggestion:
    rule_id: str
    title: str
    description: str
    severity: Severity
    model_name: str
    model_path: str
    # Optional context (e.g. the offending SQL snippet)
    context: str = ""
    # Concrete fix recommendation
    recommendation: str = ""
    # Source of the suggestion
    source: str = "rule"  # "rule" | "ai"

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "model_name": self.model_name,
            "model_path": self.model_path,
            "context": self.context,
            "recommendation": self.recommendation,
            "source": self.source,
        }


@dataclass
class AnalysisResult:
    project_name: str
    project_path: str
    models_analyzed: int
    suggestions: list[Suggestion] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    ai_analyzed_models: int = 0

    @property
    def suggestion_count(self) -> int:
        return len(self.suggestions)

    def by_severity(self, severity: Severity) -> list[Suggestion]:
        return [s for s in self.suggestions if s.severity == severity]

    def by_model(self, model_name: str) -> list[Suggestion]:
        return [s for s in self.suggestions if s.model_name == model_name]

    def sorted_suggestions(self) -> list[Suggestion]:
        return sorted(self.suggestions, key=lambda s: (SEVERITY_ORDER[s.severity], s.model_name))

    def as_dict(self) -> dict[str, Any]:
        return {
            "project_name": self.project_name,
            "project_path": self.project_path,
            "models_analyzed": self.models_analyzed,
            "ai_analyzed_models": self.ai_analyzed_models,
            "suggestion_count": self.suggestion_count,
            "suggestions_by_severity": {
                sev.value: len(self.by_severity(sev)) for sev in Severity
            },
            "suggestions": [s.as_dict() for s in self.sorted_suggestions()],
            "errors": self.errors,
        }
