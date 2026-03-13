"""Base class for rule-based checks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models import DbtModel, Suggestion


class BaseRule(ABC):
    rule_id: str
    title: str

    @abstractmethod
    def check(self, model: "DbtModel") -> list["Suggestion"]:
        """Return zero or more suggestions for the given model."""
        ...
