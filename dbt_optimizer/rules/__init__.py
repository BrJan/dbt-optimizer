"""Rule-based SQL analysis checks."""

from .lineage_rules import LINEAGE_RULES
from .sql_rules import ALL_RULES as ALL_SQL_RULES

# ALL_RULES includes both static SQL rules and lineage rules.
# Lineage rules silently no-op when MCP data is absent, so it is safe
# to include them unconditionally.
ALL_RULES = ALL_SQL_RULES + LINEAGE_RULES

__all__ = ["ALL_RULES", "ALL_SQL_RULES", "LINEAGE_RULES"]
