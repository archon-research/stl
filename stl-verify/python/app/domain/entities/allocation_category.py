"""Allocation category classification and mapping."""

from dataclasses import dataclass
from enum import Enum


class AllocationCategory(str, Enum):
    """Classification of allocation position types across primes."""

    ALLOCATION = "allocation"  # Standard deployed capital
    POL = "pol"  # Protocol-owned liquidity
    PSM3 = "psm3"  # Peg stability mechanism (Spark PSM3 variant)
    ASSET = "asset"  # Non-strategy asset holding


@dataclass(frozen=True)
class AllocationCategoryMapping:
    """Rule-based mapping from protocol/token metadata to allocation category."""

    protocol_name: str
    token_symbol: str | None  # None means match all tokens for protocol
    category: AllocationCategory
    priority: int = 100  # Higher priority rules evaluated first; default 100

    def matches(self, protocol_name: str, token_symbol: str) -> bool:
        """Check if this rule matches the given protocol and token.

        Protocol matching uses substring containment so a rule for "Aave" matches
        protocol names like "aave_v3" or "aave v2" without requiring an exact string.
        """
        protocol_match = self.protocol_name.lower() in protocol_name.lower()
        token_match = self.token_symbol is None or self.token_symbol.lower() == token_symbol.lower()
        return protocol_match and token_match
