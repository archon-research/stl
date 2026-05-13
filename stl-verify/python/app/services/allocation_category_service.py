"""Service for classification and management of allocation categories."""

from app.domain.entities.allocation_category import AllocationCategory, AllocationCategoryMapping
from app.services.allocation_category_config import default_allocation_category_rules


class AllocationCategoryService:
    """Determines allocation category based on protocol and token metadata.

    Uses rule-based matching with priority tiers from a config module. The config
    can be replaced by a database-backed mapping table without changing callers.
    """

    def __init__(self, rules: list[AllocationCategoryMapping] | None = None) -> None:
        """Initialize with provided rules or default config rules."""
        self._rules = rules if rules is not None else default_allocation_category_rules()

    def classify(self, protocol_name: str | None, token_symbol: str) -> AllocationCategory:
        """Classify allocation as one of: ALLOCATION, POL, PSM3, ASSET.

        Returns ASSET when ``protocol_name`` is None — direct asset holdings
        with no registered protocol wrapper are treasury/non-strategy positions
        by definition. Otherwise evaluates rules in priority order and falls
        back to ALLOCATION if no rule matches.
        """
        if protocol_name is None:
            return AllocationCategory.ASSET

        # Sort by priority (descending) then by rule specificity
        # (prefer rules with specific tokens over protocol-only rules)
        sorted_rules = sorted(
            self._rules,
            key=lambda r: (-r.priority, r.token_symbol is None),
        )

        for rule in sorted_rules:
            if rule.matches(protocol_name, token_symbol):
                return rule.category

        return AllocationCategory.ALLOCATION

    def get_category_label(self, category: AllocationCategory) -> str:
        """Return user-friendly display label for category."""
        labels = {
            AllocationCategory.ALLOCATION: "Allocation",
            AllocationCategory.POL: "Protocol-Owned Liquidity",
            AllocationCategory.PSM3: "Peg Stability Mechanism",
            AllocationCategory.ASSET: "Asset",
        }
        return labels.get(category, category.value)

    def get_category_description(self, category: AllocationCategory) -> str:
        """Return tooltip/description for category."""
        descriptions = {
            AllocationCategory.ALLOCATION: "Deployed capital in active lending markets or liquidity pools",
            AllocationCategory.POL: "Protocol-owned liquidity or governance assets",
            AllocationCategory.PSM3: "Peg stability mechanism reserves (Spark PSM3 variant)",
            AllocationCategory.ASSET: "Non-strategy asset holdings or treasury positions",
        }
        return descriptions.get(category, "")
