"""Service for classification and management of allocation categories."""

from app.domain.entities.allocation_category import AllocationCategory, AllocationCategoryMapping


class AllocationCategoryService:
    """Determines allocation category based on protocol and token metadata.

    Uses rule-based matching with priority tiers. Initially populated with
    sentinelwatch-inspired heuristics; can be extended with a config table later.
    """

    def __init__(self) -> None:
        """Initialize with default category rules."""
        self._rules = self._default_rules()

    def classify(self, protocol_name: str, token_symbol: str) -> AllocationCategory:
        """Classify allocation as one of: ALLOCATION, POL, PSM3, ASSET.

        Evaluates rules in priority order and returns the first match.
        Falls back to ALLOCATION if no specific rules match.
        """
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

    @staticmethod
    def _default_rules() -> list[AllocationCategoryMapping]:
        """Default classification rules based on sentinelwatch patterns.

        Rules follow Spark/Aave/Morpho conventions:
        - PSM3: Spark PSM3 protocol allocations
        - POL: Aave governance token or special liquidity positions
        - ASSET: Non-strategy holdings (e.g., staked tokens, treasury)
        - ALLOCATION: Default for all other positions
        """
        return [
            # PSM3: Spark PSM3 positions
            AllocationCategoryMapping("SparkPSM3", None, AllocationCategory.PSM3, priority=200),
            AllocationCategoryMapping("Spark PSM3", None, AllocationCategory.PSM3, priority=200),
            # POL: Aave governance liquidity and protocol-owned positions
            AllocationCategoryMapping("Aave", "AAVE", AllocationCategory.POL, priority=150),
            AllocationCategoryMapping("Aave", None, AllocationCategory.ALLOCATION, priority=100),
            # ASSET: Treasury or staking positions (commonly in audit/governance roles)
            AllocationCategoryMapping("Lido", "stETH", AllocationCategory.ASSET, priority=120),
            AllocationCategoryMapping("Curve", None, AllocationCategory.ALLOCATION, priority=100),
            # Morpho standard allocations (default to ALLOCATION)
            AllocationCategoryMapping("Morpho", None, AllocationCategory.ALLOCATION, priority=100),
        ]

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
