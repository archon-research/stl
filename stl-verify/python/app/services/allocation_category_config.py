"""Default allocation category mapping configuration.

This is intentionally file-based for now and can later be replaced by a database
mapping table without changing service callers.
"""

from app.domain.entities.allocation_category import AllocationCategory, AllocationCategoryMapping


def default_allocation_category_rules() -> list[AllocationCategoryMapping]:
    """Return default category rules in descending intent specificity."""
    return [
        # PSM3: Spark PSM3 positions.
        AllocationCategoryMapping("SparkPSM3", None, AllocationCategory.PSM3, priority=200),
        AllocationCategoryMapping("Spark PSM3", None, AllocationCategory.PSM3, priority=200),
        # POL: Protocol-owned liquidity and governance positions.
        AllocationCategoryMapping("Aave", "AAVE", AllocationCategory.POL, priority=150),
        AllocationCategoryMapping("Aave", None, AllocationCategory.ALLOCATION, priority=100),
        # ASSET: Non-strategy holdings.
        AllocationCategoryMapping("Lido", "stETH", AllocationCategory.ASSET, priority=120),
        AllocationCategoryMapping("Curve", None, AllocationCategory.ALLOCATION, priority=100),
        # Standard lending allocations.
        AllocationCategoryMapping("Morpho", None, AllocationCategory.ALLOCATION, priority=100),
        AllocationCategoryMapping("maple", None, AllocationCategory.ALLOCATION, priority=100),
    ]
