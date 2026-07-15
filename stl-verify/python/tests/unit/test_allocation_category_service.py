from app.domain.entities.allocation_category import AllocationCategory
from app.services.allocation_category_config import default_allocation_category_rules


def test_maple_protocol_classified_as_allocation():
    rules = default_allocation_category_rules()
    matched = next(r for r in sorted(rules, key=lambda r: -r.priority) if r.matches("maple", "syrupUSDC"))
    assert matched.category is AllocationCategory.ALLOCATION
