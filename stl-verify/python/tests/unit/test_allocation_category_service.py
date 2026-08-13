from app.domain.entities.allocation_category import AllocationCategory
from app.services.allocation_category_config import default_allocation_category_rules
from app.services.allocation_category_service import AllocationCategoryService


def test_maple_protocol_classified_as_allocation():
    rules = default_allocation_category_rules()
    matched = next(r for r in sorted(rules, key=lambda r: -r.priority) if r.matches("maple", "syrupUSDC"))
    assert matched.category is AllocationCategory.ALLOCATION


def test_anchorage_protocol_classified_as_custody():
    """Off-chain Anchorage BTC custody surfaces under its own CUSTODY category,
    not the ALLOCATION fallback that any unmatched protocol lands in.
    """
    assert AllocationCategoryService().classify("anchorage", "BTC") is AllocationCategory.CUSTODY


def test_custody_category_has_label_and_description():
    service = AllocationCategoryService()
    assert service.get_category_label(AllocationCategory.CUSTODY) == "Custody"
    assert service.get_category_description(AllocationCategory.CUSTODY) != ""
