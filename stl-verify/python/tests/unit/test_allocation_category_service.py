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


def test_psm3_protocol_classified_as_psm3():
    """Go-emitted protocol_name='psm3' must classify as PSM3, not ALLOCATION.

    The legacy SparkPSM3/Spark PSM3 rules cover the historic label; the added
    substring rule for 'psm3' covers the Go tracker's protocol field.
    """
    service = AllocationCategoryService()
    assert service.classify("psm3", "PSM3") is AllocationCategory.PSM3
    assert service.classify("SparkPSM3", "PSM3") is AllocationCategory.PSM3
    assert service.classify("Spark PSM3", "PSM3") is AllocationCategory.PSM3
    # Substring containment: any protocol containing psm3 should map to PSM3,
    # while an unrelated protocol falls back to ALLOCATION.
    assert service.classify("aave", "PSM3") is not AllocationCategory.PSM3


def test_custody_category_has_label_and_description():
    service = AllocationCategoryService()
    assert service.get_category_label(AllocationCategory.CUSTODY) == "Custody"
    assert service.get_category_description(AllocationCategory.CUSTODY) != ""
