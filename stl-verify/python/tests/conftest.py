from datetime import datetime, timezone
from decimal import Decimal

from app.domain.entities.allocation import AllocationPosition


def make_allocation_position(**overrides) -> AllocationPosition:
    defaults = dict(
        id=1,
        chain_id=1,
        name="spark",
        proxy_address="0xabc",
        token_address="0xdef",
        token_symbol="USDC",
        token_decimals=6,
        balance=Decimal("100.0"),
        scaled_balance=None,
        block_number=1000,
        block_version=0,
        tx_hash="0x123",
        log_index=0,
        tx_amount=Decimal("100.0"),
        direction="in",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    defaults.update(overrides)
    return AllocationPosition(**defaults)
