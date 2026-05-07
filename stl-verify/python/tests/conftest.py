from decimal import Decimal

from app.domain.entities.allocation import ReceiptTokenPosition


def make_receipt_token_position(**overrides) -> ReceiptTokenPosition:
    defaults = dict(
        chain_id=1,
        receipt_token_id=1,
        receipt_token_address="0x" + "a" * 40,
        underlying_token_id=10,
        underlying_token_address="0x" + "b" * 40,
        symbol="aUSDC",
        underlying_symbol="USDC",
        protocol_name="aave_v3",
        balance=Decimal("100.0"),
    )
    defaults.update(overrides)
    return ReceiptTokenPosition(**defaults)
