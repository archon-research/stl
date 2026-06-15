from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from app.domain.entities.allocation import DirectAssetHolding, ReceiptTokenPosition
from app.main import app


@pytest.fixture(autouse=True)
def clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def make_receipt_token_position() -> Callable[..., ReceiptTokenPosition]:
    def _make(**overrides: Any) -> ReceiptTokenPosition:
        defaults: dict[str, Any] = dict(
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

    return _make


@pytest.fixture
def make_direct_asset_holding() -> Callable[..., DirectAssetHolding]:
    def _make(**overrides: Any) -> DirectAssetHolding:
        defaults: dict[str, Any] = dict(
            chain_id=1,
            token_id=99,
            token_address="0x" + "c" * 40,
            symbol="PYUSD",
            balance=Decimal("250.0"),
        )
        defaults.update(overrides)
        return DirectAssetHolding(**defaults)

    return _make
