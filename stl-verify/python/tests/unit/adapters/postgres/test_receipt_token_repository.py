from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository


def _engine_returning(row) -> MagicMock:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=row)))
    engine.connect.return_value = conn
    return engine


@pytest.mark.asyncio
async def test_get_by_chain_and_address_includes_the_underlying_identity() -> None:
    # The receipt-token lookup joins the underlying `token` row in the same
    # query, so the reference-allocation enrichment costs no second round trip.
    row = MagicMock(
        id=41,
        protocol_id=1,
        underlying_token_id=7,
        receipt_token_address=b"\xcd" * 20,
        chain_id=1,
        protocol_name="SparkLend",
        receipt_token_token_id=None,
        underlying_token_address=b"\x77" * 20,
        underlying_symbol="USDT",
    )
    repository = ReceiptTokenRepository(_engine_returning(row))

    result = await repository.get_by_chain_and_address(1, MagicMock(to_bytes=lambda: b"\xcd" * 20))

    assert result is not None
    assert result.underlying_token_id == 7
    assert result.underlying_symbol == "USDT"
    assert result.underlying_token_address_hex == "0x" + "77" * 20


@pytest.mark.asyncio
async def test_list_protocol_pairs_returns_receipt_token_id_and_protocol_name() -> None:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(
        return_value=MagicMock(
            fetchall=MagicMock(
                return_value=[
                    MagicMock(receipt_token_id=1, protocol_name="SparkLend"),
                    MagicMock(receipt_token_id=2, protocol_name="Morpho Blue"),
                ]
            )
        )
    )
    engine.connect.return_value = conn

    repository = ReceiptTokenRepository(engine)

    result = await repository.list_protocol_pairs()

    assert [(item.receipt_token_id, item.protocol_name) for item in result] == [
        (1, "SparkLend"),
        (2, "Morpho Blue"),
    ]
