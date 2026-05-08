from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository


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
