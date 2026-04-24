"""Integration tests for the risk API warm-up behavior.

These tests cover the case where a ``receipt_token`` row exists before the
prime-allocation-indexer has created the matching ``token`` row for the
receipt token's own address. The API should treat that as "data not indexed
yet" (HTTP 503), not "unknown receipt token" (HTTP 404).
"""

import asyncio

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.main import create_app

_RECEIPT_TOKEN_ADDRESS_HEX = "59cd1c87501baa753d0b5b5ab5d8416a45cd71dc"


async def _seed(async_url: str) -> int:
    """Insert a SparkLend receipt_token row without creating its token row."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            protocol_id = (
                await conn.execute(text("SELECT id FROM protocol WHERE name = 'SparkLend' AND chain_id = 1"))
            ).scalar_one()
            underlying_token_id = (
                await conn.execute(text("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
            ).scalar_one()

            receipt_token_id = (
                await conn.execute(
                    text(
                        """
                        INSERT INTO receipt_token
                            (protocol_id, underlying_token_id, receipt_token_address, symbol,
                             created_at_block, chain_id)
                        VALUES (:protocol_id, :underlying_token_id, decode(:addr, 'hex'), 'spWETH-warmup', 16776402, 1)
                        ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                            DO UPDATE SET symbol = EXCLUDED.symbol
                        RETURNING id
                        """
                    ),
                    {
                        "protocol_id": protocol_id,
                        "underlying_token_id": underlying_token_id,
                        "addr": _RECEIPT_TOKEN_ADDRESS_HEX,
                    },
                )
            ).scalar_one()
            return int(receipt_token_id)
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def seeded_receipt_token_id(async_db_url: str) -> int:
    """A receipt_token that exists before its own token row has been indexed."""
    return asyncio.run(_seed(async_db_url))


@pytest.fixture()
def client(async_db_url: str):
    """Return a TestClient wired to the module's isolated database."""
    test_app = create_app(Settings(database_url=SecretStr(async_db_url)))
    with TestClient(test_app) as c:
        yield c


def test_risk_breakdown_returns_503_when_receipt_token_token_row_is_missing(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    response = client.get(f"/v1/risk/{seeded_receipt_token_id}/breakdown")

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "share_data_missing"


def test_risk_bad_debt_returns_503_when_receipt_token_token_row_is_missing(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    response = client.get(f"/v1/risk/{seeded_receipt_token_id}/bad-debt?gap_pct=0.1")

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "share_data_missing"
