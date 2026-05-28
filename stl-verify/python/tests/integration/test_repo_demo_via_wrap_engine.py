"""Canonical demo: ``seeds.*`` + ``wrap_engine`` for repository integration tests.

Pattern this file pins for future repo tests:

    1. Seed prerequisites via ``tests.seeds`` helpers using ``wrap_conn``.
    2. Construct the repo with ``wrap_engine``.
    3. Exercise the repo's public API.

Per-test outer txn + nested savepoint guarantees the DB is rolled back at
teardown, so no manual cleanup is needed. Repos that issue multiple
``engine.connect()`` calls per method share the same savepoint because
``wrap_engine`` re-yields the single ``wrap_conn``.

``ReceiptTokenRepository`` is used here because its public API exercises both
a primary-key fetch (``get``) and a unique-constraint fetch
(``get_by_chain_and_address``), and its underlying SQL JOINs the ``protocol``
and ``token`` tables — a representative shape for the repo layer.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository
from app.domain.entities.allocation import EthAddress
from tests import seeds

# Synthetic addresses keep this test fully hermetic — no reliance on
# migration-seeded rows. Each ``\xNN`` byte is chosen so the address is
# obviously a fixture, not a real on-chain address.
_PROTOCOL_ADDRESS = b"\xaa" * 20
_UNDERLYING_TOKEN_ADDRESS = b"\xbb" * 20
_RECEIPT_TOKEN_ADDRESS = b"\xcc" * 20


async def _seed_receipt_token(
    conn: AsyncConnection,
    *,
    index_receipt_token_address: bool = True,
) -> tuple[int, int, int]:
    """Seed the protocol + underlying token + receipt_token chain.

    Returns ``(protocol_id, underlying_token_id, receipt_token_id)``.
    """
    protocol_id = await seeds.insert_protocol(
        conn,
        name="DemoProtocol",
        address=_PROTOCOL_ADDRESS,
    )
    underlying_token_id = await seeds.insert_token(
        conn,
        address=_UNDERLYING_TOKEN_ADDRESS,
        symbol="WUNDER",
        decimals=18,
    )
    receipt_token_id = await seeds.insert_receipt_token(
        conn,
        protocol_id=protocol_id,
        underlying_token_id=underlying_token_id,
        address=_RECEIPT_TOKEN_ADDRESS,
        symbol="rWUNDER",
        index_receipt_token_address=index_receipt_token_address,
    )
    return protocol_id, underlying_token_id, receipt_token_id


@pytest.mark.asyncio(loop_scope="session")
async def test_get_returns_receipt_token_info(
    wrap_conn: AsyncConnection,
    wrap_engine: AsyncEngine,
) -> None:
    protocol_id, underlying_token_id, rt_id = await _seed_receipt_token(wrap_conn)

    repo = ReceiptTokenRepository(wrap_engine)
    result = await repo.get(rt_id)

    assert result is not None
    assert result.receipt_token_id == rt_id
    assert result.protocol_id == protocol_id
    assert result.underlying_token_id == underlying_token_id
    assert result.protocol_name == "DemoProtocol"
    assert result.receipt_token_address == _RECEIPT_TOKEN_ADDRESS
    assert isinstance(result.receipt_token_token_id, int)


@pytest.mark.asyncio(loop_scope="session")
async def test_get_returns_none_when_receipt_token_address_token_row_missing(
    wrap_conn: AsyncConnection,
    wrap_engine: AsyncEngine,
) -> None:
    """``receipt_token_token_id`` is None until the indexer materialises the token row."""
    _, _, rt_id = await _seed_receipt_token(wrap_conn, index_receipt_token_address=False)

    repo = ReceiptTokenRepository(wrap_engine)
    result = await repo.get(rt_id)

    assert result is not None
    assert result.receipt_token_id == rt_id
    assert result.receipt_token_token_id is None


@pytest.mark.asyncio(loop_scope="session")
async def test_get_by_chain_and_address_is_case_insensitive(
    wrap_conn: AsyncConnection,
    wrap_engine: AsyncEngine,
) -> None:
    _, _, rt_id = await _seed_receipt_token(wrap_conn)

    repo = ReceiptTokenRepository(wrap_engine)
    upper = await repo.get_by_chain_and_address(1, EthAddress("0x" + "CC" * 20))
    lower = await repo.get_by_chain_and_address(1, EthAddress("0x" + "cc" * 20))

    assert upper is not None
    assert lower is not None
    assert upper.receipt_token_id == rt_id
    assert lower.receipt_token_id == rt_id


@pytest.mark.asyncio(loop_scope="session")
async def test_get_returns_none_for_unknown_id(wrap_engine: AsyncEngine) -> None:
    repo = ReceiptTokenRepository(wrap_engine)
    assert await repo.get(99_999) is None


@pytest.mark.asyncio(loop_scope="session")
async def test_get_by_chain_and_address_returns_none_for_unknown_address(
    wrap_engine: AsyncEngine,
) -> None:
    repo = ReceiptTokenRepository(wrap_engine)
    assert await repo.get_by_chain_and_address(1, EthAddress("0x" + "ff" * 20)) is None
