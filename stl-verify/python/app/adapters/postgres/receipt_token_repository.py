from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.logging import get_logger
from app.risk_engine.mapping import MappingError

logger = get_logger(__name__)

# LEFT JOIN to `token` on (chain_id, address) so callers can query
# allocation_position / token_total_supply by token_id when available. The
# receipt-token-address `token` row is created lazily — typically when the
# prime-allocation-indexer first writes an allocation_position for it — so a
# fresh receipt_token can exist without a matching token row. Returning the
# receipt-token metadata anyway lets the API distinguish "not indexed yet"
# (warm-up → 503) from "unknown receipt token" (→ 404), and lets non-Aave
# branches (e.g. Morpho) resolve without ever needing receipt_token_token_id.
_RECEIPT_TOKEN_SQL = """
SELECT rt.id, rt.protocol_id, rt.underlying_token_id, rt.receipt_token_address,
       p.chain_id, p.name AS protocol_name,
       t.id AS receipt_token_token_id
FROM receipt_token rt
JOIN protocol p ON p.id = rt.protocol_id
LEFT JOIN token t ON t.chain_id = p.chain_id AND t.address = rt.receipt_token_address
WHERE rt.id = :receipt_token_id
"""


class ReceiptTokenRepository:
    """Look up a receipt token and its associated protocol metadata."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get(self, receipt_token_id: int) -> ReceiptTokenInfo | None:
        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(_RECEIPT_TOKEN_SQL), {"receipt_token_id": receipt_token_id})
                row = result.fetchone()
        except SQLAlchemyError:
            logger.exception("receipt_token_repository: DB error fetching receipt_token_id=%d", receipt_token_id)
            raise

        if row is None:
            return None

        return ReceiptTokenInfo(
            receipt_token_id=row.id,
            protocol_id=row.protocol_id,
            underlying_token_id=row.underlying_token_id,
            receipt_token_address=bytes(row.receipt_token_address),
            chain_id=row.chain_id,
            protocol_name=row.protocol_name,
            receipt_token_token_id=row.receipt_token_token_id,
        )


async def resolve_receipt_token_mapping(
    raw_mapping: list[tuple[int, bytes, str]],
    engine: AsyncEngine,
) -> dict[int, str]:
    """Resolve ``(chain_id, address, rating_id)`` tuples to ``{receipt_token_id: rating_id}``.

    Raises ``MappingError`` if any ``(chain_id, address)`` pair is not
    found in the ``receipt_token`` table.
    """
    if not raw_mapping:
        return {}

    resolved: dict[int, str] = {}
    async with engine.connect() as conn:
        for chain_id, address, rating_id in raw_mapping:
            row = await conn.execute(
                text("SELECT id FROM receipt_token WHERE chain_id = :chain_id AND receipt_token_address = :address"),
                {"chain_id": chain_id, "address": address},
            )
            receipt_token_id = row.scalar_one_or_none()
            if receipt_token_id is None:
                addr_hex = "0x" + address.hex()
                raise MappingError(
                    f"asset mapping references unknown receipt token: chain_id={chain_id} address={addr_hex}"
                )
            resolved[receipt_token_id] = rating_id
    return resolved
