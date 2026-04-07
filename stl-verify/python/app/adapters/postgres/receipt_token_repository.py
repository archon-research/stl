import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.receipt_token import ReceiptTokenInfo

logger = logging.getLogger(__name__)

_RECEIPT_TOKEN_SQL = """
SELECT rt.id, rt.protocol_id, rt.underlying_token_id, rt.receipt_token_address,
       p.chain_id, p.name AS protocol_name
FROM receipt_token rt
JOIN protocol p ON p.id = rt.protocol_id
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
        )
