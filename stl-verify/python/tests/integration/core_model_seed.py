"""Seed helpers shared by the CORE model reader integration tests.

These take SQLAlchemy async connections (the readers' own engine type), unlike
``seed.py`` which is asyncpg-based.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

# A second token that copies a real symbol; one fixed address so fixtures can
# delete it and the tests that insert it can never drift apart.
SPOOF_TOKEN_ADDRESS_HEX = "5e" * 20


async def seed_spoof_token(conn: AsyncConnection, symbol: str, decimals: int = 18) -> int:
    return (
        await conn.execute(
            text("""
                INSERT INTO token (chain_id, address, symbol, decimals)
                VALUES (1, decode(:addr, 'hex'), :symbol, :decimals)
                ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING id
            """),
            {"addr": SPOOF_TOKEN_ADDRESS_HEX, "symbol": symbol, "decimals": decimals},
        )
    ).scalar_one()


async def delete_spoof_token(conn: AsyncConnection) -> None:
    await conn.execute(
        text("DELETE FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
        {"addr": SPOOF_TOKEN_ADDRESS_HEX},
    )
