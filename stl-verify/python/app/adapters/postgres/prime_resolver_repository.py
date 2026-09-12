"""Resolve a prime name, vault address or proxy address to one prime."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._reading import reading
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity
from app.domain.exceptions import InvalidPrimeIdentifierError

# `prime.name`, `prime.vault_address` and `prime_proxy.proxy_address` are each UNIQUE, so at
# most one row matches. The ORDER BY settles only one prime's proxy equalling another's vault.
RESOLVE_PRIME_SQL = """
    SELECT
        p.id,
        p.name,
        p.prime_key,
        encode(p.vault_address, 'hex') AS vault_hex
    FROM prime p
    WHERE p.name = :name
       OR p.vault_address = decode(:address_hex, 'hex')
       OR EXISTS (
            SELECT 1
            FROM prime_proxy pp
            WHERE pp.prime_id = p.id
              AND pp.proxy_address = decode(:address_hex, 'hex')
          )
    ORDER BY (p.vault_address = decode(:address_hex, 'hex')) DESC NULLS LAST, p.id
    LIMIT 1
"""


def _address_hex(identifier: str) -> str | None:
    """Return the unprefixed hex of an address-shaped ``identifier``, else ``None``.

    A ``0x`` prefix commits the value to being an address, so a malformed one raises
    rather than falling through to a name lookup that resolves to nothing.
    """
    if not identifier.lower().startswith("0x"):
        return None
    try:
        return EthAddress(identifier).hex
    except ValueError as exc:
        raise InvalidPrimeIdentifierError(f"Invalid prime identifier: {identifier}") from exc


class PrimeResolverRepository:
    """PostgreSQL adapter for :class:`app.ports.prime_resolver.PrimeResolver`."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def resolve(self, identifier: str) -> PrimeIdentity | None:
        address_hex = _address_hex(identifier)
        params = {"name": None if address_hex else identifier, "address_hex": address_hex}

        async with reading(self._engine, what=f"resolving prime {identifier}") as conn:
            row = (await conn.execute(text(RESOLVE_PRIME_SQL), params)).fetchone()

        if row is None:
            return None
        return PrimeIdentity(
            id=row.id,
            name=row.name,
            prime_key=row.prime_key,
            vault_address=EthAddress("0x" + row.vault_hex),
        )
