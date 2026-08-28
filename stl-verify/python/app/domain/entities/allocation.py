import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EthAddress(str):
    """Value object for a validated Ethereum address (0x-prefixed, 40 hex chars).

    Subclasses ``str`` so it interoperates with Pydantic, JSON, and OpenAPI
    natively. Preserves the original case of the input (e.g. EIP-55
    checksummed form) while comparing and hashing case-insensitively.
    """

    __slots__ = ()

    def __new__(cls, value: str) -> "EthAddress":
        if not isinstance(value, str) or not _ETH_ADDRESS_RE.match(value):
            raise ValueError(f"Invalid Ethereum address: {value!r} (expected 0x followed by 40 hex characters)")
        return super().__new__(cls, value)

    @property
    def hex(self) -> str:
        """The raw 40-character hex string without 0x prefix."""
        return self.removeprefix("0x")

    def to_bytes(self) -> bytes:
        """Return the address as raw 20 bytes (matches DB ``BYTEA`` columns)."""
        return bytes.fromhex(self.hex)

    def __repr__(self) -> str:
        return f"EthAddress('{self!s}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EthAddress):
            return str.lower(self) == str.lower(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(str.lower(self))

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, core_schema.str_schema())


def as_address(value: str) -> EthAddress | None:
    """Return ``value`` as an address, or ``None`` when it is not one.

    For identifiers from outside STL, where "not an address" is an expected
    answer rather than a fault — a Uniswap V4 position names itself by 32-byte
    pool id in the field an address would occupy.
    """
    try:
        return EthAddress(value)
    except ValueError:
        return None


@dataclass(frozen=True)
class ReceiptTokenPosition:
    """A receipt token held by a prime, enriched with its underlying token info."""

    chain_id: int
    receipt_token_id: int
    receipt_token_address: str
    underlying_token_id: int
    underlying_token_address: str
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: datetime | None = None
    latest_activity_action: str | None = None
    latest_activity_amount: Decimal | None = None


@dataclass(frozen=True)
class DirectAssetHolding:
    """A token held directly by a prime that is not a registered receipt-token wrapper.

    The ``underlying_*`` fields are set when the holding is allowlisted for
    underlying-value pricing and its row carries a resolvable underlying (the
    pricing basis for ``amount_usd``), and are always set or unset together.
    For non-allowlisted holdings ``None`` means the token prices by its own
    oracle and the underlying is the token itself; for allowlisted holdings
    ``None`` marks a row with no resolvable underlying (e.g. written before
    the type's valuation deployed), a surfaced coverage gap priced as NULL,
    never by the share-count balance.
    """

    chain_id: int
    token_id: int
    token_address: str
    symbol: str
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: datetime | None = None
    latest_activity_action: str | None = None
    latest_activity_amount: Decimal | None = None
    underlying_token_id: int | None = None
    underlying_token_address: str | None = None
    underlying_symbol: str | None = None


@dataclass(frozen=True)
class AnchorageCustodyHolding:
    """Off-chain custodied collateral securing a prime's Anchorage loan.

    One row per ``(asset_type, custody_type)`` after collapsing every package in
    the prime's latest snapshot cohort. Column semantics come straight from the
    Anchorage feed (see ``_ANCHORAGE_CUSTODY_HOLDINGS_SQL``):

    * ``amount_usd``     — SUM(exposure_value): the loan drawn against the
      collateral. This is the figure surfaced as the allocation's USD value
      (matches VEC-499 / skyeco's concept).
    * ``collateral_usd`` — SUM(package_value): the BTC collateral's market
      value. Carried alongside so the surfaced figure can flip from the loan to
      the collateral in one line at the endpoint, without a schema change.
    * ``balance``        — SUM(asset_quantity): collateral in native units (BTC).
    * ``as_of``          — the cohort's ``snapshot_time``. Surfaced verbatim as
      the row's latest-activity timestamp so a frozen upstream feed reads as
      honestly stale rather than hiding the staleness.
    """

    symbol: str
    custody_type: str
    balance: Decimal
    amount_usd: Decimal
    collateral_usd: Decimal
    as_of: datetime


@dataclass(frozen=True)
class Psm3Position:
    """One tracked ALM proxy's LP stake in a Spark PSM3 pool at the latest sweep.

    Mirrors ``psm3_alm_shares`` — one row per (chain_id, alm_address) at the
    latest block for that holder. ``shares`` and ``asset_value`` are stored
    raw at 1e18 in the DB and normalized here to token-unit/USD scale
    (``/ 1e18``), matching the par valuation semantics of ``PSM3.totalAssets()``
    (see ``docs/psm3_spec.md``). ``asset_value`` is par, not market-priced —
    it equals ``shares * total_assets / total_shares`` on the matching
    ``psm3_reserves`` row.
    """

    chain_id: int
    psm3_address: str
    alm_address: str
    shares: Decimal
    asset_value: Decimal
    block_number: int
    block_timestamp: datetime | None


@dataclass(frozen=True)
class Prime:
    """One of a prime's proxy wallets, as surfaced by ``/v1/primes``.

    A prime has several of these — one ALM proxy per chain it allocates on — so
    ``name`` is not a key. ``chain_id`` comes from the position rows; ``chain`` is
    derived from ``chain_id`` via ``chain_names.chain_name_for`` and is ``None``
    for a chain the vocabulary has not been taught.

    ``prime_vault_address`` is the owning prime's ``prime.vault_address`` — stable,
    unique, and the same across every proxy of a prime, so consumers group rows by
    it. ``None`` when the prime has no vault address on record.
    """

    id: str
    name: str
    address: str
    chain_id: int
    chain: str | None
    # An allocation venue by construction: SubProxy treasury wallets hold no
    # allocations, so they are excluded rather than listed with another role.
    role: Literal["alm"]
    prime_vault_address: str | None = None


@dataclass(frozen=True)
class ChainMetadata:
    chain_id: int
    name: str


@dataclass(frozen=True)
class ProtocolMetadata:
    id: int
    chain_id: int
    encode: str
    name: str
