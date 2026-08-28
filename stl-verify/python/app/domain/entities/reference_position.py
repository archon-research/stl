"""One position from Sky's upstream balance sheet.

The counterpart to :class:`~app.domain.entities.allocation.AllocationResponse`'s
indexed rows, and deliberately *not* the counterpart to
:class:`~app.domain.entities.reference_risk_capital.ReferenceAllocation`.

Those two upstream feeds answer different questions with confusingly similar
names. The Star monitor's allocations carry ``exposure`` — the priced,
risk-bearing figure its ``total_exposure`` decomposes into, 11 rows for spark
summing to 2.17bn. This feed carries ``assets`` — the whole balance sheet, 59
rows summing to 3.31bn, which reconciles to the prime's own ``assets`` and is
the same measurement as STL's allocation amounts. Serving one where the other
is expected puts figures 1.5x apart in the same column.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ReferencePosition:
    """A prime's holding in one token, on one chain.

    ``token_address`` is not always an address: Uniswap V4 positions carry a
    32-byte pool id (66 chars) in the same field, which by construction cannot
    resolve to a receipt token. Callers join on it defensively and tolerate a
    miss rather than treating it as an address.

    Two fields upstream serves are deliberately absent here, dropped because
    nothing read them rather than because they are uninteresting:

    ``wallet_address`` is the ALM proxy holding the position. It is now part of
    ``prime_reference_position``'s row identity in storage (VEC-NA: the same
    (network, token_address) legitimately recurs under a prime's different
    proxies, verified live on grove) and is parsed by the Go client and used in
    this read path's ``DISTINCT ON`` so both proxies' rows still serve — but it
    is not itself in the response, so two positions that differ only by
    wallet still read as indistinguishable duplicates here. Serving it is one
    field here and one in the response contract, once a consumer needs to
    tell the proxies apart.

    ``allocation_type`` is upstream's own category vocabulary (``allocation`` /
    ``asset`` / ``pol`` / ``psm3``), which maps closely onto the ``category``
    this codebase derives from protocol plus symbol. Serving upstream's answer
    instead of the derived one would change what every consumer of that column
    sees, so it is out of scope here rather than dismissed as an idea.
    """

    protocol_name: str
    network: str
    symbol: str
    name: str
    token_address: str
    # The full holding. `allocated` and `idle` decompose it — a position can be
    # deployed into a protocol or sitting in the proxy, and upstream reports
    # both legs rather than only the sum.
    assets_usd: Decimal
    allocated_assets_usd: Decimal | None
    idle_assets_usd: Decimal | None
    # Resolved against STL's token registry in the repository's SQL.
    # ``None`` whenever the join cannot be made — a pool id in place of an
    # address, an unmapped network, or a token STL does not index.
    receipt_token_id: int | None = None
    # This feed never reports an underlying itself. When `receipt_token_id`
    # resolves, id/address carry the registry's own underlying for that
    # receipt token (the same fact `receipt_token` FKs express for the
    # indexed path) and are both `None` when it does not. `underlying_symbol`
    # is looser: `token.symbol` is independently nullable, so a resolved
    # token can still read `""` while awaiting its on-chain symbol.
    underlying_token_id: int | None = None
    underlying_token_address: str | None = None
    underlying_symbol: str = ""
    # Both ``None`` for a network upstream has added that STL has no chain id
    # for. Callers must not substitute a placeholder id: 0 already means
    # off-chain custody, so an unmapped EVM position would read as one.
    chain_id: int | None = None
    chain: str | None = None


@dataclass(frozen=True)
class ReferencePositionSnapshot:
    """Every position a prime held at one observed instant.

    The positions ride a stamp rather than carrying one each: an indexer cycle
    writes the whole balance sheet under a single ``synced_at``, so a stamp per
    row would be one instant repeated.
    """

    synced_at: datetime
    positions: tuple[ReferencePosition, ...]
