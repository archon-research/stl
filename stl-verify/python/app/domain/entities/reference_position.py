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

    ``wallet_address`` is the ALM proxy holding the position — the grain Verify
    stores positions at, and absent from the Star monitor's feed, which reports
    per prime. It was carried as the enabler for a future merge keyed on
    ``(chain, token, proxy)``. Reinstating it is one field here and one
    ``required_text`` call in the adapter.

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
    # Resolved against STL's token registry by the service, not the adapter.
    # ``None`` whenever the join cannot be made — a pool id in place of an
    # address, an unmapped network, or a token STL does not index.
    receipt_token_id: int | None = None
    # Both ``None`` for a network upstream has added that STL has no chain id
    # for. Callers must not substitute a placeholder id: 0 already means
    # off-chain custody, so an unmapped EVM position would read as one.
    chain_id: int | None = None
    chain: str | None = None
