"""Domain-level exceptions raised by services and adapters.

These belong here (not in any adapter) so the inbound API layer can
translate them to HTTP responses without importing from a concrete
infrastructure module.
"""

from typing import ClassVar, Literal

# Stable machine-readable slugs for an allocation the default model cannot price,
# shared by every consumer that surfaces the failure — the ``/v1/risk/*`` 503
# responses and the per-allocation ``unpriced_reason`` on the prime risk-capital
# endpoint — so the two stay in lock-step. Defined in the domain layer (not the
# API) to keep the dependency direction inward.
ShareDataUnpricedReason = Literal["share_data_missing", "share_data_stale"]
AllocationUnpricedReason = ShareDataUnpricedReason | Literal["price_data_missing", "adapter_data_missing"]


class AllocationUnpricedError(Exception):
    """Base class for a single allocation the default model cannot price.

    Raised on the per-allocation compute path so ``PrimeRiskCapitalService``
    degrades just that allocation to unpriced — and the ``/v1/risk/*`` endpoints
    return ``503`` carrying ``code`` — instead of failing the whole request.
    ``code`` is abstract here: only concrete subclasses carry a slug, so reading
    ``.code`` on a bare instance fails loudly (``AttributeError``) rather than
    leaking an undocumented reason into ``unpriced_reason`` or a 503 body.
    """

    code: ClassVar[AllocationUnpricedReason]


class AllocationShareError(AllocationUnpricedError):
    """An allocation whose pool-share lookup could not be resolved."""

    code: ClassVar[ShareDataUnpricedReason]


class StaleShareError(AllocationShareError):
    """Raised when the most-recent supply row is older than the configured staleness window."""

    code = "share_data_stale"


class MissingShareError(AllocationShareError):
    """Raised when no balance or supply row is available for the (chain, token, wallet) triple."""

    code = "share_data_missing"


class PriceDataMissingError(AllocationUnpricedError):
    """Raised when a backed asset's loan token has no USD price.

    Morpho backing amounts are loan-token-denominated, so without the loan
    token's price they cannot be converted to USD and the whole vault is
    unmodellable. Kept distinct from a genuine ``rrc=0`` (a priced position with
    no liquidatable debt) so the prime endpoint reports the allocation as
    unpriced rather than fully covered.
    """

    code = "price_data_missing"


class AdapterDataMissingError(AllocationUnpricedError):
    """Raised when a Morpho VaultV2's active adapters are not indexed yet.

    A VaultV2 holds its assets in liquidity adapters, never directly, so its
    collateral (and thus its liquidation params) is reachable only through those
    adapters. Until the adapter rows are backfilled, no params resolve and every
    collateral item would silently drop — a confident ``rrc=0`` with applied=True.
    Degrade the allocation to unpriced instead. Kept distinct from a genuinely idle
    v3 vault (adapters present, no collateral markets), which resolves to a real
    ``rrc=0`` rather than a data gap.
    """

    code = "adapter_data_missing"


class InvalidOverrideError(ValueError):
    """Raised by a RiskModel when scenario overrides are malformed or out of range.

    Inherits from ``ValueError`` so existing ``except ValueError`` blocks
    continue to catch it. The handler maps this specifically to HTTP 422,
    while bare ``ValueError`` (an invariant breach) is allowed to surface
    as 500 — those should never happen in production and indicate a bug.
    """
