"""Domain-level exceptions raised by services and adapters.

These belong here (not in any adapter) so the inbound API layer can
translate them to HTTP responses without importing from a concrete
infrastructure module.
"""

from typing import ClassVar, Literal

# Stable machine-readable slugs for an unresolvable allocation share lookup,
# shared by every consumer that surfaces the failure — the ``/v1/risk/*`` 503
# responses and the per-allocation ``unpriced_reason`` on the prime risk-capital
# endpoint — so the two stay in lock-step. Defined in the domain layer (not the
# API) to keep the dependency direction inward.
ShareDataUnpricedReason = Literal["share_data_missing", "share_data_stale"]


class AllocationShareError(Exception):
    """Base class for allocation share lookup failures.

    ``code`` is abstract here: only the concrete subclasses carry a slug. A bare
    ``AllocationShareError`` is never raised, and reading ``.code`` on one fails
    loudly (``AttributeError``) rather than leaking an undocumented reason into
    ``unpriced_reason`` or a 503 body.
    """

    code: ClassVar[ShareDataUnpricedReason]


class StaleShareError(AllocationShareError):
    """Raised when the most-recent supply row is older than the configured staleness window."""

    code = "share_data_stale"


class MissingShareError(AllocationShareError):
    """Raised when no balance or supply row is available for the (chain, token, wallet) triple."""

    code = "share_data_missing"


class InvalidOverrideError(ValueError):
    """Raised by a RiskModel when scenario overrides are malformed or out of range.

    Inherits from ``ValueError`` so existing ``except ValueError`` blocks
    continue to catch it. The handler maps this specifically to HTTP 422,
    while bare ``ValueError`` (an invariant breach) is allowed to surface
    as 500 — those should never happen in production and indicate a bug.
    """
