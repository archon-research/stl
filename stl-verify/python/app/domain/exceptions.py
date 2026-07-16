"""Domain-level exceptions raised by services and adapters.

These belong here (not in any adapter) so the inbound API layer can
translate them to HTTP responses without importing from a concrete
infrastructure module.
"""


class AllocationShareError(Exception):
    """Base class for allocation share lookup failures.

    ``code`` is a stable machine-readable slug shared by every consumer that
    surfaces the failure — the ``/v1/risk/*`` 503 responses and the per-allocation
    ``unpriced_reason`` on the prime risk-capital endpoint — so the two stay in
    lock-step. It lives here (domain) rather than in the API layer to keep the
    dependency direction inward.
    """

    code = "share_data_unavailable"


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
