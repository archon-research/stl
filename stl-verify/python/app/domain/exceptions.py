"""Domain-level exceptions raised by services and adapters.

These belong here (not in any adapter) so the inbound API layer can
translate them to HTTP responses without importing from a concrete
infrastructure module.
"""


class AllocationShareError(Exception):
    """Base class for allocation share lookup failures."""


class StaleShareError(AllocationShareError):
    """Raised when the most-recent supply row is older than the configured staleness window."""


class MissingShareError(AllocationShareError):
    """Raised when no balance or supply row is available for the (chain, token, wallet) triple."""


class InvalidOverrideError(ValueError):
    """Raised by a RiskModel when scenario overrides are malformed or out of range.

    Inherits from ``ValueError`` so existing ``except ValueError`` blocks
    continue to catch it. The handler maps this specifically to HTTP 422,
    while bare ``ValueError`` (an invariant breach) is allowed to surface
    as 500 — those should never happen in production and indicate a bug.
    """
