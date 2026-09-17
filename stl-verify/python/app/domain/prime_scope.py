"""How a quantity behaves when a prime's wallets are folded together.

A prime is a set of wallets — a vault, one ALM proxy per chain, a SubProxy
treasury — so every prime-scoped figure is answered from more than one of them.
Some figures are held per wallet and have to be summed; others are one figure
for the prime that every wallet reports identically and must be counted once.
Summing a shared quantity across spark's seven proxies is a 7× error that reads
as a plausible number, which is why the rule is declared here rather than
restated at each call site.

=========  ==================================================================
Kind       Fold
=========  ==================================================================
additive   Sum the per-wallet contributions. ``None`` is unobserved rather
           than zero, so a sum of nothing is ``None``: a ``"0"`` would assert
           the prime holds nothing where the truth is that nothing was indexed
shared     The contributions must agree; the one value is the prime's figure.
           Disagreement means a shared read was fanned out per wallet, so it
           raises rather than picking one
=========  ==================================================================

Domain layer, standard library only, so any transport shares one rule.
"""

from collections.abc import Mapping, Sequence
from decimal import Decimal
from enum import StrEnum


class Quantity(StrEnum):
    """How a quantity behaves when a prime's wallets are folded together."""

    ADDITIVE = "additive"
    SHARED = "shared"


#: Every prime-scoped quantity the API serves, and its kind. A dataset that
#: reaches :func:`fold` without an entry here raises rather than guessing: a
#: missing declaration is a new quantity nobody has classified, and the
#: dangerous default is the silent one.
QUANTITIES: Mapping[str, Quantity] = {
    "exposure_usd": Quantity.ADDITIVE,
    "required_risk_capital_usd": Quantity.ADDITIVE,
    "modeled_exposure_usd": Quantity.ADDITIVE,
    "allocation_rows": Quantity.ADDITIVE,
    "activity_events": Quantity.ADDITIVE,
    "total_risk_capital_usd": Quantity.SHARED,
    "debt_wad": Quantity.SHARED,
    "anchorage_custody": Quantity.SHARED,
    "reference_figures": Quantity.SHARED,
}


def kind(quantity: str) -> Quantity:
    """The declared kind of ``quantity``, or ``KeyError`` naming it."""
    try:
        return QUANTITIES[quantity]
    except KeyError:
        raise KeyError(
            f"{quantity!r} is not declared in app.domain.prime_scope.QUANTITIES; "
            f"declare it additive or shared before folding it across a prime's wallets"
        ) from None


def fold(quantity: str, contributions: Sequence[Decimal | None]) -> Decimal | None:
    """Fold one quantity's per-wallet contributions into the prime's figure.

    ``None`` contributions are unobserved throughout: an additive fold skips
    them and answers ``None`` when nothing was observed at all, and a shared
    fold reads them as "this wallet reported nothing", not as a disagreement.

    Raises ``ValueError`` naming the quantity when a shared one disagrees
    across wallets — the double-count this module exists to prevent, surfaced
    as a failure rather than a number that is several times too large.
    """
    observed = [value for value in contributions if value is not None]
    if not observed:
        return None
    if kind(quantity) is Quantity.ADDITIVE:
        return sum(observed, Decimal("0"))

    distinct = set(observed)
    if len(distinct) > 1:
        raise ValueError(
            f"{quantity!r} is shared across a prime's wallets but its contributions disagree: "
            f"{sorted(distinct)}; it was read once per wallet where it should be read once per prime"
        )
    return observed[0]
