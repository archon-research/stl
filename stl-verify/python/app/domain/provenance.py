"""Which provenance a response is answered from.

Two provenances describe the same primes: what STL indexes from chain, and what
Sky publishes. Most endpoints can serve either, some can serve both at once, and
a few have only one — so the requested value and the *effective* value are not
the same thing, and the response reports the latter.
"""

from enum import Enum


class Provenance(str, Enum):
    """A response's provenance.

    ``str``-valued so it serialises as itself in a response model and parses
    from a query string without a converter.
    """

    INDEXED = "indexed"
    """STL's own model, computed from on-chain data it indexes."""

    REFERENCE = "reference"
    """Sky's published figures, as observed by STL."""

    BOTH = "both"
    """Both provenances, merged where they describe the same position."""


def resolve_provenance(
    requested: Provenance | None,
    *,
    available: frozenset[Provenance],
    default: Provenance = Provenance.BOTH,
) -> Provenance:
    """Narrow ``requested`` to what this endpoint can actually serve.

    ``BOTH`` narrows to the sole available provenance rather than failing: it is
    the default, so every request carries it, and an endpoint fed by one
    provenance (allocation activity has no reference feed at all) would
    otherwise reject the default. Callers report the returned value as the
    response's ``source`` so a narrowed answer never claims to be a merged one.

    Raises:
        ValueError: if a provenance was asked for by name and cannot be served.
            Silently substituting the other one would answer a different
            question than the one asked.
    """
    wanted = default if requested is None else requested

    if wanted in available:
        return wanted

    if not available:
        raise ValueError("no provenance is available for this resource")

    # Only BOTH narrows, and only where there is one provenance to narrow to.
    # Anything else asked for by name is refused rather than substituted.
    if wanted is Provenance.BOTH and len(available) == 1:
        return next(iter(available))

    servable = ", ".join(sorted(source.value for source in available))
    raise ValueError(f"source={wanted.value} is not available here; this resource serves: {servable}")


def legacy_reference_flag_as_provenance(reference: bool | None) -> Provenance | None:
    """Map the superseded ``reference`` boolean onto a :class:`Provenance`.

    ``reference=false`` is not the same as an absent parameter: it asked for
    STL's own figures, which is now ``source=indexed``, and must not fall
    through to the ``both`` default.
    """
    if reference is None:
        return None
    return Provenance.REFERENCE if reference else Provenance.INDEXED
