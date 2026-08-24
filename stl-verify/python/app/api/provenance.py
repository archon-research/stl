"""FastAPI integration for the provenance selector.

Inbound adapter for the domain ``provenance`` policy: it declares the query
parameters, including the superseded boolean, and maps the domain's
``ValueError`` to HTTP 422.
"""

from fastapi import HTTPException, Query

from app.domain.provenance import (
    Provenance,
    legacy_reference_flag_as_provenance,
    resolve_provenance,
)

_SOURCE_DESCRIPTION = (
    "Which provenance to answer from. `indexed` is STL's own model computed from the chain it "
    "indexes; `reference` is Sky's published figures as observed by STL; `both` merges them, "
    "which is the default. An endpoint fed by a single provenance narrows `both` to that one and "
    "says so in the response's `source`, but naming a provenance it cannot serve is a `422`."
)

_REFERENCE_DESCRIPTION = (
    "**Deprecated** — use `source`. `reference=true` means `source=reference` and `reference=false` "
    "means `source=indexed` (not the `both` default, since it asked for STL's own figures by name). "
    "Passing both parameters with conflicting values is a `422`."
)


INDEXED_OR_REFERENCE = frozenset({Provenance.INDEXED, Provenance.REFERENCE})
"""Resources that answer from either provenance, one at a time."""


def get_requested_provenance(
    source: Provenance | None = Query(default=None, description=_SOURCE_DESCRIPTION),
    reference: bool | None = Query(default=None, deprecated=True, description=_REFERENCE_DESCRIPTION),
) -> Provenance | None:
    """Return the provenance the caller asked for, or ``None`` for the default.

    Resolution against what an endpoint can serve is deliberately left to the
    endpoint: the set differs per resource, and the effective value has to be
    reported back in the response.
    """
    legacy = legacy_reference_flag_as_provenance(reference)

    if source is not None and legacy is not None and source is not legacy:
        raise HTTPException(
            status_code=422,
            detail=(
                f"source={source.value} conflicts with the deprecated reference={str(reference).lower()}; "
                "pass only source"
            ),
        )

    return source if source is not None else legacy


def resolve_or_422(
    requested: Provenance | None,
    *,
    available: frozenset[Provenance],
    default: Provenance = Provenance.BOTH,
) -> Provenance:
    """Narrow ``requested`` to what the endpoint serves, as a 422 on refusal."""
    try:
        return resolve_provenance(requested, available=available, default=default)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
