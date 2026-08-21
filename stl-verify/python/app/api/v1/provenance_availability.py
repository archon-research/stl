"""Which provenances each prime can be answered from.

The UI reads this before it renders anything: a provenance a prime cannot be
served from is removed from the selector, and a URL asking for one is rewritten
to a provenance that works. Kept off ``/v1/primes`` so the prime list carries no
dependency on a third-party feed.
"""

import logging
from collections.abc import Callable

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api.deps import get_engine, get_reference_risk_capital_service_factory
from app.domain.exceptions import ReferenceDataUnavailableError
from app.domain.provenance import Provenance
from app.services.allocation_service import AllocationService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["primes"])


class PrimeProvenanceResponse(BaseModel):
    """The provenances one prime can be answered from."""

    name: str = Field(description="Prime name, as `/v1/primes` reports it.", examples=["spark"])
    available: list[Provenance] = Field(
        description=(
            "Provenances this prime can be served from. `indexed` is always present — a prime is "
            "only listed because STL indexes it. `reference` and `both` appear together, and only "
            "when Sky's monitor covers the prime."
        ),
        examples=[["indexed", "reference", "both"]],
    )


class ProvenanceAvailabilityResponse(BaseModel):
    """Per-prime provenance coverage."""

    primes: list[PrimeProvenanceResponse] = Field(description="One entry per prime STL indexes.")
    reference_upstream_reachable: bool = Field(
        description=(
            "Whether Sky's monitor answered. When `false` every prime reports `indexed` alone — "
            "unknown coverage is reported as no coverage, so a client is never told a provenance is "
            "available and then handed an error for it."
        )
    )


async def _get_service(engine: AsyncEngine = Depends(get_engine)) -> AllocationService:
    return AllocationService(AllocationRepository(engine))


@router.get(
    "/provenance/available",
    response_model=ProvenanceAvailabilityResponse,
    summary="Provenance coverage per prime",
    description=(
        "List, for every prime STL indexes, which values of the `source` parameter it can be "
        "answered from. Intended to be read once before rendering, so a client can offer only the "
        "provenances that will work rather than discovering the rest by their errors."
    ),
)
async def get_provenance_availability(
    service: AllocationService = Depends(_get_service),
    reference_services: Callable[[], ReferenceRiskCapitalService] = Depends(get_reference_risk_capital_service_factory),
) -> ProvenanceAvailabilityResponse:
    primes = await service.list_primes()
    names = sorted({prime.name for prime in primes})

    try:
        # Lowercased here too: the port promises it, but a provider added later
        # should not be able to break coverage by shipping a capital letter.
        tracked = {star.lower() for star in await reference_services().tracked_stars()}
        reachable = True
    except ReferenceDataUnavailableError as exc:
        # Not a 502: STL's own figures are unaffected, and reporting `indexed`
        # alone is a true statement about what can be served right now.
        logger.warning(
            "Reference coverage unknown; reporting indexed alone",
            extra={"error_message": str(exc)},
        )
        tracked = set()
        reachable = False

    return ProvenanceAvailabilityResponse(
        reference_upstream_reachable=reachable,
        primes=[
            PrimeProvenanceResponse(
                name=name,
                available=(
                    [Provenance.INDEXED, Provenance.REFERENCE, Provenance.BOTH]
                    if name.lower() in tracked
                    else [Provenance.INDEXED]
                ),
            )
            for name in names
        ],
    )
