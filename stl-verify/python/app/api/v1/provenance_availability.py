"""Which provenances each prime can be answered from.

The UI reads this before it renders anything: a provenance a prime cannot be
served from is removed from the selector, and a URL asking for one is rewritten
to a provenance that works. Kept off ``/v1/primes`` so the prime list carries no
per-prime coverage read.
"""

import logging
from collections.abc import Callable

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import ReferenceEffectiveAtProvider
from app.api.deps import get_engine, get_reference_as_of, get_reference_risk_capital_service_factory
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
            "when STL has observed at least one reference cycle for the prime."
        ),
        examples=[["indexed", "reference", "both"]],
    )


class ProvenanceAvailabilityResponse(BaseModel):
    """Per-prime provenance coverage."""

    primes: list[PrimeProvenanceResponse] = Field(description="One entry per prime STL indexes.")
    reference_upstream_reachable: bool = Field(
        deprecated=True,
        description=(
            "DEPRECATED — always `true`. Coverage is now read from STL's own record of the "
            "reference feeds rather than by calling them, so there is no upstream to be unreachable: "
            "a read that fails is a `500` and cannot answer at all. Retained so clients that branch "
            "on it keep working. Read `available` per prime instead."
        ),
    )


async def _get_service(
    engine: AsyncEngine = Depends(get_engine),
    reference_as_of: ReferenceEffectiveAtProvider = Depends(get_reference_as_of),
) -> AllocationService:
    return AllocationService(AllocationRepository(engine, reference_as_of))


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

    # Lowercased here too: the port promises it, but a provider added later
    # should not be able to break coverage by shipping a capital letter.
    covered = {star.lower() for star in await reference_services().covered_stars()}

    return ProvenanceAvailabilityResponse(
        reference_upstream_reachable=True,
        primes=[
            PrimeProvenanceResponse(
                name=name,
                available=(
                    [Provenance.INDEXED, Provenance.REFERENCE, Provenance.BOTH]
                    if name.lower() in covered
                    else [Provenance.INDEXED]
                ),
            )
            for name in names
        ],
    )
