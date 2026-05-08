"""Data sources transparency endpoint."""

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.data_provenance_service import DataProvenanceService, SourceAccessModel

router = APIRouter(tags=["data sources"])


class DataSourceResponse(BaseModel):
    """Data source metadata for the transparency panel."""

    name: str = Field(description="Human-readable name of the data source.", examples=["Alchemy"])
    host: str = Field(description="Hostname or base URL of the source.", examples=["alch.api.example.com"])
    access_model: SourceAccessModel = Field(description="How the source is accessed (e.g. `paid_api`, `public_rpc`).")
    role: str = Field(
        description="The role this source plays in the system (e.g. block ingestion, price feed).",
        examples=["block ingestion"],
    )
    caveat: str | None = Field(
        default=None,
        description="Operational caveat callers should be aware of, when present.",
        examples=["Rate-limited to 300 req/s"],
    )
    attribution_required: bool = Field(
        default=False,
        description="Whether downstream displays must attribute the source.",
    )


class DataSourcesResponse(BaseModel):
    """Registered data sources used by STL."""

    sources: list[DataSourceResponse] = Field(description="All registered upstream data sources.")


@router.get(
    "/data-sources",
    response_model=DataSourcesResponse,
    summary="List registered data sources",
    description=(
        "Return the registry of upstream data sources the verify service depends on, "
        "with access model, role, and any operational caveats. Useful for UI transparency "
        "panels and for auditing where on-chain and off-chain data ultimately originate."
    ),
)
async def get_data_sources():
    service = DataProvenanceService()
    return DataSourcesResponse(
        sources=[
            DataSourceResponse(
                name=s.name,
                host=s.host,
                access_model=s.access_model,
                role=s.role,
                caveat=s.caveat,
                attribution_required=s.attribution_required,
            )
            for s in service.get_sources()
        ],
    )
