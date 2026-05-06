"""Data sources transparency endpoint."""

from fastapi import APIRouter
from pydantic import BaseModel

from app.services.data_provenance_service import DataProvenanceService, SourceAccessModel

router = APIRouter()


class DataSourceResponse(BaseModel):
    """Data source metadata for the transparency panel."""

    name: str
    host: str
    access_model: SourceAccessModel
    role: str
    caveat: str | None = None
    attribution_required: bool = False


class DataSourcesResponse(BaseModel):
    """Registered data sources used by STL."""

    sources: list[DataSourceResponse]


@router.get("/data-sources", response_model=DataSourcesResponse)
async def get_data_sources():
    """Retrieve the registry of data sources used by STL."""
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
