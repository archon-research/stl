import httpx
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Extract the shared httpx client from application state."""
    return request.app.state.http_client


def get_suraf_ratings(request: Request) -> dict[str, SurafResult]:
    """Extract the SURAF rating_id -> result lookup built at startup."""
    return request.app.state.suraf_ratings


def get_asset_to_rating(request: Request) -> dict[str, str]:
    """Extract the asset-key -> rating_id mapping built at startup."""
    return request.app.state.asset_to_rating


def get_suraf_rrc_service(request: Request) -> SurafRrcService:
    """Extract the SURAF RRC service built at startup."""
    return request.app.state.suraf_rrc_service
