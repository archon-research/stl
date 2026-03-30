import httpx
from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine


def get_http_client(request: Request) -> httpx.AsyncClient:
    """Extract the shared httpx client from application state."""
    return request.app.state.http_client
