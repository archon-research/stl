from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine
