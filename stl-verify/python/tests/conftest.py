"""Root test conftest.

Re-exports the session-DB fixtures from ``tests.db`` so they're discoverable
across the suite.
"""

from tests.db import (
    pg_container,
    session_engine,
    wrap_conn,
    wrap_engine,
)

__all__ = [
    "pg_container",
    "session_engine",
    "wrap_conn",
    "wrap_engine",
]
