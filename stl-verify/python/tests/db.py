"""Shared metadata cache for DB-backed test helpers.

The ``session_engine`` fixture reflects the live schema once per pytest
session and stashes the resulting ``MetaData`` here. Seed helpers in
``tests.seeds`` resolve tables by name via ``get_table()`` without each
caller having to re-reflect or import the fixture.
"""

from sqlalchemy import MetaData, Table

_session_metadata: MetaData | None = None


def set_session_metadata(metadata: MetaData) -> None:
    """Cache the reflected metadata. Called by the ``session_engine`` fixture."""
    global _session_metadata
    _session_metadata = metadata


def get_table(name: str) -> Table:
    """Return a reflected ``Table`` by name.

    Raises ``RuntimeError`` if the ``session_engine`` fixture has not run yet,
    and ``KeyError`` with the known table list if the name is unknown.
    """
    if _session_metadata is None:
        raise RuntimeError("session metadata not reflected yet — request the session_engine fixture first")
    try:
        return _session_metadata.tables[name]
    except KeyError:
        known = sorted(_session_metadata.tables)
        raise KeyError(
            f"table {name!r} not in reflected MetaData — did the migration adding it run? known tables: {known}"
        ) from None
