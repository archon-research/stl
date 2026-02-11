"""Integration test configuration with PostgreSQL testcontainers."""

import re
from pathlib import Path
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine
from testcontainers.postgres import PostgresContainer


# Migrations to skip (role/permission setup not needed for tests)
SKIP_MIGRATIONS = {
    "20260203_120000_create_app_roles.sql",
    "20260206_150000_create_protocol_event.sql",
}


async def apply_migrations(dsn: str) -> None:
    """Apply SQL migrations from db/migrations directory.
    
    Uses asyncpg directly since it supports multi-statement SQL execution,
    unlike SQLAlchemy's asyncpg dialect which uses prepared statements.
    """
    migrations_dir = Path(__file__).parent.parent.parent.parent / "db" / "migrations"
    migration_files = sorted(f for f in migrations_dir.glob("*.sql") if f.suffix == ".sql")

    # asyncpg wants postgresql:// not postgresql+asyncpg://
    dsn = dsn.replace("+asyncpg", "")
    
    conn = await asyncpg.connect(dsn)
    try:
        # Check for TimescaleDB
        has_timescaledb = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')"
        )

        for migration_file in migration_files:
            if migration_file.name in SKIP_MIGRATIONS:
                continue

            sql = migration_file.read_text()

            # Minimal preprocessing for test compatibility
            if not has_timescaledb:
                sql = re.sub(r"CREATE EXTENSION IF NOT EXISTS timescaledb;\s*", "", sql)
                sql = re.sub(r"\s+WITH\s*\([^)]*tsdb[^)]*\)", "", sql)
            
            # CONCURRENTLY requires autocommit, convert to regular for tests
            sql = sql.replace("CONCURRENTLY", "")

            await conn.execute(sql)
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def postgres_container():
    """Spin up a PostgreSQL container for the entire test session."""
    try:
        with PostgresContainer("timescale/timescaledb:latest-pg16", driver="asyncpg") as postgres:
            yield postgres
    except Exception:
        print("TimescaleDB not available, using regular PostgreSQL.")
        with PostgresContainer("postgres:16", driver="asyncpg") as postgres:
            yield postgres


@pytest_asyncio.fixture(scope="session")
async def db_engine(postgres_container) -> AsyncGenerator[AsyncEngine, None]:
    """Create async database engine with migrations applied."""
    db_url = postgres_container.get_connection_url()
    
    # Apply migrations using asyncpg directly (supports multi-statement SQL)
    await apply_migrations(db_url)
    
    # Create SQLAlchemy engine for tests
    engine = create_async_engine(db_url, echo=False)
    
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def db_sessionmaker(db_engine):
    """Create async session factory for testing."""
    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
