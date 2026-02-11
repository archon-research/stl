"""Integration test configuration with PostgreSQL testcontainers."""
import asyncio
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the entire test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def postgres_container():
    """Spin up a PostgreSQL container for the entire test session."""
    with PostgresContainer("postgres:16", driver="asyncpg") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="session")
async def db_engine(postgres_container) -> AsyncGenerator[AsyncEngine, None]:
    """Create async database engine connected to test container."""
    # Get connection URL from container
    db_url = postgres_container.get_connection_url()
    
    # Create engine
    engine = create_async_engine(db_url, echo=False)
    
    # Apply migrations
    await apply_migrations(engine)
    
    yield engine
    
    await engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def db_sessionmaker(db_engine):
    """Create async session factory for testing."""
    return async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)


async def apply_migrations(engine: AsyncEngine) -> None:
    """Apply all SQL migrations to the test database."""
    migrations_dir = Path(__file__).parent.parent.parent.parent / "db" / "migrations"
    
    # Get all migration files in order
    migration_files = sorted([
        f for f in migrations_dir.glob("*.sql") 
        if f.name != "README.md"
    ])
    
    async with engine.begin() as conn:
        for migration_file in migration_files:
            print(f"Applying migration: {migration_file.name}")
            sql_content = migration_file.read_text()
            
            # Split into individual statements (naive split by semicolon)
            # This works for our migrations but won't handle all SQL edge cases
            statements = [
                stmt.strip() 
                for stmt in sql_content.split(';') 
                if stmt.strip() and not stmt.strip().startswith('--')
            ]
            
            # Execute each statement separately
            for statement in statements:
                if statement:
                    await conn.execute(text(statement))
