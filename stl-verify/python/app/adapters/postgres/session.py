from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def create_engine(database_url: str):
    """Create async SQLAlchemy engine."""
    return create_async_engine(database_url, echo=False, future=True)


def create_sessionmaker(engine) -> async_sessionmaker[AsyncSession]:
    """Create async session factory."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
