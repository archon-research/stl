from app.config import Settings


def test_settings_loads_with_defaults():
    settings = Settings()

    assert settings.database_url is not None
    assert settings.log_level == "INFO"


class TestAsyncDatabaseUrl:
    def test_rewrites_postgresql_scheme(self):
        settings = Settings(database_url="postgresql://host:5432/db")
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"

    def test_rewrites_postgres_scheme(self):
        settings = Settings(database_url="postgres://host:5432/db")
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"

    def test_preserves_asyncpg_scheme(self):
        settings = Settings(database_url="postgresql+asyncpg://host:5432/db")
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"


class TestAllocationShareStaleness:
    def test_default_is_30_minutes(self):
        settings = Settings()
        assert settings.allocation_share_max_stale_seconds == 1800

    def test_overridable(self):
        settings = Settings(allocation_share_max_stale_seconds=600)
        assert settings.allocation_share_max_stale_seconds == 600
