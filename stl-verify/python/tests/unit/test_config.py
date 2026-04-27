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


class TestAlchemyHttpUrl:
    def test_constructs_url_from_api_key(self):
        settings = Settings(alchemy_api_key="test-key-123")
        assert settings.alchemy_http_url == "https://eth-mainnet.g.alchemy.com/v2/test-key-123"

    def test_uses_loaded_key_for_url(self):
        settings = Settings()
        key = settings.alchemy_api_key.get_secret_value()
        assert settings.alchemy_http_url == f"https://eth-mainnet.g.alchemy.com/v2/{key}"
