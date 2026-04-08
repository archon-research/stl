import functools

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", frozen=True)

    log_level: str = "INFO"
    database_url: SecretStr = SecretStr("postgresql+asyncpg://postgres:postgres@localhost:5432/stl_verify")
    # SecretStr prevents the URL (which embeds the API key) from appearing in logs or repr.
    alchemy_http_url: SecretStr = SecretStr("https://eth-mainnet.g.alchemy.com/v2/MISSING_KEY")

    @property
    def async_database_url(self) -> str:
        """Return the database URL with the asyncpg driver.

        The shared secret (pooler_url) stores a plain ``postgresql://`` URL.
        SQLAlchemy's async engine requires the ``postgresql+asyncpg://`` scheme.
        """
        url = self.database_url.get_secret_value()
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql+asyncpg://", 1)
        return url


@functools.lru_cache
def get_settings() -> Settings:
    return Settings()
