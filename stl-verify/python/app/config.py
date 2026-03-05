import functools

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Logging
    log_level: str = "INFO"

    # Database
    database_url: SecretStr = SecretStr("postgresql+asyncpg://postgres:postgres@localhost:5432/stl_verify")


@functools.lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
