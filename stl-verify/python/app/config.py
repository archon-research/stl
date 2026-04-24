import functools
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

ENV_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=(ENV_DIR / ".env.default", ENV_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    log_level: str
    log_format: str
    database_url: SecretStr
    otel_enabled: bool
    otel_exporter_otlp_endpoint: str
    otel_service_name: str
    suraf_inputs_dir: Path = ENV_DIR / "suraf" / "inputs"
    suraf_mappings_file: Path = ENV_DIR / "suraf" / "mappings" / "asset_to_rating.json"
    # Injected as a Docker build arg; see stl-verify/python/Dockerfile.
    # Falls back to "unknown" so local dev and tests don't need it set.
    git_commit: str = "unknown"
    # Maximum age (in seconds) of a token_total_supply row before the risk API
    # treats it as stale and returns HTTP 503.
    allocation_share_max_stale_seconds: int = 1800

    @property
    def async_database_url(self) -> str:
        """Return the database URL with the asyncpg driver.

        The shared secret (pooler_url) stores a plain ``postgresql://`` or
        ``postgres://`` URL. SQLAlchemy's async engine requires the
        ``postgresql+asyncpg://`` dialect. ``make_url`` handles scheme
        normalisation and query-parameter compatibility automatically.
        """
        url = make_url(self.database_url.get_secret_value())
        url = url.set(drivername="postgresql+asyncpg")
        # asyncpg does not accept sslmode (it uses ssl instead);
        # drop it to avoid a TypeError at connect time.
        query = dict(url.query)
        query.pop("sslmode", None)
        return url.set(query=query).render_as_string(hide_password=False)


@functools.lru_cache
def get_settings() -> Settings:
    return Settings()
