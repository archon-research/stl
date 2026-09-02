import functools
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

ENV_DIR = Path(__file__).resolve().parents[1]


def async_database_url(database_url: str) -> str:
    """Normalise a plain postgres URL for SQLAlchemy's async engine.

    The shared secret (pooler_url) stores a ``postgresql://`` or ``postgres://``
    URL; the async engine needs the ``postgresql+asyncpg`` dialect, and asyncpg
    takes ``ssl`` rather than libpq's ``sslmode``, so that parameter is dropped
    to avoid a TypeError at connect time. The single normalisation rule for the
    whole app — API settings and workers both call this.
    """
    url = make_url(database_url).set(drivername="postgresql+asyncpg")
    query = dict(url.query)
    query.pop("sslmode", None)
    return url.set(query=query).render_as_string(hide_password=False)


def parse_reference_effective_at(raw: str) -> datetime:
    """Parse a reference effective instant, mirroring Go's env.ReferenceEffectiveAt.

    One format, RFC 3339 UTC (ADR-0006 §5). A missing offset, a non-UTC offset and a future
    instant all raise, so a typo fails startup instead of pinning the API to an unintended
    instant — and an instant nothing can have observed resolves reference versions that did
    not exist yet.
    """
    example = "e.g. 2026-06-01T00:00:00Z"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"reference_effective_at={raw!r} is not RFC 3339 UTC ({example})") from None
    offset = parsed.utcoffset()
    if offset is None:
        raise ValueError(f"reference_effective_at={raw!r} carries no UTC offset; write it as {example}")
    if offset != timedelta(0):
        raise ValueError(
            f"reference_effective_at={raw!r} must be UTC; write the same instant with a Z offset "
            f"({parsed.astimezone(UTC).isoformat()})"
        )
    if parsed > datetime.now(UTC):
        raise ValueError(f"reference_effective_at={raw!r} is in the future")
    return parsed.astimezone(UTC)


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
    # --- auth plane (ADR-015; enforcement lands in a follow-up) ------------
    # All defaulted so the app ships dark: with auth_enabled=False nothing
    # below is read and behaviour is byte-identical. One flag reverts the
    # whole auth path (claim reading, JWT verify, FGA checks) by design.
    auth_enabled: bool = False
    oidc_issuer: str = ""  # e.g. http://keycloak.auth.svc/realms/archon
    oidc_audience: str = ""  # `aud` the API validates
    openfga_url: str = ""  # e.g. http://openfga.auth.svc:8080
    # Published by the openfga-store ConfigMap (store/model ids are created by
    # the store-bootstrap Job; they cannot be known at deploy-authoring time).
    openfga_store_id: str = ""
    openfga_model_id: str = ""

    risk_default_gap_pct: Decimal = Field(default=Decimal("0.15"), ge=0, le=1)
    suraf_inputs_dir: Path = ENV_DIR / "suraf" / "inputs"
    suraf_mappings_file: Path = ENV_DIR / "suraf" / "mappings" / "asset_to_rating.json"
    core_model_mappings_file: Path = (
        ENV_DIR / "app" / "risk_engine" / "core_model" / "mappings" / "asset_to_market_key.json"
    )
    # Injected as a Docker build arg; see stl-verify/python/Dockerfile.
    # Falls back to "unknown" so local dev and tests don't need it set.
    git_commit: str = "unknown"
    # Maximum age (in seconds) of a token_total_supply row before the risk API
    # treats it as stale and returns HTTP 503.
    allocation_share_max_stale_seconds: int = 1800
    # Pins which append-on-change reference versions the read path resolves (ADR-0006 §4).
    # Unset means now (UTC).
    #
    # A string parsed by the validator below, not a `datetime`, because pydantic reads a
    # bare numeric string as a Unix timestamp: `2026` becomes 1970-01-01T00:33:46Z and
    # `20260601` becomes 1970-08-23. Both precede every valid_from, so the priced reads
    # would collapse to zeros and 404s instead of failing.
    reference_effective_at: str | None = None

    @field_validator("reference_effective_at")
    @classmethod
    def _validate_reference_effective_at(cls, raw: str | None) -> str | None:
        if raw is None or raw == "":
            return None
        parse_reference_effective_at(raw)
        return raw

    def resolved_reference_effective_at(self) -> datetime | None:
        """The parsed reference instant, or None when unset (meaning now, UTC).

        Cannot raise; the validator above already parsed this at settings construction.
        """
        if self.reference_effective_at is None:
            return None
        return parse_reference_effective_at(self.reference_effective_at)

    # Connection-pool ceiling per replica. Set explicitly rather than left on
    # SQLAlchemy's 5 + 10, because a prime-scoped risk-capital request is a
    # concurrent fan-out rather than a single query: every repository read opens
    # its own engine.connect(), and the request gathers one receipt-token lookup
    # per position plus one model compute per allocation, across every ALM proxy
    # of the prime. Peak concurrent connections therefore scale with
    # positions × proxies, which this ceiling does not bound — it only decides how
    # far a replica gets before callers queue on pool_timeout and surface as 500s.
    # Bounding the fan-out itself is VEC-532.
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=20, ge=0)
    # How long a caller queues for a connection before its request fails. Left on
    # SQLAlchemy's unset 30s, a saturated replica holds a worker for half a minute
    # per queued caller, so one burst on the fan-out also stalls the endpoints that
    # never touch this pool. Set well above a healthy acquisition so it fires on
    # real exhaustion rather than on load: the fan-out's own queries run in
    # hundreds of milliseconds, so ten seconds of queueing means saturation, and
    # failing then keeps it legible instead of silently slow.
    db_pool_timeout: int = Field(default=10, ge=1)
    # Ceiling on how long a pooled connection lives before it is re-opened.
    # Bounds the blast radius of a connection the disconnect handling misses:
    # after a pooler incident (see create_db_engine), a poisoned connection can
    # keep answering the pre-ping while failing real queries, and this is the
    # backstop that retires it.
    db_pool_recycle_seconds: int = Field(default=300, ge=1)
    # Per-connection prepared-statement caching assumes one server backend per
    # client connection, which a transaction-mode pooler (the TigerData pooler
    # in staging/prod) does not guarantee: a statement prepared on one backend
    # can be executed on another. Governs both asyncpg's implicit cache and the
    # SQLAlchemy dialect's own cache. 0 disables both; raise it only for a
    # direct connection or a session-mode pooler.
    db_statement_cache_size: int = Field(default=0, ge=0)

    @property
    def async_database_url(self) -> str:
        return async_database_url(self.database_url.get_secret_value())


@functools.lru_cache
def get_settings() -> Settings:
    return Settings.model_validate({})
