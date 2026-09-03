from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.config import Settings, async_database_url


def test_settings_loads_with_defaults():
    settings = Settings.model_validate({})

    assert settings.database_url is not None
    assert settings.log_level == "INFO"


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [
        pytest.param(None, Decimal("0.15"), id="default"),
        pytest.param("0.22", Decimal("0.22"), id="env-override"),
    ],
)
def test_risk_default_gap_pct(monkeypatch: pytest.MonkeyPatch, env_value: str | None, expected: Decimal) -> None:
    monkeypatch.delenv("RISK_DEFAULT_GAP_PCT", raising=False)
    if env_value is not None:
        monkeypatch.setenv("RISK_DEFAULT_GAP_PCT", env_value)

    settings = Settings.model_validate({})

    assert settings.risk_default_gap_pct == expected


@pytest.mark.parametrize("env_value", ["-0.01", "1.01"])
def test_risk_default_gap_pct_rejects_out_of_bounds_env_values(monkeypatch: pytest.MonkeyPatch, env_value: str) -> None:
    monkeypatch.setenv("RISK_DEFAULT_GAP_PCT", env_value)

    with pytest.raises(ValidationError, match="risk_default_gap_pct"):
        Settings.model_validate({})


class TestAsyncDatabaseUrl:
    def test_rewrites_postgresql_scheme(self):
        settings = Settings.model_validate({"database_url": "postgresql://host:5432/db"})
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"

    def test_rewrites_postgres_scheme(self):
        settings = Settings.model_validate({"database_url": "postgres://host:5432/db"})
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"

    def test_preserves_asyncpg_scheme(self):
        settings = Settings.model_validate({"database_url": "postgresql+asyncpg://host:5432/db"})
        assert settings.async_database_url == "postgresql+asyncpg://host:5432/db"

    def test_drops_sslmode(self):
        # asyncpg takes `ssl`, not libpq's `sslmode`, so the parameter is dropped.
        assert async_database_url("postgresql://u:p@h:5432/db?sslmode=require") == "postgresql+asyncpg://u:p@h:5432/db"

    def test_keeps_the_password(self):
        assert async_database_url("postgres://u:p@h:5432/db") == "postgresql+asyncpg://u:p@h:5432/db"


class TestAllocationShareStaleness:
    def test_default_is_30_minutes(self):
        settings = Settings.model_validate({})
        assert settings.allocation_share_max_stale_seconds == 1800

    def test_overridable(self):
        settings = Settings.model_validate({"allocation_share_max_stale_seconds": 600})
        assert settings.allocation_share_max_stale_seconds == 600


class TestReferenceEffectiveAt:
    """The reference instant must reject anything Go's resolver would reject.

    A value that parses to an unintended instant is worse than one that fails:
    an instant before every oracle_asset.valid_from makes the pinned read
    return no rows, and the priced reads then report zeros and 404s rather
    than erroring.
    """

    def test_unset_means_now(self):
        assert Settings.model_validate({}).resolved_reference_effective_at() is None

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("2026-06-01T02:30:00Z", datetime(2026, 6, 1, 2, 30, tzinfo=UTC)),
            ("2026-06-01T02:30:00+00:00", datetime(2026, 6, 1, 2, 30, tzinfo=UTC)),
            ("2026-06-01T02:30:00.123456Z", datetime(2026, 6, 1, 2, 30, 0, 123456, tzinfo=UTC)),
        ],
    )
    def test_accepts_rfc3339_utc(self, raw, expected):
        settings = Settings.model_validate({"reference_effective_at": raw})
        assert settings.resolved_reference_effective_at() == expected

    @pytest.mark.parametrize(
        "raw",
        [
            # Pydantic would read these as Unix timestamps: `2026` at
            # 1970-01-01T00:33:46Z, `20260601` at 1970-08-23.
            "2026",
            "20260601",
            "1700000000",
            "0",
            # Reading an operator's local wall clock as UTC resolves the wrong version.
            "2026-06-01T02:30:00",
            "2026-06-01",
            # One format everywhere: a non-UTC offset is rejected, not normalised.
            "2026-06-01T02:30:00+02:00",
            "not-a-date",
        ],
    )
    def test_rejects_values_that_would_resolve_to_an_unintended_instant(self, raw):
        with pytest.raises(ValidationError):
            Settings.model_validate({"reference_effective_at": raw})

    def test_rejects_a_future_instant(self):
        """An instant nothing has observed yet resolves reference versions that do not exist."""
        future = (datetime.now(UTC) + timedelta(days=1)).isoformat().replace("+00:00", "Z")

        with pytest.raises(ValidationError):
            Settings.model_validate({"reference_effective_at": future})
