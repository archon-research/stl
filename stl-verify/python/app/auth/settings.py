"""Startup validation for the auth settings."""

from app.config import Settings

# Blank fails each of these somewhere else — an empty audience 401s every token,
# an empty issuer reports "malformed token" — so the AUTH_ENABLED flip would be an
# outage debugged from the wrong error. OPENFGA_API_KEY is excluded: keyless
# OpenFGA is valid, and a wrong key is already a plain 503.
REQUIRED_AUTH_SETTINGS = ("oidc_issuer", "oidc_audience", "openfga_url", "openfga_store_name")


def check_auth_settings(settings: Settings) -> None:
    if not settings.auth_enabled:
        return
    missing = [name for name in REQUIRED_AUTH_SETTINGS if not str(getattr(settings, name, "")).strip()]
    if missing:
        raise RuntimeError(f"auth_enabled is true but these settings are blank: {', '.join(missing)}")
