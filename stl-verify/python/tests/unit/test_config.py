from app.config import Settings


def test_settings_loads_with_defaults():
    settings = Settings()

    assert settings.database_url is not None
    assert settings.log_level == "INFO"
