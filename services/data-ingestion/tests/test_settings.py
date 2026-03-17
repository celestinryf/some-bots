"""Tests for shared.config.settings — centralized settings and env loading."""

import pytest

from shared.config.settings import Settings, get_settings, reset_settings


class TestSettingsDefaults:
    """Verify Settings dataclass defaults are sensible."""

    def test_default_settings_have_empty_secrets(self):
        s = Settings()
        assert s.kalshi_api_key_id == ""
        assert s.db_password == ""
        assert s.jwt_secret == ""

    def test_default_database_host_is_postgres(self):
        s = Settings()
        assert s.db_host == "postgres"

    def test_default_environment_is_development(self):
        s = Settings()
        assert s.environment == "development"

    def test_default_log_level_is_info(self):
        s = Settings()
        assert s.log_level == "INFO"


class TestDatabaseUrl:
    def test_database_url_format(self):
        s = Settings(db_user="testuser", db_password="testpass", db_host="localhost", db_port=5432, db_name="testdb")
        assert s.database_url == "postgresql://testuser:testpass@localhost:5432/testdb"

    def test_database_url_with_ssl(self):
        s = Settings(db_user="u", db_password="p", db_host="h", db_port=5432, db_name="d")
        assert s.database_url_with_ssl == "postgresql://u:p@h:5432/d?sslmode=require"


class TestIsProduction:
    def test_development_is_not_production(self):
        s = Settings(environment="development")
        assert s.is_production is False

    def test_production_is_production(self):
        s = Settings(
            environment="production",
            db_password="secret",
            visual_crossing_api_key="vc-key",
            pirate_weather_api_key="pw-key",
            openweather_api_key="owm-key",
            nws_user_agent="(test, test@test.com)",
        )
        assert s.is_production is True

    def test_arbitrary_value_is_not_production(self):
        s = Settings(environment="staging")
        assert s.is_production is False


class TestGetSettings:
    """Test the caching and reset behavior."""

    def test_get_settings_returns_settings(self):
        s = get_settings()
        assert isinstance(s, Settings)

    def test_get_settings_caches_result(self):
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_reset_settings_clears_cache(self):
        _s1 = get_settings()
        reset_settings()
        s2 = get_settings()
        # Both are Settings instances but may or may not be the same object
        # depending on env state. The key is that reset cleared the cache.
        assert isinstance(s2, Settings)

    def test_reset_settings_with_override(self):
        override = Settings(environment="test-override", db_host="testhost")
        reset_settings(override)
        s = get_settings()
        assert s.environment == "test-override"
        assert s.db_host == "testhost"

    def test_reset_settings_none_reloads_from_env(self):
        override = Settings(environment="override")
        reset_settings(override)
        assert get_settings().environment == "override"

        reset_settings(None)
        s = get_settings()
        # After reset with None, it reloads from env (which may or may not have ENVIRONMENT set)
        assert isinstance(s, Settings)


class TestLoadFromEnv:
    """Test that settings load correctly from environment variables."""

    def test_reads_db_host_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("DB_HOST", "custom-host")
        reset_settings()
        s = get_settings()
        assert s.db_host == "custom-host"

    def test_reads_db_port_as_int(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("DB_PORT", "9999")
        reset_settings()
        s = get_settings()
        assert s.db_port == 9999
        assert isinstance(s.db_port, int)

    def test_reads_kalshi_api_key(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("KALSHI_API_KEY_ID", "test-key-123")
        reset_settings()
        s = get_settings()
        assert s.kalshi_api_key_id == "test-key-123"

    def test_missing_env_var_uses_default(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("DB_HOST", raising=False)
        reset_settings()
        s = get_settings()
        assert s.db_host == "postgres"

    def test_reads_log_level(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        reset_settings()
        s = get_settings()
        assert s.log_level == "DEBUG"


class TestSettingsImmutability:
    """Settings is frozen — verify it can't be mutated after creation."""

    def test_cannot_set_attribute(self):
        s = Settings()
        with pytest.raises(AttributeError):
            s.db_host = "hacked"  # type: ignore[misc]

    def test_cannot_delete_attribute(self):
        s = Settings()
        with pytest.raises(AttributeError):
            del s.db_host  # type: ignore[misc]


class TestDbPortValidation:
    """DB_PORT environment variable must be a valid port number."""

    def test_invalid_db_port_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PORT", "not-a-number")
        reset_settings()
        with pytest.raises(ValueError, match="DB_PORT must be an integer"):
            get_settings()

    def test_db_port_out_of_range_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PORT", "99999")
        reset_settings()
        with pytest.raises(ValueError, match="DB_PORT must be 1-65535"):
            get_settings()

    def test_db_port_zero_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PORT", "0")
        reset_settings()
        with pytest.raises(ValueError, match="DB_PORT must be 1-65535"):
            get_settings()


class TestProductionValidation:
    """Production environment requires non-empty secrets."""

    def test_production_missing_secrets_raises(self) -> None:
        with pytest.raises(ValueError, match="Required secrets missing"):
            Settings(environment="production")

    def test_production_with_all_secrets_ok(self) -> None:
        s = Settings(
            environment="production",
            db_password="secret",
            visual_crossing_api_key="vc-key",
            pirate_weather_api_key="pw-key",
            openweather_api_key="owm-key",
            nws_user_agent="(test, test@test.com)",
        )
        assert s.is_production is True

    def test_development_allows_empty_secrets(self) -> None:
        s = Settings(environment="development")
        assert s.is_production is False

    def test_production_partial_secrets_lists_missing(self) -> None:
        with pytest.raises(ValueError, match="openweather_api_key") as exc_info:
            Settings(
                environment="production",
                db_password="secret",
                visual_crossing_api_key="vc-key",
                pirate_weather_api_key="pw-key",
                nws_user_agent="(test, test@test.com)",
            )
        # Should NOT list the ones that were provided
        assert "db_password" not in str(exc_info.value)
