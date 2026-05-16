"""Tests for bootstrap configuration."""

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from central.bootstrap_config import Settings, get_settings


class TestSettingsFromEnv:
    """Test loading settings from environment variables."""

    def test_reads_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings are read from CENTRAL_* environment variables."""
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://test:pass@localhost/testdb")
        monkeypatch.setenv("CENTRAL_NATS_URL", "nats://10.0.0.1:4222")
        monkeypatch.setenv("CENTRAL_MASTER_KEY_PATH", "/tmp/test.key")
        monkeypatch.setenv("CENTRAL_LOG_LEVEL", "DEBUG")

        settings = Settings()

        assert settings.db_dsn == "postgresql://test:pass@localhost/testdb"
        assert settings.nats_url == "nats://10.0.0.1:4222"
        assert settings.master_key_path == Path("/tmp/test.key")
        assert settings.log_level == "DEBUG"

    def test_defaults_applied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default values are used when env vars not set."""
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://x:y@localhost/db")
        # Clear any existing env vars that might interfere
        monkeypatch.delenv("CENTRAL_NATS_URL", raising=False)
        monkeypatch.delenv("CENTRAL_MASTER_KEY_PATH", raising=False)
        monkeypatch.delenv("CENTRAL_LOG_LEVEL", raising=False)

        settings = Settings()

        assert settings.nats_url == "nats://localhost:4222"
        assert settings.master_key_path == Path("/etc/central/master.key")
        assert settings.log_level == "INFO"


class TestSettingsFromFile:
    """Test loading settings from .env file."""

    def test_reads_from_env_file(self, tmp_path: Path) -> None:
        """Settings are read from .env file when env vars not present."""
        env_file = tmp_path / ".env"
        env_file.write_text(
            "CENTRAL_DB_DSN=postgresql://file:pass@localhost/filedb\n"
            "CENTRAL_NATS_URL=nats://file.local:4222\n"
            "CENTRAL_LOG_LEVEL=WARNING\n"
        )

        # Create settings pointing to the temp .env file
        settings = Settings(_env_file=env_file)

        assert settings.db_dsn == "postgresql://file:pass@localhost/filedb"
        assert settings.nats_url == "nats://file.local:4222"
        assert settings.log_level == "WARNING"

    def test_env_vars_override_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Environment variables take precedence over .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("CENTRAL_DB_DSN=postgresql://file@localhost/filedb\n")
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://env@localhost/envdb")

        settings = Settings(_env_file=env_file)

        assert settings.db_dsn == "postgresql://env@localhost/envdb"


class TestSettingsValidation:
    """Test settings validation and error handling."""

    def test_fails_if_required_var_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Clear error when required CENTRAL_DB_DSN is missing."""
        # Ensure no env vars or .env file provides the DSN
        monkeypatch.delenv("CENTRAL_DB_DSN", raising=False)

        with pytest.raises(Exception) as exc_info:
            # Use a non-existent .env file path to ensure no fallback
            Settings(_env_file=Path("/nonexistent/.env"))

        # pydantic-settings raises ValidationError for missing required fields
        assert "db_dsn" in str(exc_info.value).lower() or "validation" in str(exc_info.value).lower()

    def test_invalid_log_level_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid log level values are rejected."""
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://x@localhost/db")
        monkeypatch.setenv("CENTRAL_LOG_LEVEL", "INVALID")

        with pytest.raises(Exception):
            Settings()


class TestGetSettings:
    """Test the cached settings loader."""

    def test_caches_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """get_settings() returns cached instance."""
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://cached@localhost/db")
        get_settings.cache_clear()

        s1 = get_settings()
        s2 = get_settings()

        assert s1 is s2

    def test_cache_clear_reloads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """cache_clear() forces reload on next call."""
        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://first@localhost/db")
        get_settings.cache_clear()
        s1 = get_settings()

        monkeypatch.setenv("CENTRAL_DB_DSN", "postgresql://second@localhost/db")
        get_settings.cache_clear()
        s2 = get_settings()

        assert s1.db_dsn != s2.db_dsn
