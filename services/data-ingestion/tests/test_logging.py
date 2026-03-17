"""Tests for shared.config.logging — structured logging, redaction, and correlation IDs."""

import uuid
from typing import Any

import pytest

from shared.config.logging import (
    _redact_sensitive,  # pyright: ignore[reportPrivateUsage]
    bind_correlation_id,
    clear_correlation_id,
    generate_correlation_id,
    get_logger,
    setup_logging,
)


class TestRedactSensitive:
    """Verify the redaction processor catches all sensitive field patterns."""

    def test_redacts_api_key(self):
        event = {"kalshi_api_key": "secret123", "city": "MIAMI"}
        result = _redact_sensitive(None, None, event)
        assert result["kalshi_api_key"] == "[REDACTED]"
        assert result["city"] == "MIAMI"

    def test_redacts_password(self):
        event = {"db_password": "hunter2"}
        result = _redact_sensitive(None, None, event)
        assert result["db_password"] == "[REDACTED]"

    def test_redacts_token(self):
        event = {"jwt_token": "eyJ..."}
        result = _redact_sensitive(None, None, event)
        assert result["jwt_token"] == "[REDACTED]"

    def test_redacts_authorization_header(self):
        event = {"authorization": "Bearer abc123"}
        result = _redact_sensitive(None, None, event)
        assert result["authorization"] == "[REDACTED]"

    def test_redacts_private_key(self):
        event = {"private_key_path": "/app/secrets/key.pem"}
        result = _redact_sensitive(None, None, event)
        assert result["private_key_path"] == "[REDACTED]"

    def test_redacts_secret(self):
        event = {"jwt_secret": "abc123def"}
        result = _redact_sensitive(None, None, event)
        assert result["jwt_secret"] == "[REDACTED]"

    def test_redacts_credential(self):
        event = {"user_credential": "pass"}
        result = _redact_sensitive(None, None, event)
        assert result["user_credential"] == "[REDACTED]"

    def test_redacts_pem(self):
        event = {"pem_content": "-----BEGIN RSA-----"}
        result = _redact_sensitive(None, None, event)
        assert result["pem_content"] == "[REDACTED]"

    def test_case_insensitive(self):
        event = {"API_KEY": "secret", "Password": "hunter2", "JWT_TOKEN": "abc"}
        result = _redact_sensitive(None, None, event)
        assert result["API_KEY"] == "[REDACTED]"
        assert result["Password"] == "[REDACTED]"
        assert result["JWT_TOKEN"] == "[REDACTED]"

    def test_non_sensitive_fields_untouched(self):
        event: dict[str, Any] = {"city": "NYC", "temperature": 72.5, "source": "nws", "event": "ingestion_complete"}
        result = _redact_sensitive(None, None, event)
        assert result == event

    def test_empty_event_dict(self):
        result = _redact_sensitive(None, None, {})
        assert result == {}

    def test_multiple_sensitive_fields_in_one_event(self):
        event = {
            "api_key": "k1",
            "db_password": "p1",
            "jwt_secret": "s1",
            "city": "MIAMI",
        }
        result = _redact_sensitive(None, None, event)
        assert result["api_key"] == "[REDACTED]"
        assert result["db_password"] == "[REDACTED]"
        assert result["jwt_secret"] == "[REDACTED]"
        assert result["city"] == "MIAMI"


class TestCorrelationId:
    def test_generate_returns_valid_uuid4(self):
        cid = generate_correlation_id()
        # Should be parseable as a UUID
        parsed = uuid.UUID(cid)
        assert parsed.version == 4

    def test_generate_returns_unique_ids(self):
        ids = {generate_correlation_id() for _ in range(100)}
        assert len(ids) == 100

    def test_bind_generates_new_id_when_none(self):
        clear_correlation_id()
        cid = bind_correlation_id()
        assert cid is not None
        assert len(cid) == 36  # UUID4 string length

    def test_bind_uses_provided_id(self):
        clear_correlation_id()
        cid = bind_correlation_id("custom-id-123")
        assert cid == "custom-id-123"

    def test_clear_removes_correlation_id(self):
        bind_correlation_id("to-be-cleared")
        clear_correlation_id()
        # After clearing, a new bind should generate a fresh ID
        new_id = bind_correlation_id()
        assert new_id != "to-be-cleared"


class TestSetupLogging:
    def test_setup_does_not_raise(self):
        setup_logging("DEBUG")

    def test_setup_with_json_output(self):
        setup_logging("INFO", json_output=True)

    def test_setup_with_console_output(self):
        setup_logging("INFO", json_output=False)

    def test_get_logger_returns_bound_logger(self):
        setup_logging("DEBUG")
        logger = get_logger("test-service")
        assert logger is not None

    def test_log_output_includes_service_name(self, capfd: pytest.CaptureFixture[str]):
        setup_logging("DEBUG", json_output=True)
        logger = get_logger("test-service")
        logger.info("test_event", city="NYC")
        captured = capfd.readouterr()
        # JSON output should contain the service name and event
        assert "test-service" in captured.err or "test-service" in captured.out

    def test_log_redacts_sensitive_in_output(self, capfd: pytest.CaptureFixture[str]):
        setup_logging("DEBUG", json_output=True)
        logger = get_logger("test-service")
        logger.info("auth_attempt", api_key="super-secret-key")
        captured = capfd.readouterr()
        output = captured.err + captured.out
        assert "super-secret-key" not in output
        assert "[REDACTED]" in output
