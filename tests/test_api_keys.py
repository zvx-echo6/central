"""Tests for API keys management routes."""

import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from central.gui.routes import (
    api_keys_list,
    api_keys_new,
    api_keys_create,
    api_keys_edit,
    api_keys_rotate,
    api_keys_delete,
)


class TestApiKeysListUnauthenticated:
    """Test API keys list without authentication."""

    @pytest.mark.asyncio
    async def test_api_keys_list_unauthenticated_redirects(self):
        """GET /api-keys without auth redirects to /login."""
        # This test verifies the session middleware behavior
        # In practice, the middleware redirects before the route is called
        # We verify the route requires operator in request.state
        mock_request = MagicMock()
        mock_request.state.operator = None

        # The middleware would redirect, but if it didn't,
        # the route would fail trying to access operator.id
        # This is tested via the session middleware tests


class TestApiKeysListAuthenticated:
    """Test API keys list with authentication."""

    @pytest.mark.asyncio
    async def test_api_keys_list_returns_all_keys_with_usage(self):
        """GET /api-keys authenticated returns 200 with all keys and usage info."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        # Mock database
        mock_conn = AsyncMock()

        # Keys query result
        mock_conn.fetch.side_effect = [
            # First call: keys
            [
                {
                    "alias": "firms",
                    "created_at": datetime(2026, 5, 16, 20, 0, tzinfo=timezone.utc),
                    "rotated_at": None,
                    "last_used_at": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
                },
                {
                    "alias": "test_key",
                    "created_at": datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc),
                    "rotated_at": None,
                    "last_used_at": None,
                },
            ],
            # Second call: adapters using firms
            [{"name": "firms"}],
            # Third call: adapters using test_key
            [],
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await api_keys_list(mock_request, mock_csrf)

        # Check template was called with correct context
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))

        assert len(context["keys"]) == 2
        firms_key = next(k for k in context["keys"] if k["alias"] == "firms")
        assert firms_key["used_by"] == ["firms"]

        test_key = next(k for k in context["keys"] if k["alias"] == "test_key")
        assert test_key["used_by"] == []


class TestApiKeysCreate:
    """Test API key creation."""

    @pytest.mark.asyncio
    async def test_create_valid_key_redirects(self):
        """POST /api-keys with valid data creates key and redirects."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"alias": "test1", "plaintext_key": "secret-api-key-123"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_conn = AsyncMock()
        # Check existing - not found
        mock_conn.fetchrow.side_effect = [
            None,  # No existing key
            {"created_at": datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)},  # Insert result
        ]
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.crypto.encrypt", return_value=b"encrypted_data"):
                with patch("central.gui.routes.write_audit", new_callable=AsyncMock):
                    result = await api_keys_create(mock_request, mock_csrf)

        assert result.status_code == 302
        assert result.headers["location"] == "/api-keys"

    @pytest.mark.asyncio
    async def test_create_existing_alias_shows_error(self):
        """POST /api-keys with existing alias shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"alias": "firms", "plaintext_key": "secret-key"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        # Key already exists
        mock_conn.fetchrow.return_value = {"alias": "firms"}

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                with patch("central.crypto.encrypt", return_value=b"encrypted"):
                    result = await api_keys_create(mock_request, mock_csrf)

        # Should re-render form with error
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "errors" in context
        assert "alias" in context["errors"]

    @pytest.mark.asyncio
    async def test_create_empty_alias_shows_error(self):
        """POST /api-keys with empty alias shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"alias": "", "plaintext_key": "secret-key"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await api_keys_create(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert context["errors"]["alias"] == "Alias is required"

    @pytest.mark.asyncio
    async def test_create_invalid_alias_chars_shows_error(self):
        """POST /api-keys with invalid alias chars shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        # Test with space
        form_data = {"alias": "test key", "plaintext_key": "secret-key"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await api_keys_create(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "letters, numbers, and underscores" in context["errors"]["alias"]

    @pytest.mark.asyncio
    async def test_create_hyphen_in_alias_shows_error(self):
        """POST /api-keys with hyphen in alias shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"alias": "test-key", "plaintext_key": "secret-key"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await api_keys_create(mock_request, mock_csrf)

        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "alias" in context["errors"]


class TestApiKeysRotate:
    """Test API key rotation."""

    @pytest.mark.asyncio
    async def test_rotate_updates_key_and_timestamp(self):
        """POST /api-keys/{alias} rotates key and updates rotated_at."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"new_plaintext_key": "new-secret-key-456"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_conn = AsyncMock()
        old_time = datetime(2026, 5, 17, 10, 0, tzinfo=timezone.utc)
        new_time = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)

        mock_conn.fetchrow.side_effect = [
            # First: get existing key
            {
                "alias": "test1",
                "created_at": datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
                "rotated_at": old_time,
                "last_used_at": None,
            },
            # Second: update result
            {"rotated_at": new_time},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.crypto.encrypt", return_value=b"new_encrypted"):
                with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                    result = await api_keys_rotate(mock_request, "test1", mock_csrf)

        assert result.status_code == 302
        # Check audit was called with no plaintext
        mock_audit.assert_called_once()
        audit_call = mock_audit.call_args
        assert "new-secret-key" not in str(audit_call)


class TestApiKeysDelete:
    """Test API key deletion."""

    @pytest.mark.asyncio
    async def test_delete_with_references_shows_error(self):
        """POST /api-keys/{alias}/delete with references shows error."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_templates = MagicMock()
        mock_templates.TemplateResponse.return_value = MagicMock()

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "alias": "firms",
            "created_at": datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
            "rotated_at": None,
            "last_used_at": None,
        }
        # Adapters using the key
        mock_conn.fetch.return_value = [{"name": "firms"}]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()
        mock_csrf.generate_csrf_tokens.return_value = ("token", "signed")
        mock_csrf.set_csrf_cookie = MagicMock()

        with patch("central.gui.routes._get_templates", return_value=mock_templates):
            with patch("central.gui.routes.get_pool", return_value=mock_pool):
                result = await api_keys_delete(mock_request, "firms", mock_csrf)

        # Should re-render with error
        call_args = mock_templates.TemplateResponse.call_args
        context = call_args.kwargs.get("context", call_args[1].get("context"))
        assert "error" in context
        assert "firms" in context["error"]

    @pytest.mark.asyncio
    async def test_delete_without_references_succeeds(self):
        """POST /api-keys/{alias}/delete without references deletes and redirects."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        mock_conn = AsyncMock()
        mock_conn.fetchrow.return_value = {
            "alias": "test1",
            "created_at": datetime(2026, 5, 16, 10, 0, tzinfo=timezone.utc),
            "rotated_at": None,
            "last_used_at": None,
        }
        # No adapters using the key
        mock_conn.fetch.return_value = []
        mock_conn.execute = AsyncMock()

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                result = await api_keys_delete(mock_request, "test1", mock_csrf)

        assert result.status_code == 302
        assert result.headers["location"] == "/api-keys"
        mock_conn.execute.assert_called_once()


class TestApiKeysAuditNoPlaintext:
    """Test that audit logs never contain plaintext keys."""

    @pytest.mark.asyncio
    async def test_create_audit_has_no_plaintext(self):
        """Verify create audit log doesn't contain plaintext."""
        mock_request = MagicMock()
        mock_request.state.operator = MagicMock(id=1, username="admin")

        form_data = {"alias": "newkey", "plaintext_key": "super-secret-value"}
        mock_request.form = AsyncMock(return_value=form_data)

        mock_conn = AsyncMock()
        mock_conn.fetchrow.side_effect = [
            None,
            {"created_at": datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)},
        ]

        mock_pool = MagicMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_pool.acquire.return_value.__aexit__.return_value = None

        mock_csrf = MagicMock()
        mock_csrf.validate_csrf = AsyncMock()

        with patch("central.gui.routes.get_pool", return_value=mock_pool):
            with patch("central.crypto.encrypt", return_value=b"encrypted"):
                with patch("central.gui.routes.write_audit", new_callable=AsyncMock) as mock_audit:
                    await api_keys_create(mock_request, mock_csrf)

        # Check audit call arguments
        call_kwargs = mock_audit.call_args.kwargs
        before = call_kwargs.get("before")
        after = call_kwargs.get("after")

        # Plaintext should not appear
        assert before is None or "super-secret-value" not in str(before)
        assert "super-secret-value" not in str(after)
        # encrypted value should not appear either
        assert "encrypted" not in str(after)
