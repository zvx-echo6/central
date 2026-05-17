"""Tests for audit log module."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from central.gui.audit import (
    write_audit,
    AUTH_LOGIN,
    AUTH_LOGIN_FAILED,
    AUTH_LOGOUT,
    AUTH_PASSWORD_CHANGE,
    OPERATOR_CREATE,
)


class TestAuditConstants:
    """Tests for audit action constants."""

    def test_auth_login(self):
        assert AUTH_LOGIN == "auth.login"

    def test_auth_login_failed(self):
        assert AUTH_LOGIN_FAILED == "auth.login_failed"

    def test_auth_logout(self):
        assert AUTH_LOGOUT == "auth.logout"

    def test_auth_password_change(self):
        assert AUTH_PASSWORD_CHANGE == "auth.password_change"

    def test_operator_create(self):
        assert OPERATOR_CREATE == "operator.create"


class TestWriteAudit:
    """Tests for write_audit function."""

    @pytest.mark.asyncio
    async def test_write_audit_basic(self):
        """write_audit inserts basic audit record."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await write_audit(mock_conn, action="auth.login", operator_id=1)

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO config.audit_log" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_write_audit_with_target(self):
        """write_audit includes target when provided."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await write_audit(
            mock_conn,
            action="operator.create",
            operator_id=1,
            target="newuser",
        )

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        # target is the 3rd positional arg (after operator_id and action)
        assert "newuser" in call_args[0]

    @pytest.mark.asyncio
    async def test_write_audit_with_before_after(self):
        """write_audit includes before/after when provided."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await write_audit(
            mock_conn,
            action="config.update",
            operator_id=1,
            before={"value": "old"},
            after={"value": "new"},
        )

        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_write_audit_no_operator(self):
        """write_audit works with operator_id=None."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await write_audit(mock_conn, action="auth.login_failed", operator_id=None)

        mock_conn.execute.assert_called_once()
