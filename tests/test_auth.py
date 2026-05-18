"""Tests for auth module."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from central.gui.auth import (
    hash_password,
    verify_password,
    validate_password,
    generate_token,
    create_session,
    get_session,
    delete_session,
    get_operator_by_username,
    create_operator,
    Operator,
)


class TestPasswordHashing:
    """Tests for password hashing functions."""

    def test_hash_password_returns_string(self):
        """hash_password returns a string."""
        result = hash_password("testpassword")
        assert isinstance(result, str)

    def test_hash_password_includes_argon2id(self):
        """hash_password uses argon2id algorithm."""
        result = hash_password("testpassword")
        assert result.startswith("$argon2id$")

    def test_hash_password_different_each_time(self):
        """hash_password produces different hashes for same password."""
        hash1 = hash_password("testpassword")
        hash2 = hash_password("testpassword")
        assert hash1 != hash2

    def test_verify_password_correct(self):
        """verify_password returns True for correct password."""
        password = "testpassword"
        hashed = hash_password(password)
        assert verify_password(password, hashed) is True

    def test_verify_password_incorrect(self):
        """verify_password returns False for wrong password."""
        hashed = hash_password("testpassword")
        assert verify_password("wrongpassword", hashed) is False

    def test_verify_password_empty(self):
        """verify_password handles empty strings."""
        hashed = hash_password("testpassword")
        assert verify_password("", hashed) is False


class TestPasswordValidation:
    """Tests for password validation."""

    def test_valid_password(self):
        """validate_password passes for valid password."""
        validate_password("password123")  # No exception

    def test_short_password(self):
        """validate_password raises for short password."""
        with pytest.raises(ValueError) as exc_info:
            validate_password("short")
        assert "8 characters" in str(exc_info.value)


class TestTokenGeneration:
    """Tests for token generation."""

    def test_generate_token_length(self):
        """generate_token produces expected length."""
        token = generate_token()
        # URL-safe base64 of 32 bytes is 43 characters
        assert len(token) == 43

    def test_generate_token_unique(self):
        """generate_token produces unique tokens."""
        tokens = [generate_token() for _ in range(100)]
        assert len(set(tokens)) == 100


class TestSessionManagement:
    """Tests for session creation and retrieval."""

    @pytest.mark.asyncio
    async def test_create_session(self):
        """create_session inserts a session record."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        token, expires_at, csrf_token = await create_session(mock_conn, operator_id=1, lifetime_days=90)

        assert len(token) == 43
        assert len(csrf_token) == 64  # 32 bytes hex = 64 chars
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "INSERT INTO config.sessions" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_get_session_found(self):
        """get_session returns (Operator, csrf_token) when session exists."""
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "id": 1,
            "username": "testuser",
            "created_at": datetime.now(timezone.utc),
            "password_changed_at": datetime.now(timezone.utc),
            "csrf_token": "test_csrf_token_12345",
        })

        result = await get_session(mock_conn, "valid-token")

        assert result is not None
        operator, csrf_token = result
        assert operator.id == 1
        assert operator.username == "testuser"
        assert csrf_token == "test_csrf_token_12345"

    @pytest.mark.asyncio
    async def test_get_session_not_found(self):
        """get_session returns None when session doesn\'t exist."""
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)

        operator = await get_session(mock_conn, "invalid-token")

        assert operator is None

    @pytest.mark.asyncio
    async def test_delete_session(self):
        """delete_session removes the session."""
        mock_conn = MagicMock()
        mock_conn.execute = AsyncMock()

        await delete_session(mock_conn, "some-token")

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "DELETE FROM config.sessions" in call_args[0][0]


class TestOperatorManagement:
    """Tests for operator creation and retrieval."""

    @pytest.mark.asyncio
    async def test_get_operator_by_username_found(self):
        """get_operator_by_username returns operator when found."""
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value={
            "id": 1,
            "username": "admin",
            "password_hash": "somehash",
            "created_at": datetime.now(timezone.utc),
            "password_changed_at": datetime.now(timezone.utc),
        })

        result = await get_operator_by_username(mock_conn, "admin")

        assert result is not None
        assert result["username"] == "admin"

    @pytest.mark.asyncio
    async def test_get_operator_by_username_not_found(self):
        """get_operator_by_username returns None when not found."""
        mock_conn = MagicMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)

        result = await get_operator_by_username(mock_conn, "nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_create_operator(self):
        """create_operator inserts and returns operator ID."""
        mock_conn = MagicMock()
        mock_conn.fetchval = AsyncMock(return_value=1)

        operator_id = await create_operator(mock_conn, "newuser", "password123")

        assert operator_id == 1
        mock_conn.fetchval.assert_called_once()
        call_args = mock_conn.fetchval.call_args
        assert "INSERT INTO config.operators" in call_args[0][0]
