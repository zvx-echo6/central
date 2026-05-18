"""Authentication utilities for Central GUI."""

import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

# Use argon2-cffi defaults (argon2id)
_hasher = PasswordHasher()


class CsrfValidationError(Exception):
    """Raised when CSRF token validation fails."""
    pass


@dataclass
class Operator:
    """Operator account."""
    id: int
    username: str
    created_at: datetime
    password_changed_at: datetime | None = None


def hash_password(plain: str) -> str:
    """Hash a password using argon2id."""
    return _hasher.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a password against its hash."""
    try:
        _hasher.verify(hashed, plain)
        return True
    except VerifyMismatchError:
        return False


def validate_password(plain: str) -> None:
    """Validate password meets requirements. Raises ValueError if invalid."""
    if len(plain) < 8:
        raise ValueError("Password must be at least 8 characters")


def generate_token() -> str:
    """Generate a cryptographically secure session token."""
    return secrets.token_urlsafe(32)


def generate_csrf_token() -> str:
    """Generate a cryptographically secure CSRF token."""
    return secrets.token_hex(32)


async def create_session(
    conn: Any,  # asyncpg.Connection
    operator_id: int,
    lifetime_days: int,
) -> tuple[str, datetime, str]:
    """Create a new session for an operator.
    
    Returns (token, expires_at, csrf_token).
    """
    token = generate_token()
    csrf_token = generate_csrf_token()
    expires_at = datetime.now(timezone.utc) + timedelta(days=lifetime_days)
    
    await conn.execute(
        """
        INSERT INTO config.sessions (token, operator_id, expires_at, csrf_token)
        VALUES ($1, $2, $3, $4)
        """,
        token,
        operator_id,
        expires_at,
        csrf_token,
    )
    
    return token, expires_at, csrf_token


async def get_session(conn: Any, token: str) -> tuple[Operator, str] | None:
    """Look up a session and return the associated operator and csrf_token.
    
    Returns (Operator, csrf_token) or None if token is invalid or expired.
    """
    row = await conn.fetchrow(
        """
        SELECT o.id, o.username, o.created_at, o.password_changed_at, s.csrf_token
        FROM config.sessions s
        JOIN config.operators o ON s.operator_id = o.id
        WHERE s.token = $1 AND s.expires_at > now()
        """,
        token,
    )
    
    if row is None:
        return None
    
    operator = Operator(
        id=row["id"],
        username=row["username"],
        created_at=row["created_at"],
        password_changed_at=row.get("password_changed_at"),
    )
    
    return operator, row["csrf_token"]


async def delete_session(conn: Any, token: str) -> None:
    """Delete a session."""
    await conn.execute(
        "DELETE FROM config.sessions WHERE token = $1",
        token,
    )


async def get_operator_by_username(conn: Any, username: str) -> dict | None:
    """Get an operator by username.
    
    Returns the row dict or None if not found.
    """
    return await conn.fetchrow(
        """
        SELECT id, username, password_hash, created_at, password_changed_at
        FROM config.operators
        WHERE username = $1
        """,
        username,
    )


async def create_operator(conn: Any, username: str, password: str) -> int:
    """Create a new operator.
    
    Returns the new operator ID.
    """
    password_hash = hash_password(password)
    row = await conn.fetchval(
        """
        INSERT INTO config.operators (username, password_hash)
        VALUES ($1, $2)
        RETURNING id
        """,
        username,
        password_hash,
    )
    return row
