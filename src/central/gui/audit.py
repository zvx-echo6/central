"""Audit logging for Central GUI."""

import json
from typing import Any

# Audit action constants
AUTH_LOGIN = "auth.login"
AUTH_LOGIN_FAILED = "auth.login_failed"
AUTH_LOGOUT = "auth.logout"
AUTH_PASSWORD_CHANGE = "auth.password_change"
OPERATOR_CREATE = "operator.create"
ADAPTER_UPDATE = "adapter.update"


async def write_audit(
    conn: Any,  # asyncpg.Connection
    action: str,
    operator_id: int | None = None,
    target: str | None = None,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
) -> None:
    """Write an audit log entry."""
    # asyncpg handles dict -> jsonb conversion automatically
    await conn.execute(
        """
        INSERT INTO config.audit_log (operator_id, action, target, before, after)
        VALUES ($1, $2, $3, $4, $5)
        """,
        operator_id,
        action,
        target,
        before,
        after,
    )
