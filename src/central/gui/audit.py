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
STREAM_UPDATE = "stream.update"
API_KEY_CREATE = "api_key.create"
API_KEY_ROTATE = "api_key.rotate"
API_KEY_DELETE = "api_key.delete"
CONSUMER_DELETE = "consumer.delete"
SYSTEM_UPDATE = "system.update"
MONITORING_AREA_CREATE = "monitoring_area.create"
MONITORING_AREA_UPDATE = "monitoring_area.update"
MONITORING_AREA_DELETE = "monitoring_area.delete"
SETUP_COMPLETE = "setup.complete"


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
