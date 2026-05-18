"""Pre-auth CSRF protection for login and setup/operator pages.

These routes cannot use session-bound CSRF because no session exists yet.
Uses a simple cookie-based pattern with short-lived tokens.
"""

import secrets
from typing import Optional

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from starlette.requests import Request
from starlette.responses import Response


# 10 minute max age for pre-auth CSRF tokens
PRE_AUTH_CSRF_MAX_AGE = 600
PRE_AUTH_CSRF_COOKIE = "central_preauth_csrf"


def _get_serializer(secret_key: str) -> URLSafeTimedSerializer:
    """Get a timed serializer for CSRF tokens."""
    return URLSafeTimedSerializer(secret_key, salt="preauth-csrf")


def generate_pre_auth_csrf(secret_key: str) -> tuple[str, str]:
    """Generate a pre-auth CSRF token pair.
    
    Returns (plain_token, signed_token).
    The plain_token goes in the form, signed_token goes in the cookie.
    """
    plain_token = secrets.token_hex(32)
    serializer = _get_serializer(secret_key)
    signed_token = serializer.dumps(plain_token)
    return plain_token, signed_token


def set_pre_auth_csrf_cookie(response: Response, signed_token: str) -> None:
    """Set the pre-auth CSRF cookie on a response."""
    response.set_cookie(
        PRE_AUTH_CSRF_COOKIE,
        signed_token,
        max_age=PRE_AUTH_CSRF_MAX_AGE,
        path="/",
        httponly=True,
        samesite="lax",
    )


def validate_pre_auth_csrf(
    request: Request,
    form_token: str,
    secret_key: str,
) -> bool:
    """Validate a pre-auth CSRF token.
    
    Returns True if valid, False otherwise.
    """
    cookie_value = request.cookies.get(PRE_AUTH_CSRF_COOKIE)
    if not cookie_value or not form_token:
        return False
    
    serializer = _get_serializer(secret_key)
    try:
        expected_token = serializer.loads(cookie_value, max_age=PRE_AUTH_CSRF_MAX_AGE)
        return secrets.compare_digest(form_token, expected_token)
    except (BadSignature, SignatureExpired):
        return False


def unset_pre_auth_csrf_cookie(response: Response) -> None:
    """Remove the pre-auth CSRF cookie."""
    response.delete_cookie(PRE_AUTH_CSRF_COOKIE, path="/")
