"""Wizard state management for deferred-commit setup flow.

The wizard collects configuration across 5 steps and commits everything
atomically at the final step. State is carried in a signed cookie.
"""

import base64
from dataclasses import dataclass, field, asdict
from typing import Any

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from starlette.requests import Request
from starlette.responses import Response


# 1 hour max age for wizard cookie
WIZARD_MAX_AGE = 3600
WIZARD_COOKIE = "central_wizard"


@dataclass
class WizardOperator:
    """Operator data collected in step 1."""
    username: str
    password_hash: str


@dataclass
class WizardSystem:
    """System settings collected in step 2."""
    map_tile_url: str
    map_attribution: str


@dataclass
class WizardApiKey:
    """API key collected in step 3."""
    alias: str
    encrypted_value_b64: str  # base64-encoded encrypted value


@dataclass
class WizardAdapter:
    """Adapter config collected in step 4."""
    enabled: bool
    cadence_s: int
    settings: dict[str, Any]


@dataclass
class WizardState:
    """Complete wizard state carried across all steps."""
    wizard_step: int = 1
    operator: dict | None = None
    system: dict | None = None
    api_keys: list[dict] = field(default_factory=list)
    adapters: dict[str, dict] | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "wizard_step": self.wizard_step,
            "operator": self.operator,
            "system": self.system,
            "api_keys": self.api_keys,
            "adapters": self.adapters,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WizardState":
        """Create from dictionary."""
        return cls(
            wizard_step=data.get("wizard_step", 1),
            operator=data.get("operator"),
            system=data.get("system"),
            api_keys=data.get("api_keys", []),
            adapters=data.get("adapters"),
        )


def _get_wizard_serializer(secret_key: str) -> URLSafeTimedSerializer:
    """Get a timed serializer for wizard state."""
    return URLSafeTimedSerializer(secret_key, salt="wizard-state")


def get_wizard_state(request: Request, secret_key: str) -> WizardState | None:
    """Decode wizard state from cookie.
    
    Returns WizardState if valid, None if missing/invalid/expired.
    """
    cookie_value = request.cookies.get(WIZARD_COOKIE)
    if not cookie_value:
        return None
    
    serializer = _get_wizard_serializer(secret_key)
    try:
        data = serializer.loads(cookie_value, max_age=WIZARD_MAX_AGE)
        return WizardState.from_dict(data)
    except (BadSignature, SignatureExpired):
        return None


def set_wizard_cookie(response: Response, state: WizardState, secret_key: str) -> None:
    """Set the wizard state cookie on a response."""
    serializer = _get_wizard_serializer(secret_key)
    signed_value = serializer.dumps(state.to_dict())
    response.set_cookie(
        WIZARD_COOKIE,
        signed_value,
        max_age=WIZARD_MAX_AGE,
        path="/setup",
        httponly=True,
        samesite="lax",
    )


def clear_wizard_cookie(response: Response) -> None:
    """Remove the wizard state cookie."""
    response.delete_cookie(WIZARD_COOKIE, path="/setup")


def get_step_route(step: int) -> str:
    """Get the route for a wizard step number."""
    routes = {
        1: "/setup/operator",
        2: "/setup/system",
        3: "/setup/keys",
        4: "/setup/adapters",
        5: "/setup/finish",
    }
    return routes.get(step, "/setup/operator")
