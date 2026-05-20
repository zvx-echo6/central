"""Route handlers for Central GUI."""

import base64
import json
import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger("central.gui.routes")


from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from central.bootstrap_config import get_settings
from central.gui.csrf import (
    reuse_or_generate_pre_auth_csrf,
    generate_pre_auth_csrf,
    set_pre_auth_csrf_cookie,
    validate_pre_auth_csrf,
    unset_pre_auth_csrf_cookie,
)

from central.gui.auth import (
    CsrfValidationError,
    create_session,
    delete_session,
    hash_password,
    validate_password,
    verify_password,
)
from central.gui.audit import (
    ADAPTER_UPDATE,
    API_KEY_CREATE,
    API_KEY_DELETE,
    API_KEY_ROTATE,
    AUTH_LOGIN,
    AUTH_LOGIN_FAILED,
    AUTH_LOGOUT,
    AUTH_PASSWORD_CHANGE,
    OPERATOR_CREATE,
    SETUP_COMPLETE,
    STREAM_UPDATE,
    SYSTEM_UPDATE,
    write_audit,
)
from functools import cache

from pathlib import Path

from central.config_models import AdapterConfig
from central.gui.db import get_pool
from central.gui.form_descriptors import describe_fields, FieldDescriptor
from central.api_key_resolver import adapter_has_resolved_api_key
from central.adapter_discovery import discover_adapters
from central.streams import STREAMS as STREAM_REGISTRY
from pydantic import ValidationError

@cache
def _adapter_classes() -> dict:
    """Cached adapter class discovery.

    GUI is a separate process from supervisor; walks pkgutil itself.
    Python's import cache makes subsequent calls free.
    """
    return discover_adapters()


class _PreviewConfigStore:
    """No-op stand-in passed to adapter __init__ when calling preview_for_settings.

    preview_for_settings implementations must create their own one-shot HTTP
    session and must not depend on config_store / cursor_db state — the GUI
    process has no live ConfigStore (the supervisor owns the real one)."""

    pass


router = APIRouter()

# Streams to display on dashboard -- derived from the registry's dashboard flag.
DASHBOARD_STREAMS = [s.name for s in STREAM_REGISTRY if s.dashboard]

# Email validation regex (simple but effective)
ALIAS_REGEX = re.compile(r"^[a-zA-Z0-9_]+$")

# Email validation regex (simple but effective)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _get_templates():
    """Get templates instance (deferred import to avoid circular)."""
    from central.gui import templates
    return templates


def _format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _set_session_cookie(
    response: Response,
    token: str,
    max_age: int,
) -> None:
    """Set the session cookie on a response."""
    response.set_cookie(
        key="central_session",
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=max_age,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    """Clear the session cookie."""
    response.delete_cookie(
        key="central_session",
        path="/",
    )


@router.get("/health")
async def health() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the index page."""
    templates = _get_templates()
    operator = getattr(request.state, "operator", None)
    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"operator": operator, "csrf_token": csrf_token},
    )
    return response


@router.get("/dashboard/events", response_class=HTMLResponse)
async def dashboard_events(request: Request) -> HTMLResponse:
    """Get events by adapter for the last 24 hours."""
    templates = _get_templates()
    pool = get_pool()

    events = []
    error = None

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT adapter, COUNT(*) as count
                FROM events
                WHERE received > NOW() - INTERVAL '24 hours'
                GROUP BY adapter
                ORDER BY count DESC
                """
            )
            events = [{"adapter": row["adapter"], "count": row["count"]} for row in rows]
    except Exception as e:
        error = f"Database error: {str(e)}"

    return templates.TemplateResponse(
        request=request,
        name="_dashboard_events.html",
        context={"events": events, "error": error},
    )


@router.get("/dashboard/streams", response_class=HTMLResponse)
async def dashboard_streams(request: Request) -> HTMLResponse:
    """Get stream sizes from NATS JetStream."""
    from central.gui.nats import get_js

    templates = _get_templates()
    js = get_js()

    streams = None
    error = None

    if js is None:
        error = "NATS unavailable"
    else:
        streams = []
        for stream_name in DASHBOARD_STREAMS:
            try:
                info = await js.stream_info(stream_name)
                streams.append({
                    "name": stream_name,
                    "messages": info.state.messages,
                    "size": _format_bytes(info.state.bytes),
                    "error": None,
                })
            except Exception:
                streams.append({
                    "name": stream_name,
                    "messages": 0,
                    "size": "0 B",
                    "error": "unavailable",
                })

    return templates.TemplateResponse(
        request=request,
        name="_dashboard_streams.html",
        context={"streams": streams, "error": error},
    )


@router.get("/dashboard/polls", response_class=HTMLResponse)
async def dashboard_polls(request: Request) -> HTMLResponse:
    """Get last poll times for each adapter."""
    from central.gui.nats import get_js
    from nats.js.errors import NotFoundError

    templates = _get_templates()
    pool = get_pool()
    js = get_js()

    adapters = []
    error = None

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT name FROM config.adapters ORDER BY name"
            )
            adapter_names = [row["name"] for row in rows]
    except Exception as e:
        error = f"Database error: {str(e)}"
        return templates.TemplateResponse(
            request=request,
            name="_dashboard_polls.html",
            context={"adapters": [], "error": error},
        )

    if js is None:
        error = "NATS unavailable"
        adapters = [{"name": name, "last_poll": None, "status": None, "error": "NATS unavailable"} for name in adapter_names]
    else:
        for name in adapter_names:
            try:
                msg = await js.get_last_msg(
                    "CENTRAL_META",
                    f"central.meta.adapter.{name}.status",
                )
                data = json.loads(msg.data.decode())
                adapters.append({
                    "name": name,
                    "last_poll": data.get("ts"),
                    "status": "✓" if data.get("ok") else "✗",
                    "error": data.get("error") if not data.get("ok") else None,
                })
            except NotFoundError:
                # No status message for this adapter yet
                adapters.append({
                    "name": name,
                    "last_poll": None,
                    "status": None,
                    "error": None,
                })
            except Exception as e:
                adapters.append({
                    "name": name,
                    "last_poll": None,
                    "status": "?",
                    "error": str(e),
                })

    return templates.TemplateResponse(
        request=request,
        name="_dashboard_polls.html",
        context={"adapters": adapters, "error": error},
    )


# =============================================================================
# Setup Wizard routes
# =============================================================================


# =============================================================================
# Setup Wizard routes (deferred-commit pattern)
# =============================================================================


@router.get("/setup/operator", response_class=HTMLResponse)
async def setup_operator_form(request: Request) -> HTMLResponse:
    """Render the setup operator form (step 1)."""
    from central.gui.wizard import get_wizard_state
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    templates = _get_templates()
    settings = get_settings()

    # Get wizard state from cookie (if any)
    state = get_wizard_state(request, settings.csrf_secret)

    # Pre-fill from cookie state if available
    form_data = None
    if state and state.operator:
        form_data = {"username": state.operator.get("username", "")}

    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)

    response = templates.TemplateResponse(
        request=request,
        name="setup_operator.html",
        context={
            "csrf_token": csrf_token,
            "error": None,
            "form_data": form_data,
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/setup/operator")
async def setup_operator_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
) -> Response:
    """Process the setup operator form (step 1)."""
    from central.gui.wizard import get_wizard_state, set_wizard_cookie, WizardState
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    templates = _get_templates()
    settings = get_settings()

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    # Get or create wizard state
    state = get_wizard_state(request, settings.csrf_secret) or WizardState()

    # Validate input
    error = None
    if password != confirm_password:
        error = "Passwords do not match"
    else:
        try:
            validate_password(password)
        except ValueError as e:
            error = str(e)

    if error:
        csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
        response = templates.TemplateResponse(
            request=request,
            name="setup_operator.html",
            context={
                "csrf_token": csrf_token,
                "error": error,
                "form_data": {"username": username},
            },
            status_code=200,
        )
        if signed_token is not None:
            set_pre_auth_csrf_cookie(response, signed_token)
        return response

    # Hash password and store in wizard state (NO DB write)
    password_hash = hash_password(password)
    state.operator = {"username": username, "password_hash": password_hash}
    state.wizard_step = max(state.wizard_step, 2)

    # Redirect to next step with updated wizard cookie
    response = RedirectResponse(url="/setup/system", status_code=302)
    set_wizard_cookie(response, state, settings.csrf_secret)
    return response


@router.get("/setup/system", response_class=HTMLResponse)
async def setup_system_form(request: Request) -> HTMLResponse:
    """Render the system settings form (step 2)."""
    from central.gui.wizard import get_wizard_state
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    settings = get_settings()

    # Get wizard state - required for step 2+
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    # Pre-fill from cookie state or DB defaults
    if state.system:
        system = state.system
    else:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            system = {
                "map_tile_url": row["map_tile_url"] if row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                "map_attribution": row["map_attribution"] if row else "&copy; OpenStreetMap contributors",
            }

    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="setup_system.html",
        context={
            "csrf_token": csrf_token,
            "error": None,
            "errors": None,
            "form_data": None,
            "system": system,
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/setup/system")
async def setup_system_submit(request: Request) -> Response:
    """Process the system settings form (step 2)."""
    from central.gui.wizard import get_wizard_state, set_wizard_cookie
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    templates = _get_templates()
    settings = get_settings()

    # Get wizard state - required
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    map_tile_url = form.get("map_tile_url", "").strip()
    map_attribution = form.get("map_attribution", "").strip()

    form_data = {
        "map_tile_url": map_tile_url,
        "map_attribution": map_attribution,
    }

    errors: dict[str, str] = {}

    # Validate map_tile_url
    if not map_tile_url:
        errors["map_tile_url"] = "Map tile URL is required"
    elif "{z}" not in map_tile_url or "{x}" not in map_tile_url or "{y}" not in map_tile_url:
        errors["map_tile_url"] = "URL must contain {z}, {x}, and {y} placeholders"

    # Validate map_attribution
    if not map_attribution:
        errors["map_attribution"] = "Map attribution is required"

    if errors:
        csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
        response = templates.TemplateResponse(
            request=request,
            name="setup_system.html",
            context={
                "csrf_token": csrf_token,
                "error": None,
                "errors": errors,
                "form_data": form_data,
                "system": state.system or form_data,
            },
            status_code=200,
        )
        if signed_token is not None:
            set_pre_auth_csrf_cookie(response, signed_token)
        return response

    # Update wizard state (NO DB write)
    state.system = {"map_tile_url": map_tile_url, "map_attribution": map_attribution}
    state.wizard_step = max(state.wizard_step, 3)

    response = RedirectResponse(url="/setup/keys", status_code=302)
    set_wizard_cookie(response, state, settings.csrf_secret)
    return response


@router.get("/setup/keys", response_class=HTMLResponse)
async def setup_keys_form(request: Request) -> HTMLResponse:
    """Render the API keys form (step 3)."""
    from central.gui.wizard import get_wizard_state
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    settings = get_settings()

    # Get wizard state - required
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()

    # Keys come from cookie state (not DB)
    keys = [{"alias": k["alias"], "created_at": None} for k in state.api_keys]

    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="setup_keys.html",
        context={
            "csrf_token": csrf_token,
            "keys": keys,
            "errors": None,
            "form_data": None,
            "success": None,
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/setup/keys")
async def setup_keys_submit(request: Request) -> Response:
    """Process the API keys form (step 3)."""
    from central.gui.wizard import get_wizard_state, set_wizard_cookie
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf
    from central.crypto import encrypt

    templates = _get_templates()
    settings = get_settings()

    # Get wizard state - required
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    action = form.get("action", "add")

    # If action is "next", advance to adapters step
    if action == "next":
        state.wizard_step = max(state.wizard_step, 4)
        response = RedirectResponse(url="/setup/adapters", status_code=302)
        set_wizard_cookie(response, state, settings.csrf_secret)
        return response

    # Otherwise, add a new key
    alias = form.get("alias", "").strip()
    plaintext_key = form.get("plaintext_key", "")

    form_data = {"alias": alias}
    errors: dict[str, str] = {}

    # Validate alias
    if not alias:
        errors["alias"] = "Alias is required"
    elif len(alias) > 64:
        errors["alias"] = "Alias must be at most 64 characters"
    elif not ALIAS_REGEX.match(alias):
        errors["alias"] = "Alias must contain only letters, numbers, and underscores"
    elif any(k["alias"] == alias for k in state.api_keys):
        errors["alias"] = "An API key with this alias already exists"

    # Validate plaintext_key
    if not plaintext_key:
        errors["plaintext_key"] = "API key is required"
    elif len(plaintext_key) > 4096:
        errors["plaintext_key"] = "API key must be at most 4096 characters"

    keys = [{"alias": k["alias"], "created_at": None} for k in state.api_keys]

    if errors:
        csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
        response = templates.TemplateResponse(
            request=request,
            name="setup_keys.html",
            context={
                "csrf_token": csrf_token,
                "keys": keys,
                "errors": errors,
                "form_data": form_data,
                "success": None,
            },
            status_code=200,
        )
        if signed_token is not None:
            set_pre_auth_csrf_cookie(response, signed_token)
        return response

    # Encrypt the key and add to state (NO DB write)
    encrypted_value = encrypt(plaintext_key.encode())
    encrypted_b64 = base64.b64encode(encrypted_value).decode()
    state.api_keys.append({"alias": alias, "encrypted_value_b64": encrypted_b64})

    # Re-render with success message
    keys = [{"alias": k["alias"], "created_at": None} for k in state.api_keys]
    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="setup_keys.html",
        context={
            "csrf_token": csrf_token,
            "keys": keys,
            "errors": None,
            "form_data": None,
            "success": f"API key '{alias}' added successfully.",
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    set_wizard_cookie(response, state, settings.csrf_secret)
    return response


@router.get("/setup/adapters", response_class=HTMLResponse)
async def setup_adapters_form(request: Request) -> HTMLResponse:
    """Render the adapters configuration form (step 4)."""
    from central.gui.wizard import get_wizard_state
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    settings = get_settings()

    # Get wizard state - required
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    # Get wizard adapters (filtered by wizard_order)
    adapter_classes = _adapter_classes()
    wizard_adapters = sorted(
        [(name, cls) for name, cls in adapter_classes.items() if cls.wizard_order is not None],
        key=lambda nc: nc[1].wizard_order
    )

    # Pre-fill from cookie state or DB defaults
    if state.adapters:
        adapters = []
        for name, cls in wizard_adapters:
            if name in state.adapters:
                a = state.adapters[name]
                settings_dict = a["settings"]
            else:
                settings_dict = {}
            fields = describe_fields(cls.settings_schema, settings_dict)
            # Swap widget for api_key_field to api_key_select
            if cls.api_key_field is not None:
                for f in fields:
                    if f.name == cls.api_key_field:
                        f.widget = "api_key_select"
            adapters.append({
                "name": name,
                "display_name": cls.display_name,
                "enabled": a["enabled"] if name in state.adapters else False,
                "cadence_s": a["cadence_s"] if name in state.adapters else 300,
                "settings": settings_dict,
                "fields": fields,
            })
    else:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT name, enabled, cadence_s, settings
                FROM config.adapters
                ORDER BY name
                """
            )
            db_adapters = {row["name"]: row for row in rows}

        adapters = []
        for name, cls in wizard_adapters:
            if name in db_adapters:
                row = db_adapters[name]
                settings_dict = row["settings"] or {}
                enabled = row["enabled"]
                cadence_s = row["cadence_s"]
            else:
                settings_dict = {}
                enabled = False
                cadence_s = 300
            fields = describe_fields(cls.settings_schema, settings_dict)
            # Swap widget for api_key_field to api_key_select
            if cls.api_key_field is not None:
                for f in fields:
                    if f.name == cls.api_key_field:
                        f.widget = "api_key_select"
            adapters.append({
                "name": name,
                "display_name": cls.display_name,
                "enabled": enabled,
                "cadence_s": cadence_s,
                "settings": settings_dict,
                "fields": fields,
            })

    # Get API keys from wizard state (not DB)
    api_keys = [{"alias": k["alias"]} for k in state.api_keys]

    # Get map tile settings from wizard state or DB
    if state.system:
        tile_url = state.system["map_tile_url"]
        tile_attribution = state.system["map_attribution"]
    else:
        async with pool.acquire() as conn:
            sys_row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            tile_url = sys_row["map_tile_url"] if sys_row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="setup_adapters.html",
        context={
            "csrf_token": csrf_token,
            "adapters": adapters,
            "api_keys": api_keys,
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
            "error": None,
            "errors": None,
            "form_data": None,
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/setup/adapters")
async def setup_adapters_submit(request: Request) -> Response:
    """Process the adapters configuration form (step 4)."""
    from central.gui.wizard import get_wizard_state, set_wizard_cookie
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    templates = _get_templates()
    pool = get_pool()
    settings = get_settings()

    # Get wizard state - required
    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    errors: dict[str, str] = {}
    new_adapters: dict[str, dict] = {}

    # Get current adapter configs from state or DB as baseline
    if state.adapters:
        current_adapters = state.adapters
    else:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT name, enabled, cadence_s, settings FROM config.adapters ORDER BY name"
            )
            current_adapters = {}
            for row in rows:
                current_adapters[row["name"]] = {
                    "enabled": row["enabled"],
                    "cadence_s": row["cadence_s"],
                    "settings": row["settings"] or {},
                }

    # Get wizard adapters (filtered by wizard_order)
    adapter_classes = _adapter_classes()
    wizard_adapters = sorted(
        [(name, cls) for name, cls in adapter_classes.items() if cls.wizard_order is not None],
        key=lambda nc: nc[1].wizard_order
    )

    for adapter_name, adapter_cls in wizard_adapters:
        current = current_adapters.get(adapter_name, {"enabled": False, "cadence_s": 300, "settings": {}})
        current_settings = current.get("settings", {})
        new_settings = dict(current_settings)

        # Parse enabled
        enabled = f"{adapter_name}_enabled" in form

        # Parse cadence using AdapterConfig field constraint
        cadence_str = form.get(f"{adapter_name}_cadence_s", "")
        try:
            cadence_s = int(cadence_str)
            from central.config_models import AdapterConfig
            min_cadence = AdapterConfig.model_fields["cadence_s"].metadata[0].ge
            if cadence_s < min_cadence:
                errors[f"{adapter_name}_cadence_s"] = (
                    f"Input should be greater than or equal to {min_cadence}"
                )
        except ValueError:
            errors[f"{adapter_name}_cadence_s"] = "Cadence must be a valid integer"
            cadence_s = current.get("cadence_s", 300)

        # Generic field parsing using describe_fields
        fields = describe_fields(adapter_cls.settings_schema, current_settings)
        for field in fields:
            form_key = f"{adapter_name}_{field.name}"

            if field.widget == "text":
                value = form.get(form_key, "").strip()
                new_settings[field.name] = value if value else current_settings.get(field.name)

            elif field.widget == "api_key_select":
                # API key alias field - stored as text, validated post-loop
                value = form.get(form_key, "").strip()
                new_settings[field.name] = value if value else None

            elif field.widget == "number":
                value_str = form.get(form_key, "").strip()
                if value_str:
                    try:
                        new_settings[field.name] = int(value_str)
                    except ValueError:
                        errors[form_key] = f"{field.label} must be a valid number"
                else:
                    new_settings[field.name] = current_settings.get(field.name)

            elif field.widget == "checkbox":
                new_settings[field.name] = form_key in form

            elif field.widget == "csv":
                value = form.get(form_key, "").strip()
                if value:
                    new_settings[field.name] = [v.strip() for v in value.split(",") if v.strip()]
                else:
                    new_settings[field.name] = []

            elif field.widget == "select":
                value = form.get(form_key, "").strip()
                if value and field.options and value not in field.options:
                    errors[form_key] = f"Invalid {field.label.lower()}"
                else:
                    new_settings[field.name] = value

            elif field.widget == "checkboxes":
                # Use getlist for checkbox groups - absence means empty list
                values = form.getlist(form_key)
                if field.options:
                    invalid = [v for v in values if v not in field.options]
                    if invalid:
                        errors[form_key] = f"Invalid values: {', '.join(invalid)}"
                    else:
                        new_settings[field.name] = values
                else:
                    new_settings[field.name] = values

            elif field.widget == "region":
                # Region validation via RegionConfig model
                from central.config_models import RegionConfig
                region_north_str = form.get(f"{adapter_name}_{field.name}_north", "").strip()
                region_south_str = form.get(f"{adapter_name}_{field.name}_south", "").strip()
                region_east_str = form.get(f"{adapter_name}_{field.name}_east", "").strip()
                region_west_str = form.get(f"{adapter_name}_{field.name}_west", "").strip()

                try:
                    region_model = RegionConfig(
                        north=float(region_north_str),
                        south=float(region_south_str),
                        east=float(region_east_str),
                        west=float(region_west_str),
                    )
                    new_settings[field.name] = region_model.model_dump()
                except (ValueError, ValidationError) as e:
                    errors[f"{adapter_name}_{field.name}"] = str(e)

        # Run Pydantic validation on assembled settings to catch Literal violations etc.
        try:
            adapter_cls.settings_schema(**new_settings)
        except ValidationError as e:
            for err in e.errors():
                loc = err["loc"][0] if err["loc"] else "unknown"
                errors[f"{adapter_name}_{loc}"] = err["msg"]

        # Generic api_key_field validation against wizard state
        if adapter_cls.api_key_field is not None:
            field_value = new_settings.get(adapter_cls.api_key_field)
            if field_value:
                if not any(k["alias"] == field_value for k in state.api_keys):
                    errors[f"{adapter_name}_{adapter_cls.api_key_field}"] = (
                        "API key alias does not exist"
                    )

        new_adapters[adapter_name] = {
            "enabled": enabled,
            "cadence_s": cadence_s,
            "settings": new_settings,
        }

    # If errors, re-render
    if errors:
        adapters = []
        for name, cls in wizard_adapters:
            settings_dict = new_adapters[name]["settings"]
            fields = describe_fields(cls.settings_schema, settings_dict)
            # Swap widget for api_key_field to api_key_select
            if cls.api_key_field is not None:
                for f in fields:
                    if f.name == cls.api_key_field:
                        f.widget = "api_key_select"
            adapters.append({
                "name": name,
                "display_name": cls.display_name,
                "enabled": new_adapters[name]["enabled"],
                "cadence_s": new_adapters[name]["cadence_s"],
                "settings": settings_dict,
                "fields": fields,
            })
        api_keys = [{"alias": k["alias"]} for k in state.api_keys]
        
        if state.system:
            tile_url = state.system["map_tile_url"]
            tile_attribution = state.system["map_attribution"]
        else:
            tile_url = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = "&copy; OpenStreetMap contributors"

        csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
        response = templates.TemplateResponse(
            request=request,
            name="setup_adapters.html",
            context={
                "csrf_token": csrf_token,
                "adapters": adapters,
                "api_keys": api_keys,
                "tile_url": tile_url,
                "tile_attribution": tile_attribution,
                "error": "Please fix the errors below.",
                "errors": errors,
                "form_data": form,
            },
            status_code=200,
        )
        if signed_token is not None:
            set_pre_auth_csrf_cookie(response, signed_token)
        return response

    # Update wizard state (NO DB write)
    state.adapters = new_adapters
    state.wizard_step = max(state.wizard_step, 5)

    response = RedirectResponse(url="/setup/finish", status_code=302)
    set_wizard_cookie(response, state, settings.csrf_secret)
    return response

@router.get("/setup/finish", response_class=HTMLResponse)
async def setup_finish_form(request: Request) -> HTMLResponse:
    """Render the finish setup page (step 5)."""
    from central.gui.wizard import get_wizard_state
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf

    settings = get_settings()

    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()

    operator_count = 1 if state.operator else 0
    key_count = len(state.api_keys)
    system = state.system or {"map_tile_url": "(not configured)"}

    adapters = []
    if state.adapters:
        adapter_classes = _adapter_classes()
        wizard_adapters = sorted(
            [(name, cls) for name, cls in adapter_classes.items() if cls.wizard_order is not None],
            key=lambda nc: nc[1].wizard_order
        )
        for name, cls in wizard_adapters:
            if name in state.adapters:
                a = state.adapters[name]
                adapters.append({
                    "name": name,
                    "display_name": cls.display_name,
                    "enabled": a["enabled"],
                    "cadence_s": a["cadence_s"],
                })

    csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="setup_finish.html",
        context={
            "csrf_token": csrf_token,
            "operator_count": operator_count,
            "key_count": key_count,
            "system": system,
            "adapters": adapters,
            "error": None,
        },
    )
    if signed_token is not None:
        set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/setup/finish")
async def setup_finish_submit(request: Request) -> Response:
    """Complete the setup wizard - atomic commit of all wizard state."""
    from central.gui.wizard import get_wizard_state, clear_wizard_cookie
    from central.gui.csrf import reuse_or_generate_pre_auth_csrf
    from asyncpg.exceptions import UniqueViolationError

    templates = _get_templates()
    pool = get_pool()
    settings = get_settings()

    state = get_wizard_state(request, settings.csrf_secret)
    if state is None or state.operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    if not state.system:
        return RedirectResponse(url="/setup/system", status_code=302)
    if not state.adapters:
        return RedirectResponse(url="/setup/adapters", status_code=302)

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. INSERT operator
                op_row = await conn.fetchrow(
                    "INSERT INTO config.operators (username, password_hash) VALUES ($1, $2) RETURNING id",
                    state.operator["username"],
                    state.operator["password_hash"],
                )
                operator_id = op_row["id"]

                await write_audit(conn, OPERATOR_CREATE, operator_id=operator_id, target=state.operator["username"])

                # 2. Create session
                sysrow = await conn.fetchrow("SELECT session_lifetime_days FROM config.system WHERE id = true")
                lifetime_days = sysrow["session_lifetime_days"] if sysrow else 90
                token, expires_at, _ = await create_session(conn, operator_id, lifetime_days)

                # 3. UPDATE config.system
                old_sys = await conn.fetchrow("SELECT map_tile_url, map_attribution FROM config.system WHERE id = true")
                await conn.execute(
                    "UPDATE config.system SET map_tile_url = $1, map_attribution = $2, setup_complete = true WHERE id = true",
                    state.system["map_tile_url"],
                    state.system["map_attribution"],
                )
                await write_audit(conn, SYSTEM_UPDATE, operator_id=operator_id, target="system",
                    before={"map_tile_url": old_sys["map_tile_url"], "map_attribution": old_sys["map_attribution"]} if old_sys else None,
                    after={"map_tile_url": state.system["map_tile_url"], "map_attribution": state.system["map_attribution"]})

                # 4. INSERT each API key
                for key in state.api_keys:
                    encrypted = base64.b64decode(key["encrypted_value_b64"])
                    await conn.execute("INSERT INTO config.api_keys (alias, encrypted_value) VALUES ($1, $2)", key["alias"], encrypted)
                    await write_audit(conn, API_KEY_CREATE, operator_id=operator_id, target=key["alias"])

                # 5. UPDATE config.adapters
                for name, adapter_cfg in state.adapters.items():
                    old_adapter = await conn.fetchrow("SELECT enabled, cadence_s, settings FROM config.adapters WHERE name = $1", name)
                    await conn.execute(
                        "UPDATE config.adapters SET enabled = $1, cadence_s = $2, settings = $3, updated_at = now() WHERE name = $4",
                        adapter_cfg["enabled"], adapter_cfg["cadence_s"], adapter_cfg["settings"], name)
                    await write_audit(conn, ADAPTER_UPDATE, operator_id=operator_id, target=name,
                        before={"enabled": old_adapter["enabled"], "cadence_s": old_adapter["cadence_s"]} if old_adapter else None,
                        after={"enabled": adapter_cfg["enabled"], "cadence_s": adapter_cfg["cadence_s"]})

                await write_audit(conn, SETUP_COMPLETE, operator_id=operator_id, target="system")

    except UniqueViolationError:
        csrf_token, signed_token = reuse_or_generate_pre_auth_csrf(request, settings.csrf_secret)
        response = templates.TemplateResponse(request=request, name="setup_finish.html",
            context={"csrf_token": csrf_token, "operator_count": 1, "key_count": len(state.api_keys),
                     "system": state.system, "adapters": [{"name": n, "enabled": a["enabled"], "cadence_s": a["cadence_s"]} for n, a in state.adapters.items()],
                     "error": f"Username '{state.operator['username']}' already exists."}, status_code=200)
        if signed_token is not None:
            set_pre_auth_csrf_cookie(response, signed_token)
        return response

    response = RedirectResponse(url="/", status_code=302)
    clear_wizard_cookie(response)
    unset_pre_auth_csrf_cookie(response)
    _set_session_cookie(response, token, lifetime_days * 86400)
    return response
@router.get("/login", response_class=HTMLResponse)
async def login_form(
    request: Request,
) -> HTMLResponse:
    """Render the login form."""
    templates = _get_templates()
    settings = get_settings()
    csrf_token, signed_token = generate_pre_auth_csrf(settings.csrf_secret)
    response = templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"csrf_token": csrf_token, "error": None},
    )
    set_pre_auth_csrf_cookie(response, signed_token)
    return response


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),

) -> Response:
    """Process the login form."""
    templates = _get_templates()
    pool = get_pool()

    # Validate CSRF
    settings = get_settings()
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    # Look up operator
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, username, password_hash, created_at, password_changed_at
            FROM config.operators
            WHERE username = $1
            """,
            username,
        )

        if row is None:
            # Unknown user - still audit the attempt
            await write_audit(conn, AUTH_LOGIN_FAILED, target=username)
            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"csrf_token": csrf_token, "error": "Invalid username or password"},
                status_code=200,
            )
            return response

        # Verify password
        if not verify_password(password, row["password_hash"]):
            await write_audit(conn, AUTH_LOGIN_FAILED, operator_id=row["id"], target=username)
            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"csrf_token": csrf_token, "error": "Invalid username or password"},
                status_code=200,
            )
            return response

        # Get session lifetime
        sysrow = await conn.fetchrow(
            "SELECT session_lifetime_days FROM config.system WHERE id = true"
        )
        lifetime_days = sysrow["session_lifetime_days"] if sysrow else 90

        # Create session
        token, expires_at, _ = await create_session(conn, row["id"], lifetime_days)

        # Audit login
        await write_audit(conn, AUTH_LOGIN, operator_id=row["id"], target=username)

    # Redirect with session cookie
    response = RedirectResponse(url="/", status_code=302)
    _set_session_cookie(response, token, lifetime_days * 86400)
    return response


@router.post("/logout")
async def logout(
    request: Request,

) -> Response:
    """Log out the current user."""
    pool = get_pool()

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    # Get current session
    session_token = request.cookies.get("central_session")
    operator = getattr(request.state, "operator", None)

    async with pool.acquire() as conn:
        if session_token:
            await delete_session(conn, session_token)

        if operator:
            await write_audit(conn, AUTH_LOGOUT, operator_id=operator.id, target=operator.username)

    response = RedirectResponse(url="/login", status_code=302)
    _clear_session_cookie(response)
    return response


@router.get("/change-password", response_class=HTMLResponse)
async def change_password_form(
    request: Request,

) -> HTMLResponse:
    """Render the change password form."""
    templates = _get_templates()
    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="change_password.html",
        context={"csrf_token": csrf_token, "error": None, "success": False},
    )
    return response


@router.post("/change-password")
async def change_password_submit(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),

) -> Response:
    """Process the change password form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    # Get current password hash
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT password_hash FROM config.operators WHERE id = $1",
            operator.id,
        )

        error = None

        # Verify current password
        if not verify_password(current_password, row["password_hash"]):
            error = "Current password is incorrect"
        elif new_password != confirm_password:
            error = "New passwords do not match"
        else:
            try:
                validate_password(new_password)
            except ValueError as e:
                error = str(e)

        if error:
            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="change_password.html",
                context={"csrf_token": csrf_token, "error": error, "success": False},
                status_code=200,
            )
            return response

        # Update password
        new_hash = hash_password(new_password)
        await conn.execute(
            """
            UPDATE config.operators
            SET password_hash = $1, password_changed_at = now()
            WHERE id = $2
            """,
            new_hash,
            operator.id,
        )

        # Audit
        await write_audit(
            conn,
            AUTH_PASSWORD_CHANGE,
            operator_id=operator.id,
            target=operator.username,
        )

    # Redirect to index
    return RedirectResponse(url="/", status_code=302)


# =============================================================================
# Adapters routes
# =============================================================================


@router.get("/adapters", response_class=HTMLResponse)
async def adapters_list(
    request: Request,

) -> HTMLResponse:
    """List all adapters."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator
    adapter_classes = _adapter_classes()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at, last_error
            FROM config.adapters
            ORDER BY name
            """
        )

        adapters = []
        for row in rows:
            settings = row["settings"] or {}
            adapter_cls = adapter_classes.get(row["name"])

            # Check if required API key is missing — resolve via the per-row
            # settings[api_key_field] (operator-selected alias), falling back
            # to the class-attribute default when settings hasn't been set.
            has_key, requires_api_key_alias = await adapter_has_resolved_api_key(
                conn, adapter_cls, settings,
            )
            api_key_missing = not has_key

            adapters.append({
                "name": row["name"],
                "display_name": getattr(adapter_cls, "display_name", row["name"]) if adapter_cls else row["name"],
                "enabled": row["enabled"],
                "cadence_s": row["cadence_s"],
                "settings": settings,
                "paused_at": row["paused_at"],
                "updated_at": row["updated_at"],
                "last_error": row["last_error"],
                "api_key_missing": api_key_missing,
                "requires_api_key_alias": requires_api_key_alias,
            })

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="adapters_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "adapters": adapters,
        },
    )
    return response


@router.get("/adapters/{name}", response_class=HTMLResponse)
async def adapters_edit_form(
    request: Request,
    name: str,

) -> Response:
    """Render the adapter edit form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Look up the adapter class
    adapter_classes = _adapter_classes()
    adapter_cls = adapter_classes.get(name)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at, last_error
            FROM config.adapters
            WHERE name = $1
            """,
            name,
        )

        if row is None:
            return Response(status_code=404, content="Adapter not found")

        # Get map tile settings from config.system
        sys_row = await conn.fetchrow(
            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
        )
        tile_url = sys_row["map_tile_url"] if sys_row else "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

    settings = row["settings"] or {}

    # Build adapter dict with class metadata
    adapter = {
        "name": row["name"],
        "display_name": getattr(adapter_cls, "display_name", row["name"]) if adapter_cls else row["name"],
        "description": getattr(adapter_cls, "description", "") if adapter_cls else "",
        "enabled": row["enabled"],
        "cadence_s": row["cadence_s"],
        "settings": settings,
        "paused_at": row["paused_at"],
        "updated_at": row["updated_at"],
        "last_error": row["last_error"],
    }

    # Generate field descriptors if we have the adapter class
    fields = []
    if adapter_cls and hasattr(adapter_cls, "settings_schema"):
        fields = describe_fields(adapter_cls.settings_schema, settings)
        # Swap widget for api_key_field to api_key_select
        if adapter_cls.api_key_field is not None:
            for f in fields:
                if f.name == adapter_cls.api_key_field:
                    f.widget = "api_key_select"

    # Fetch API keys for api_key_select widget + resolve the per-adapter
    # alias against the operator-set settings, not the class-attr default.
    async with pool.acquire() as conn:
        api_key_rows = await conn.fetch("SELECT alias FROM config.api_keys ORDER BY alias")
        api_keys = [{"alias": r["alias"]} for r in api_key_rows]
        has_key, requires_api_key_alias = await adapter_has_resolved_api_key(
            conn, adapter_cls, settings,
        )
        api_key_missing = not has_key

    # Generic settings-driven preview. Adapters opt in by overriding
    # SourceAdapter.preview_for_settings; the framework is duck-typed on the
    # returned list[dict] shape and never branches on adapter name.
    preview_rows: list[dict] | None = None
    preview_error: str | None = None
    if adapter_cls is not None and hasattr(adapter_cls, "settings_schema"):
        try:
            settings_obj = adapter_cls.settings_schema(**settings)
            preview_cfg = AdapterConfig(
                name=row["name"],
                enabled=row["enabled"],
                cadence_s=row["cadence_s"],
                settings=settings,
                updated_at=row["updated_at"],
            )
            preview_adapter = adapter_cls(
                preview_cfg, _PreviewConfigStore(), Path("/dev/null")
            )
            preview_rows = await preview_adapter.preview_for_settings(settings_obj)
        except Exception as exc:
            preview_error = f"Preview unavailable: {exc}"

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="adapters_edit.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "adapter": adapter,
            "fields": fields,
            "api_keys": api_keys,
            "errors": None,
            "form_data": None,
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
            "api_key_missing": api_key_missing,
            "requires_api_key_alias": requires_api_key_alias,
            "preview_rows": preview_rows,
            "preview_error": preview_error,
        },
    )
    return response


@router.post("/adapters/{name}")
async def adapters_edit_submit(
    request: Request,
    name: str,

) -> Response:
    """Process the adapter edit form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    # Look up the adapter class
    adapter_classes = _adapter_classes()
    adapter_cls = adapter_classes.get(name)

    # Parse common form fields
    enabled = "enabled" in form
    cadence_s_str = form.get("cadence_s", "")

    errors: dict[str, str] = {}
    form_data: dict[str, Any] = {
        "enabled": enabled,
        "cadence_s": cadence_s_str,
    }

    # Validate cadence_s using AdapterConfig field constraint (ge=10)
    try:
        cadence_s = int(cadence_s_str)
        from central.config_models import AdapterConfig
        min_cadence = AdapterConfig.model_fields["cadence_s"].metadata[0].ge
        if cadence_s < min_cadence:
            errors["cadence_s"] = f"Input should be greater than or equal to {min_cadence}"
    except ValueError:
        errors["cadence_s"] = "Cadence must be a valid integer"
        cadence_s = 0

    async with pool.acquire() as conn:
        # Get current adapter state
        row = await conn.fetchrow(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at, last_error
            FROM config.adapters
            WHERE name = $1
            """,
            name,
        )

        if row is None:
            return Response(status_code=404, content="Adapter not found")

        current_settings = row["settings"] or {}

        # Parse and validate settings via Pydantic if we have the adapter class
        new_settings = {}
        if adapter_cls and hasattr(adapter_cls, "settings_schema"):
            schema = adapter_cls.settings_schema
            fields = describe_fields(schema, current_settings)

            # Parse form values based on widget type
            parsed_values = {}
            for field in fields:
                raw = form.get(field.name, "")
                form_data[field.name] = raw

                if field.widget == "text":
                    parsed_values[field.name] = raw.strip() if raw else None
                elif field.widget == "number":
                    try:
                        parsed_values[field.name] = int(raw) if raw else None
                    except ValueError:
                        errors[field.name] = f"{field.label} must be a number"
                elif field.widget == "checkbox":
                    parsed_values[field.name] = field.name in form
                elif field.widget == "csv":
                    if raw.strip():
                        parsed_values[field.name] = [v.strip() for v in raw.split(",") if v.strip()]
                    else:
                        parsed_values[field.name] = []
                elif field.widget == "select":
                    value = raw.strip() if raw else None
                    if value and field.options and value not in field.options:
                        errors[field.name] = f"Invalid {field.label.lower()}"
                    else:
                        parsed_values[field.name] = value
                elif field.widget == "checkboxes":
                    # Use getlist for checkbox groups
                    values = form.getlist(field.name)
                    form_data[field.name] = values  # Override raw value
                    if field.options:
                        invalid = [v for v in values if v not in field.options]
                        if invalid:
                            errors[field.name] = f"Invalid values: {', '.join(invalid)}"
                        else:
                            parsed_values[field.name] = values
                    else:
                        parsed_values[field.name] = values
                elif field.widget == "api_key_select":
                    # API key select - validate against existing keys
                    value = raw.strip() if raw else None
                    parsed_values[field.name] = value
                elif field.widget == "region":
                    # Region handled separately below
                    pass

            # Handle region fields (common pattern)
            region_north_str = form.get("region_north", "").strip()
            region_south_str = form.get("region_south", "").strip()
            region_east_str = form.get("region_east", "").strip()
            region_west_str = form.get("region_west", "").strip()

            form_data["region_north"] = region_north_str
            form_data["region_south"] = region_south_str
            form_data["region_east"] = region_east_str
            form_data["region_west"] = region_west_str

            # Check if any region field has a value
            has_region = any([region_north_str, region_south_str, region_east_str, region_west_str])

            if has_region:
                try:
                    region_north = float(region_north_str)
                    region_south = float(region_south_str)
                    region_east = float(region_east_str)
                    region_west = float(region_west_str)

                    if not (-90 <= region_south < region_north <= 90):
                        errors["region"] = "Invalid latitude: south must be less than north, both between -90 and 90"
                    elif not (-180 <= region_west < region_east <= 180):
                        errors["region"] = "Invalid longitude: west must be less than east, both between -180 and 180"
                    else:
                        parsed_values["region"] = {
                            "north": region_north,
                            "south": region_south,
                            "east": region_east,
                            "west": region_west,
                        }
                except ValueError:
                    errors["region"] = "Region coordinates must be valid numbers"
            else:
                parsed_values["region"] = None

            # Only validate with Pydantic if no parse errors
            if not errors:
                try:
                    # Filter out None values for optional fields without defaults
                    validated_data = {k: v for k, v in parsed_values.items() if v is not None}
                    validated = schema(**validated_data)
                    new_settings = validated.model_dump(mode="json")
                except ValidationError as e:
                    for err in e.errors():
                        field_name = err["loc"][0] if err["loc"] else "unknown"
                        errors[str(field_name)] = err["msg"]
        else:
            # No schema - just preserve existing settings
            new_settings = dict(current_settings)

        # If there are errors, re-render the form
        if errors:
            # Get map tile settings for re-render
            sys_row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            tile_url = sys_row["map_tile_url"] if sys_row else "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

            adapter = {
                "name": row["name"],
                "display_name": getattr(adapter_cls, "display_name", row["name"]) if adapter_cls else row["name"],
                "description": getattr(adapter_cls, "description", "") if adapter_cls else "",
                "enabled": row["enabled"],
                "cadence_s": row["cadence_s"],
                "settings": current_settings,
                "paused_at": row["paused_at"],
                "updated_at": row["updated_at"],
                "last_error": row["last_error"],
            }

            fields = []
            if adapter_cls and hasattr(adapter_cls, "settings_schema"):
                fields = describe_fields(adapter_cls.settings_schema, current_settings)
                # Swap widget for api_key_field to api_key_select
                if adapter_cls.api_key_field is not None:
                    for f in fields:
                        if f.name == adapter_cls.api_key_field:
                            f.widget = "api_key_select"

            # Fetch API keys for api_key_select widget + resolve the per-adapter
            # alias against the pre-edit settings (form validation failed, so
            # the stored settings haven't been replaced).
            api_key_rows = await conn.fetch("SELECT alias FROM config.api_keys ORDER BY alias")
            api_keys = [{"alias": r["alias"]} for r in api_key_rows]
            has_key, requires_api_key_alias = await adapter_has_resolved_api_key(
                conn, adapter_cls, current_settings,
            )
            api_key_missing = not has_key

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="adapters_edit.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "adapter": adapter,
                    "fields": fields,
                    "api_keys": api_keys,
                    "errors": errors,
                    "form_data": form_data,
                    "tile_url": tile_url,
                    "tile_attribution": tile_attribution,
                    "api_key_missing": api_key_missing,
                    "requires_api_key_alias": requires_api_key_alias,
                },
                status_code=200,
            )
            return response

        # Build before state for audit
        before = {
            "enabled": row["enabled"],
            "cadence_s": row["cadence_s"],
            "settings": current_settings,
        }

        # Build after state for audit
        after = {
            "enabled": enabled,
            "cadence_s": cadence_s,
            "settings": new_settings,
        }

        # Update the adapter
        await conn.execute(
            """
            UPDATE config.adapters
            SET enabled = $1, cadence_s = $2, settings = $3, updated_at = now()
            WHERE name = $4
            """,
            enabled,
            cadence_s,
            new_settings,
            name,
        )

        # Write audit log
        await write_audit(
            conn,
            ADAPTER_UPDATE,
            operator_id=operator.id,
            target=name,
            before=before,
            after=after,
        )

    return RedirectResponse(url="/adapters", status_code=302)


# =============================================================================
# Streams routes
# =============================================================================


@router.get("/streams", response_class=HTMLResponse)
async def streams_list(
    request: Request,

) -> HTMLResponse:
    """List all streams with live data."""
    from central.gui.nats import get_js

    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator
    js = get_js()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, max_age_s, max_bytes, managed_max_bytes, updated_at
            FROM config.streams
            ORDER BY name
            """
        )

    streams = []
    for row in rows:
        stream_data = {
            "name": row["name"],
            "max_age_s": row["max_age_s"],
            "max_bytes_cfg": row["max_bytes"],
            "managed_max_bytes": row["managed_max_bytes"],
            "live_bytes": None,
            "live_messages": None,
            "live_first_seq": None,
            "live_last_seq": None,
            "live_first_ts": None,
            "live_last_ts": None,
            "first_ts_error": None,
            "last_ts_error": None,
            "error": None,
        }

        # Fetch live data from JetStream
        if js is not None:
            try:
                info = await js.stream_info(row["name"])
                stream_data["live_bytes"] = info.state.bytes
                stream_data["live_messages"] = info.state.messages
                stream_data["live_first_seq"] = info.state.first_seq
                stream_data["live_last_seq"] = info.state.last_seq

                # Fetch first / last message timestamps via get_msg
                # RawStreamMsg has .time attribute (not .metadata.timestamp)
                if info.state.first_seq > 0:
                    try:
                        first_msg = await js.get_msg(row["name"], seq=info.state.first_seq)
                        stream_data["live_first_ts"] = first_msg.time
                    except Exception as e:
                        logger.warning(
                            "get_msg first failed",
                            extra={"stream": row["name"], "err": type(e).__name__},
                        )
                        stream_data["live_first_ts"] = None
                        stream_data["first_ts_error"] = type(e).__name__

                if info.state.last_seq > 0 and info.state.last_seq != info.state.first_seq:
                    try:
                        last_msg = await js.get_msg(row["name"], seq=info.state.last_seq)
                        stream_data["live_last_ts"] = last_msg.time
                    except Exception as e:
                        logger.warning(
                            "get_msg last failed",
                            extra={"stream": row["name"], "err": type(e).__name__},
                        )
                        stream_data["live_last_ts"] = None
                        stream_data["last_ts_error"] = type(e).__name__
                elif info.state.last_seq == info.state.first_seq and info.state.first_seq > 0:
                    # Single message in stream
                    stream_data["live_last_ts"] = stream_data.get("live_first_ts")

            except Exception as e:
                logger.exception("Stream info failed", extra={"stream": row["name"]})
                stream_data["error"] = f"unavailable: {type(e).__name__}"
        else:
            stream_data["error"] = "NATS unavailable"

        streams.append(stream_data)

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="streams_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "streams": streams,
        },
    )
    return response


@router.post("/streams/{name}", response_class=HTMLResponse)
async def streams_update(
    request: Request,
    name: str,

) -> Response:
    """Update stream max_age_s."""
    from central.gui.nats import get_js

    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Validate CSRF
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
    max_age_s_str = form.get("max_age_s", "").strip()

    errors: dict[str, str] = {}

    # Parse max_age_s
    try:
        max_age_s = int(max_age_s_str)
    except (ValueError, TypeError):
        max_age_s = 0
        errors[name] = "max_age_s must be a valid integer"

    # Validate range: 1 hour to 5 years
    MIN_AGE = 3600  # 1 hour
    MAX_AGE = 5 * 365 * 24 * 3600  # 5 years (157680000)
    if not errors and (max_age_s < MIN_AGE or max_age_s > MAX_AGE):
        errors[name] = f"max_age_s must be between {MIN_AGE} (1 hour) and {MAX_AGE} (5 years)"

    async with pool.acquire() as conn:
        # Check stream exists
        row = await conn.fetchrow(
            "SELECT name, max_age_s FROM config.streams WHERE name = $1",
            name,
        )

        if row is None:
            return Response(status_code=404, content="Stream not found")

        if errors:
            # Re-render with errors
            js = get_js()
            rows = await conn.fetch(
                """
                SELECT name, max_age_s, max_bytes, managed_max_bytes, updated_at
                FROM config.streams
                ORDER BY name
                """
            )

            streams = []
            for r in rows:
                stream_data = {
                    "name": r["name"],
                    "max_age_s": r["max_age_s"],
                    "max_bytes_cfg": r["max_bytes"],
                    "managed_max_bytes": r["managed_max_bytes"],
                    "live_bytes": None,
                    "live_messages": None,
                    "live_first_ts": None,
                    "live_last_ts": None,
                    "error": None,
                }

                if js is not None:
                    try:
                        info = await js.stream_info(r["name"])
                        stream_data["live_bytes"] = info.state.bytes
                        stream_data["live_messages"] = info.state.messages
                        stream_data["live_first_ts"] = info.state.first_ts
                        stream_data["live_last_ts"] = info.state.last_ts
                    except Exception:
                        stream_data["error"] = "unavailable"
                else:
                    stream_data["error"] = "NATS unavailable"

                streams.append(stream_data)

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="streams_list.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "streams": streams,
                    "errors": errors,
                },
            )
            return response

        old_max_age_s = row["max_age_s"]

        # Update stream
        await conn.execute(
            """
            UPDATE config.streams
            SET max_age_s = $1, updated_at = now()
            WHERE name = $2
            """,
            max_age_s,
            name,
        )

        # Write audit log
        await write_audit(
            conn,
            STREAM_UPDATE,
            operator_id=operator.id,
            target=name,
            before={"max_age_s": old_max_age_s},
            after={"max_age_s": max_age_s},
        )

    return RedirectResponse(url="/streams", status_code=302)


# =============================================================================
# Enrichment config route
# =============================================================================


def _enrichment_fields(current: dict) -> list[FieldDescriptor]:
    """Field descriptors for the single-row EnrichmentConfig form (generic
    machinery — same describe_fields used by adapter pages)."""
    from central.config_models import EnrichmentConfig

    return describe_fields(EnrichmentConfig, current)


async def _read_enrichment_row(conn) -> dict:
    row = await conn.fetchrow(
        """
        SELECT enricher_class, backend_class, backend_settings, cache_ttl_s
        FROM config.enrichment WHERE id = true
        """
    )
    return dict(row) if row is not None else {}


@router.get("/enrichment", response_class=HTMLResponse)
async def enrichment_form(request: Request) -> HTMLResponse:
    """Render the enrichment config form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    async with pool.acquire() as conn:
        current = await _read_enrichment_row(conn)

    response = templates.TemplateResponse(
        request=request,
        name="enrichment.html",
        context={
            "operator": operator,
            "csrf_token": request.state.csrf_token,
            "fields": _enrichment_fields(current),
            "errors": None,
            "form_data": None,
        },
    )
    return response


@router.post("/enrichment")
async def enrichment_update(request: Request) -> Response:
    """Validate + persist the enrichment config. Hot-reload picks it up via
    the config.enrichment NOTIFY trigger."""
    from central.config_models import EnrichmentConfig

    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    form = await request.form()
    if not form.get("csrf_token") or form.get("csrf_token") != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    errors: dict[str, str] = {}
    form_data: dict[str, Any] = {}
    parsed: dict[str, Any] = {}

    for field in _enrichment_fields({}):
        raw = form.get(field.name, "")
        form_data[field.name] = raw
        if field.widget == "number":
            try:
                parsed[field.name] = int(raw) if raw else None
            except ValueError:
                errors[field.name] = f"{field.label} must be a number"
        elif field.widget == "json":
            if not raw or not raw.strip():
                parsed[field.name] = {}
            else:
                try:
                    loaded = json.loads(raw)
                    if not isinstance(loaded, dict):
                        errors[field.name] = f"{field.label} must be a JSON object"
                    else:
                        parsed[field.name] = loaded
                except json.JSONDecodeError as e:
                    errors[field.name] = f"{field.label} is not valid JSON: {e}"
        else:  # text
            parsed[field.name] = raw.strip() if raw else None

    if not errors:
        try:
            validated = EnrichmentConfig(
                **{k: v for k, v in parsed.items() if v is not None}
            )
        except ValidationError as e:
            for err in e.errors():
                loc = err["loc"][0] if err["loc"] else "unknown"
                errors[str(loc)] = err["msg"]

    if errors:
        async with pool.acquire() as conn:
            current = await _read_enrichment_row(conn)
        response = templates.TemplateResponse(
            request=request,
            name="enrichment.html",
            context={
                "operator": operator,
                "csrf_token": request.state.csrf_token,
                "fields": _enrichment_fields(current),
                "errors": errors,
                "form_data": form_data,
            },
            status_code=200,
        )
        return response

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO config.enrichment
                (id, enricher_class, backend_class, backend_settings, cache_ttl_s)
            VALUES (true, $1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                enricher_class = EXCLUDED.enricher_class,
                backend_class = EXCLUDED.backend_class,
                backend_settings = EXCLUDED.backend_settings,
                cache_ttl_s = EXCLUDED.cache_ttl_s
            """,
            validated.enricher_class,
            validated.backend_class,
            validated.backend_settings,  # encoded as jsonb by the pool codec
            validated.cache_ttl_s,
        )

    return RedirectResponse(url="/enrichment", status_code=302)


# Alias validation regex
ALIAS_REGEX = re.compile(r'^[a-zA-Z0-9_]+$')


@router.get("/api-keys", response_class=HTMLResponse)
async def api_keys_list(
    request: Request,

) -> HTMLResponse:
    """List all API keys."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    async with pool.acquire() as conn:
        # Fetch keys (NOT encrypted_value)
        rows = await conn.fetch(
            """
            SELECT alias, created_at, rotated_at, last_used_at
            FROM config.api_keys
            ORDER BY alias
            """
        )

        # For each key, find adapters that reference it
        keys = []
        for row in rows:
            adapters = await conn.fetch(
                """
                SELECT name FROM config.adapters
                WHERE settings->>'api_key_alias' = $1
                ORDER BY name
                """,
                row["alias"],
            )
            keys.append({
                "alias": row["alias"],
                "created_at": row["created_at"],
                "rotated_at": row["rotated_at"],
                "last_used_at": row["last_used_at"],
                "used_by": [a["name"] for a in adapters],
            })

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="api_keys_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "keys": keys,
        },
    )
    return response


@router.get("/api-keys/new", response_class=HTMLResponse)
async def api_keys_new(
    request: Request,

) -> HTMLResponse:
    """Show form to add a new API key."""
    templates = _get_templates()
    operator = request.state.operator

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="api_keys_new.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
        },
    )
    return response


@router.post("/api-keys", response_class=HTMLResponse)
async def api_keys_create(
    request: Request,

) -> Response:
    """Create a new API key."""
    from central.crypto import encrypt

    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
    alias = form.get("alias", "").strip()
    plaintext_key = form.get("plaintext_key", "")

    errors: dict[str, str] = {}

    # Validate alias
    if not alias:
        errors["alias"] = "Alias is required"
    elif len(alias) > 64:
        errors["alias"] = "Alias must be at most 64 characters"
    elif not ALIAS_REGEX.match(alias):
        errors["alias"] = "Alias must contain only letters, numbers, and underscores"

    # Validate plaintext_key
    if not plaintext_key:
        errors["plaintext_key"] = "API key is required"
    elif len(plaintext_key) > 4096:
        errors["plaintext_key"] = "API key must be at most 4096 characters"

    if errors:
        csrf_token = request.state.csrf_token
        response = templates.TemplateResponse(
            request=request,
            name="api_keys_new.html",
            context={
                "operator": operator,
                "csrf_token": csrf_token,
                "errors": errors,
                "alias": alias,
            },
        )
        return response

    # Encrypt the key
    encrypted_value = encrypt(plaintext_key.encode())

    async with pool.acquire() as conn:
        # Check if alias already exists
        existing = await conn.fetchrow(
            "SELECT alias FROM config.api_keys WHERE alias = $1",
            alias,
        )

        if existing:
            errors["alias"] = "An API key with this alias already exists"
            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="api_keys_new.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "errors": errors,
                    "alias": alias,
                },
            )
            return response

        # Insert the new key
        row = await conn.fetchrow(
            """
            INSERT INTO config.api_keys (alias, encrypted_value)
            VALUES ($1, $2)
            RETURNING created_at
            """,
            alias,
            encrypted_value,
        )

        # Write audit log (no plaintext!)
        await write_audit(
            conn,
            API_KEY_CREATE,
            operator_id=operator.id,
            target=alias,
            before=None,
            after={"alias": alias, "created_at": row["created_at"].isoformat()},
        )

    return RedirectResponse(url="/api-keys", status_code=302)


@router.get("/api-keys/{alias}", response_class=HTMLResponse)
async def api_keys_edit(
    request: Request,
    alias: str,

) -> Response:
    """Show form to rotate or delete an API key."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT alias, created_at, rotated_at, last_used_at
            FROM config.api_keys
            WHERE alias = $1
            """,
            alias,
        )

        if row is None:
            return Response(status_code=404, content="API key not found")

        # Find adapters that reference this key
        adapters = await conn.fetch(
            """
            SELECT name FROM config.adapters
            WHERE settings->>'api_key_alias' = $1
            ORDER BY name
            """,
            alias,
        )

    key = {
        "alias": row["alias"],
        "created_at": row["created_at"],
        "rotated_at": row["rotated_at"],
        "last_used_at": row["last_used_at"],
        "used_by": [a["name"] for a in adapters],
    }

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="api_keys_edit.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "key": key,
        },
    )
    return response


@router.post("/api-keys/{alias}", response_class=HTMLResponse)
async def api_keys_rotate(
    request: Request,
    alias: str,

) -> Response:
    """Rotate an API key."""
    from central.crypto import encrypt

    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
    new_plaintext_key = form.get("new_plaintext_key", "")

    errors: dict[str, str] = {}

    # Validate new key
    if not new_plaintext_key:
        errors["new_plaintext_key"] = "New API key is required"
    elif len(new_plaintext_key) > 4096:
        errors["new_plaintext_key"] = "API key must be at most 4096 characters"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT alias, created_at, rotated_at, last_used_at
            FROM config.api_keys
            WHERE alias = $1
            """,
            alias,
        )

        if row is None:
            return Response(status_code=404, content="API key not found")

        if errors:
            adapters = await conn.fetch(
                """
                SELECT name FROM config.adapters
                WHERE settings->>'api_key_alias' = $1
                ORDER BY name
                """,
                alias,
            )

            key = {
                "alias": row["alias"],
                "created_at": row["created_at"],
                "rotated_at": row["rotated_at"],
                "last_used_at": row["last_used_at"],
                "used_by": [a["name"] for a in adapters],
            }

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="api_keys_edit.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "key": key,
                    "errors": errors,
                },
            )
            return response

        old_rotated_at = row["rotated_at"]

        # Encrypt the new key
        encrypted_value = encrypt(new_plaintext_key.encode())

        # Update the key
        new_row = await conn.fetchrow(
            """
            UPDATE config.api_keys
            SET encrypted_value = $1, rotated_at = now()
            WHERE alias = $2
            RETURNING rotated_at
            """,
            encrypted_value,
            alias,
        )

        # Write audit log (no plaintext!)
        await write_audit(
            conn,
            API_KEY_ROTATE,
            operator_id=operator.id,
            target=alias,
            before={"rotated_at": old_rotated_at.isoformat() if old_rotated_at else None},
            after={"rotated_at": new_row["rotated_at"].isoformat()},
        )

    return RedirectResponse(url="/api-keys", status_code=302)


@router.post("/api-keys/{alias}/delete", response_class=HTMLResponse)
async def api_keys_delete(
    request: Request,
    alias: str,

) -> Response:
    """Delete an API key."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT alias, created_at, rotated_at, last_used_at
            FROM config.api_keys
            WHERE alias = $1
            """,
            alias,
        )

        if row is None:
            return Response(status_code=404, content="API key not found")

        # Check for adapter references
        adapters = await conn.fetch(
            """
            SELECT name FROM config.adapters
            WHERE settings->>'api_key_alias' = $1
            ORDER BY name
            """,
            alias,
        )

        if adapters:
            adapter_names = [a["name"] for a in adapters]
            key = {
                "alias": row["alias"],
                "created_at": row["created_at"],
                "rotated_at": row["rotated_at"],
                "last_used_at": row["last_used_at"],
                "used_by": adapter_names,
            }

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="api_keys_edit.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "key": key,
                    "error": f"Cannot delete: used by {', '.join(adapter_names)}. Remove these references first.",
                },
            )
            return response

        # Delete the key
        await conn.execute(
            "DELETE FROM config.api_keys WHERE alias = $1",
            alias,
        )

        # Write audit log (no plaintext!)
        await write_audit(
            conn,
            API_KEY_DELETE,
            operator_id=operator.id,
            target=alias,
            before={
                "alias": row["alias"],
                "created_at": row["created_at"].isoformat(),
                "rotated_at": row["rotated_at"].isoformat() if row["rotated_at"] else None,
            },
            after=None,
        )

    return RedirectResponse(url="/api-keys", status_code=302)




# --- Events query helper ---

class EventsQueryResult:
    """Result from events query."""
    def __init__(self, events: list, next_cursor: str | None, error: str | None = None):
        self.events = events
        self.next_cursor = next_cursor
        self.error = error


def _parse_events_params(params) -> tuple[dict | None, str | None]:
    """
    Parse and validate events query parameters.

    Returns:
        (parsed_params, error_message)
        If error_message is not None, parsed_params is None.
    """
    # Parse and validate limit
    limit_str = params.get("limit", "50")
    try:
        limit = int(limit_str)
    except ValueError:
        return None, f"Invalid limit value: {limit_str}"

    if limit < 1 or limit > 200:
        return None, "limit must be between 1 and 200"

    # Parse adapter filter
    adapter = params.get("adapter")
    if adapter == "":
        adapter = None

    # Parse category filter
    category = params.get("category")
    if category == "":
        category = None

    # Parse since/until filters
    since = None
    until = None

    since_str = params.get("since")
    if since_str:
        try:
            since = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
        except ValueError:
            return None, f"Invalid ISO 8601 datetime for since: {since_str}"

    until_str = params.get("until")
    if until_str:
        try:
            until = datetime.fromisoformat(until_str.replace("Z", "+00:00"))
        except ValueError:
            return None, f"Invalid ISO 8601 datetime for until: {until_str}"

    # Validate since <= until
    if since and until and since > until:
        return None, "since must be before or equal to until"

    # Parse region bbox
    region_north = params.get("region_north")
    region_south = params.get("region_south")
    region_east = params.get("region_east")
    region_west = params.get("region_west")

    # Treat empty strings as None
    if region_north == "":
        region_north = None
    if region_south == "":
        region_south = None
    if region_east == "":
        region_east = None
    if region_west == "":
        region_west = None

    region_params = [region_north, region_south, region_east, region_west]
    region_supplied = [p for p in region_params if p is not None]

    if len(region_supplied) > 0 and len(region_supplied) < 4:
        return None, "Region filter requires all four parameters: region_north, region_south, region_east, region_west"

    bbox = None
    if len(region_supplied) == 4:
        try:
            bbox = {
                "north": float(region_north),
                "south": float(region_south),
                "east": float(region_east),
                "west": float(region_west),
            }
        except ValueError:
            return None, "Region parameters must be valid numbers"

    # Parse cursor
    cursor_time = None
    cursor_id = None
    cursor_str = params.get("cursor")

    if cursor_str:
        try:
            decoded = base64.b64decode(cursor_str).decode("utf-8")
            parts = decoded.split("|", 1)
            if len(parts) != 2:
                raise ValueError("Invalid cursor format")
            cursor_time = datetime.fromisoformat(parts[0])
            cursor_id = parts[1]
        except Exception:
            return None, "Invalid cursor"

    return {
        "limit": limit,
        "adapter": adapter,
        "category": category,
        "since": since,
        "until": until,
        "bbox": bbox,
        "cursor_time": cursor_time,
        "cursor_id": cursor_id,
    }, None


async def _fetch_events(parsed_params: dict) -> EventsQueryResult:
    """
    Fetch events from database using parsed parameters.

    Returns EventsQueryResult with events list, next_cursor, and optional error.
    """
    pool = get_pool()

    limit = parsed_params["limit"]
    adapter = parsed_params["adapter"]
    category = parsed_params["category"]
    since = parsed_params["since"]
    until = parsed_params["until"]
    bbox = parsed_params["bbox"]
    cursor_time = parsed_params["cursor_time"]
    cursor_id = parsed_params["cursor_id"]

    # Build query
    conditions = []
    query_params = []
    param_idx = 1

    if adapter:
        conditions.append(f"adapter = ${param_idx}")
        query_params.append(adapter)
        param_idx += 1

    if category:
        conditions.append(f"category = ${param_idx}")
        query_params.append(category)
        param_idx += 1

    if since:
        conditions.append(f"time >= ${param_idx}")
        query_params.append(since)
        param_idx += 1

    if until:
        conditions.append(f"time < ${param_idx}")
        query_params.append(until)
        param_idx += 1

    if bbox:
        conditions.append(
            f"ST_Intersects(geom, ST_MakeEnvelope(${param_idx}, ${param_idx+1}, ${param_idx+2}, ${param_idx+3}, 4326))"
        )
        query_params.extend([bbox["west"], bbox["south"], bbox["east"], bbox["north"]])
        param_idx += 4

    if cursor_time and cursor_id:
        conditions.append(f"(time, id) < (${param_idx}, ${param_idx+1})")
        query_params.append(cursor_time)
        query_params.append(cursor_id)
        param_idx += 2

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    # Fetch limit+1 to check for next page
    query = f"""
        SELECT
            id,
            time,
            received,
            adapter,
            category,
            payload->>'subject' as subject,
            ST_AsGeoJSON(geom) as geometry,
            payload as data,
            regions
        FROM public.events
        {where_clause}
        ORDER BY time DESC, id DESC
        LIMIT ${param_idx}
    """
    query_params.append(limit + 1)

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *query_params)
    except Exception as e:
        logger.error(f"Database error in _fetch_events: {e}")
        return EventsQueryResult([], None, "Database error")

    # Check if there is a next page
    has_next = len(rows) > limit
    if has_next:
        rows = rows[:limit]

    # Build response
    events = []
    for row in rows:
        geometry = None
        if row["geometry"]:
            geometry = json.loads(row["geometry"])

        events.append({
            "id": row["id"],
            "time": row["time"].isoformat(),
            "received": row["received"].isoformat(),
            "adapter": row["adapter"],
            "category": row["category"],
            "subject": row["subject"],
            "geometry": geometry,
            "data": dict(row["data"]) if row["data"] else {},
            "regions": list(row["regions"]) if row["regions"] else [],
        })

    # Build next_cursor if there are more results
    next_cursor = None
    if has_next and events:
        last_event = rows[-1]
        cursor_data = f"{last_event['time'].isoformat()}|{last_event['id']}"
        next_cursor = base64.b64encode(cursor_data.encode("utf-8")).decode("utf-8")

    return EventsQueryResult(events, next_cursor)


def _geometry_summary(geometry: dict | None) -> str:
    """Generate a human-readable summary of a geometry."""
    if not geometry:
        return "None"

    geom_type = geometry.get("type", "Unknown")

    if geom_type == "Point":
        return "Point"
    elif geom_type == "LineString":
        coords = geometry.get("coordinates", [])
        return f"Line ({len(coords)} pts)"
    elif geom_type == "Polygon":
        coords = geometry.get("coordinates", [[]])
        if coords:
            return f"Polygon ({len(coords[0])} pts)"
        return "Polygon"
    elif geom_type == "MultiPolygon":
        coords = geometry.get("coordinates", [])
        return f"MultiPolygon ({len(coords)} parts)"
    else:
        return geom_type



@router.get("/events.json")
async def events_json(request: Request):
    """
    Paginated, filterable JSON endpoint for events.

    Query parameters (all optional):
        adapter: filter by adapter name
        category: filter by event category
        since: ISO 8601 datetime - events where time >= since
        until: ISO 8601 datetime - events where time < until
        region_north, region_south, region_east, region_west: bbox filter (all four required if any)
        limit: page size (default 50, max 200)
        cursor: opaque pagination cursor

    Returns:
        {"events": [...], "next_cursor": string or null}
    """
    from fastapi.responses import JSONResponse

    params = request.query_params

    # Parse and validate parameters using shared helper
    parsed, error = _parse_events_params(params)
    if error:
        return JSONResponse({"error": error}, status_code=400)

    # Fetch events using shared helper
    result = await _fetch_events(parsed)
    if result.error:
        return JSONResponse({"error": result.error}, status_code=500)

    return JSONResponse({
        "events": result.events,
        "next_cursor": result.next_cursor,
    })


# --- Events feed frontend routes ---

@router.get("/events", response_class=HTMLResponse)
async def events_list(request: Request) -> HTMLResponse:
    """Events feed page with filter form, table, and map."""
    templates = _get_templates()
    operator = getattr(request.state, "operator", None)
    csrf_token = getattr(request.state, "csrf_token", "")

    params = request.query_params

    # Parse parameters
    parsed, error = _parse_events_params(params)

    # Get system settings for map tiles
    pool = get_pool()
    async with pool.acquire() as conn:
        system_row = await conn.fetchrow("SELECT map_tile_url, map_attribution FROM config.system")

    tile_url = system_row["map_tile_url"] if system_row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    tile_attribution = system_row["map_attribution"] if system_row else "OpenStreetMap"

    # Prepare filter values for template
    filter_values = {
        "adapter": params.get("adapter", ""),
        "category": params.get("category", ""),
        "since": params.get("since", ""),
        "until": params.get("until", ""),
        "region_north": params.get("region_north", ""),
        "region_south": params.get("region_south", ""),
        "region_east": params.get("region_east", ""),
        "region_west": params.get("region_west", ""),
        "limit": params.get("limit", "50"),
    }

    events = []
    next_cursor = None

    if error:
        # Validation error - show error banner but don't fail the page
        pass
    else:
        # Fetch events
        result = await _fetch_events(parsed)
        if result.error:
            error = result.error
        else:
            events = result.events
            next_cursor = result.next_cursor

    # Add geometry summary to each event
    for event in events:
        event["geometry_summary"] = _geometry_summary(event.get("geometry"))

    return templates.TemplateResponse(
        request=request,
        name="events_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "events": events,
            "next_cursor": next_cursor,
            "filter_values": filter_values,
            "filter_error": error,
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
        },
    )


@router.get("/events/rows", response_class=HTMLResponse)
async def events_rows(request: Request) -> HTMLResponse:
    """HTMX fragment: events table rows only (no page chrome)."""
    templates = _get_templates()

    params = request.query_params

    # Parse parameters
    parsed, error = _parse_events_params(params)

    # Prepare filter values for template
    filter_values = {
        "adapter": params.get("adapter", ""),
        "category": params.get("category", ""),
        "since": params.get("since", ""),
        "until": params.get("until", ""),
        "region_north": params.get("region_north", ""),
        "region_south": params.get("region_south", ""),
        "region_east": params.get("region_east", ""),
        "region_west": params.get("region_west", ""),
        "limit": params.get("limit", "50"),
    }

    events = []
    next_cursor = None

    if error:
        pass
    else:
        result = await _fetch_events(parsed)
        if result.error:
            error = result.error
        else:
            events = result.events
            next_cursor = result.next_cursor

    # Add geometry summary to each event
    for event in events:
        event["geometry_summary"] = _geometry_summary(event.get("geometry"))

    return templates.TemplateResponse(
        request=request,
        name="_events_rows.html",
        context={
            "events": events,
            "next_cursor": next_cursor,
            "filter_values": filter_values,
            "filter_error": error,
        },
    )
