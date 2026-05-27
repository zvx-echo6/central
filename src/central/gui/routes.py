"""Route handlers for Central GUI."""

import base64
import html
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import aiohttp

from central.cloudevents_wire import wrap_event
from central.config_store import ConfigStore
from central.tomtom_flow_parse import decode_flow_tile
from central.gui.nats import get_js

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
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

logger = logging.getLogger("central.gui.routes")


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


def _parse_model_list(form, field) -> list[dict]:
    """Reconstruct a list[dict] from indexed form keys ``<field>-<i>-<sub>``.
    Coerces numeric sub-fields; omits blank optional numbers (model default
    applies); drops fully-empty rows (e.g. a blank cloned template row)."""
    sub_widgets = {sf.name: sf.widget for sf in (field.sub_fields or [])}
    rows: dict[int, dict] = {}
    prefix = f"{field.name}-"
    for key, value in form.multi_items():
        if not key.startswith(prefix):
            continue
        idx_str, _, sub = key[len(prefix):].partition("-")
        if not sub or not idx_str.isdigit():
            continue
        rows.setdefault(int(idx_str), {})[sub] = value
    out: list[dict] = []
    for idx in sorted(rows):
        row: dict = {}
        for sub, raw in rows[idx].items():
            val = (raw or "").strip()
            if sub_widgets.get(sub) == "number":
                if val == "":
                    continue  # -> use model default (e.g. cadence_s=None)
                try:
                    row[sub] = float(val) if "." in val or "e" in val.lower() else int(val)
                except ValueError:
                    row[sub] = val  # let Pydantic raise a typed error
            else:
                row[sub] = val
        if any(v != "" for v in row.values()):
            out.append(row)
    return out


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

    # Read-only API-quota panel (adapters opt in via quota_estimate; duck-typed).
    quota: dict | None = None
    if adapter_cls is not None and hasattr(adapter_cls, "settings_schema"):
        try:
            quota = adapter_cls.quota_estimate(
                adapter_cls.settings_schema(**settings), row["cadence_s"]
            )
        except Exception:
            quota = None

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
            "quota": quota,
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
                elif field.widget == "model_list":
                    rows = _parse_model_list(form, field)
                    form_data[field.name] = rows
                    parsed_values[field.name] = rows
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

                    # Hard-block a save that would blow the provider free tier.
                    q = adapter_cls.quota_estimate(validated, cadence_s)
                    if q and q.get("blocked"):
                        ml = next((f.name for f in fields if f.widget == "model_list"), "quota")
                        errors[ml] = (
                            f"Estimated {q['calls_per_month']:,} calls/month exceeds the "
                            f"{q['cap']:,}/month free-tier cap — raise cadence or remove rows."
                        )
                except ValidationError as e:
                    ml_name = next((f.name for f in fields if f.widget == "model_list"), None)
                    for err in e.errors():
                        loc = err["loc"]
                        key = str(loc[0]) if loc else (ml_name or "unknown")
                        if len(loc) >= 2 and isinstance(loc[1], int):
                            errors[key] = f"Row {loc[1] + 1}: {err['msg']}"
                        else:
                            errors[key] = err["msg"]
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

            quota = None
            if adapter_cls and hasattr(adapter_cls, "settings_schema"):
                try:
                    quota = adapter_cls.quota_estimate(
                        adapter_cls.settings_schema(**(new_settings or current_settings)),
                        cadence_s,
                    )
                except Exception:
                    quota = None
            # list-of-model validation failures are a 422; single-region stays 200.
            status = 422 if any(f.widget == "model_list" for f in fields) else 200

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
                    "quota": quota,
                },
                status_code=status,
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


def _outer_enrichment_fields(current: dict) -> list[FieldDescriptor]:
    """EnrichmentConfig form fields EXCEPT backend_settings — that one is
    rendered as a per-backend <fieldset> via _backend_fields()."""
    from central.config_models import EnrichmentConfig

    return [
        f for f in describe_fields(EnrichmentConfig, current)
        if f.name != "backend_settings"
    ]


def _backend_fields(backend_class: str | None, current_bs: dict) -> list[FieldDescriptor]:
    """Field descriptors for the selected backend's settings_schema, or [] when
    the backend class is unknown. Same generic describe_fields machinery."""
    from central.supervisor import _BACKEND_REGISTRY

    cls = _BACKEND_REGISTRY.get(backend_class or "")
    if cls is None:
        return []
    return describe_fields(cls.settings_schema, current_bs or {})


async def _read_enrichment_row(conn) -> dict:
    row = await conn.fetchrow(
        """
        SELECT enricher_class, backend_class, backend_settings, cache_ttl_s
        FROM config.enrichment WHERE id = true
        """
    )
    return dict(row) if row is not None else {}


def _enrichment_context(request, *, outer_fields, backend_fields, backend_class,
                        errors=None, form_data=None, backend_form_data=None):
    return {
        "operator": request.state.operator,
        "csrf_token": request.state.csrf_token,
        "outer_fields": outer_fields,
        "backend_fields": backend_fields,
        "backend_class": backend_class,
        "errors": errors,
        "form_data": form_data,
        "backend_form_data": backend_form_data,
    }


@router.get("/enrichment", response_class=HTMLResponse)
async def enrichment_form(request: Request) -> HTMLResponse:
    """Render the enrichment config form (outer fields + a per-backend fieldset
    for the currently-selected backend_class)."""
    templates = _get_templates()
    pool = get_pool()

    async with pool.acquire() as conn:
        current = await _read_enrichment_row(conn)

    backend_class = current.get("backend_class") or "NoOpBackend"
    current_bs = current.get("backend_settings") or {}

    return templates.TemplateResponse(
        request=request,
        name="enrichment.html",
        context=_enrichment_context(
            request,
            outer_fields=_outer_enrichment_fields(current),
            backend_fields=_backend_fields(backend_class, current_bs),
            backend_class=backend_class,
        ),
    )


@router.post("/enrichment")
async def enrichment_update(request: Request) -> Response:
    """Validate + persist the enrichment config. Hot-reload picks it up via the
    config.enrichment NOTIFY trigger. backend_settings is validated against the
    SUBMITTED backend_class's settings_schema."""
    from central.config_models import EnrichmentConfig
    from central.supervisor import _BACKEND_REGISTRY

    templates = _get_templates()
    pool = get_pool()

    form = await request.form()
    if not form.get("csrf_token") or form.get("csrf_token") != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    errors: dict[str, str] = {}
    form_data: dict[str, Any] = {}
    backend_form_data: dict[str, Any] = {}
    parsed: dict[str, Any] = {}

    # --- outer EnrichmentConfig fields (backend_settings excluded) ---
    for field in _outer_enrichment_fields({}):
        raw = form.get(field.name, "")
        form_data[field.name] = raw
        if field.widget == "number":
            try:
                parsed[field.name] = int(raw) if raw else None
            except ValueError:
                errors[field.name] = f"{field.label} must be a number"
        else:  # text
            parsed[field.name] = raw.strip() if raw else None

    submitted_backend_class = parsed.get("backend_class")

    # --- backend settings fieldset, validated against the SUBMITTED backend ---
    backend_settings: dict[str, Any] = {}
    backend_cls = _BACKEND_REGISTRY.get(submitted_backend_class or "")
    if backend_cls is None and submitted_backend_class:
        errors["backend_class"] = f"Unknown backend: {submitted_backend_class}"
    elif backend_cls is not None:
        for f in describe_fields(backend_cls.settings_schema, {}):
            formkey = f"bs_{f.name}"
            raw = form.get(formkey, "")
            backend_form_data[formkey] = raw
            if f.widget == "checkbox":
                backend_settings[f.name] = formkey in form
            elif f.widget == "json":
                if raw and raw.strip():
                    try:
                        backend_settings[f.name] = json.loads(raw)
                    except json.JSONDecodeError as e:
                        errors[formkey] = f"{f.label} is not valid JSON: {e}"
                # blank -> omit, schema default applies
            else:  # text / number — let pydantic coerce, omit blanks for defaults
                if raw.strip() != "":
                    backend_settings[f.name] = raw.strip()
        if not errors:
            try:
                backend_settings = backend_cls.settings_schema.model_validate(
                    backend_settings
                ).model_dump()
            except ValidationError as e:
                for err in e.errors():
                    loc = err["loc"][0] if err["loc"] else "unknown"
                    errors[f"bs_{loc}"] = err["msg"]

    # --- outer EnrichmentConfig validation ---
    if not errors:
        try:
            validated = EnrichmentConfig(
                **{k: v for k, v in parsed.items() if v is not None},
                backend_settings=backend_settings,
            )
        except ValidationError as e:
            for err in e.errors():
                loc = err["loc"][0] if err["loc"] else "unknown"
                errors[str(loc)] = err["msg"]

    if errors:
        # Re-render against the SUBMITTED backend_class so field errors attach
        # to the right schema (operator may be mid-switch with a typo).
        return templates.TemplateResponse(
            request=request,
            name="enrichment.html",
            context=_enrichment_context(
                request,
                outer_fields=_outer_enrichment_fields({}),
                backend_fields=_backend_fields(submitted_backend_class, backend_settings),
                backend_class=submitted_backend_class,
                errors=errors,
                form_data=form_data,
                backend_form_data=backend_form_data,
            ),
            status_code=200,
        )

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


# --- Monitoring area (system-level archive bbox filter) --------------------

_DEFAULT_MONITOR = {"north": 44.5, "south": 41.8, "east": -111.0, "west": -117.5}


async def _read_monitoring_area(conn) -> dict[str, Any]:
    """Read the monitoring-area bbox + map tile settings from config.system."""
    row = await conn.fetchrow(
        "SELECT monitor_north, monitor_south, monitor_east, monitor_west, "
        "map_tile_url, map_attribution FROM config.system WHERE id = true"
    )
    if row is None:
        return {
            **_DEFAULT_MONITOR,
            "tile_url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "tile_attribution": "&copy; OpenStreetMap contributors",
        }
    return {
        "north": row["monitor_north"], "south": row["monitor_south"],
        "east": row["monitor_east"], "west": row["monitor_west"],
        "tile_url": row["map_tile_url"],
        "tile_attribution": row["map_attribution"],
    }


@router.get("/monitoring-area", response_class=HTMLResponse)
async def monitoring_area_form(request: Request) -> HTMLResponse:
    """Render the system monitoring-area editor (one draggable Leaflet rectangle).

    Events whose geometry falls entirely outside this box are dropped by the
    archive-level bbox filter; null-geom events are always kept."""
    templates = _get_templates()
    pool = get_pool()
    async with pool.acquire() as conn:
        area = await _read_monitoring_area(conn)
    return templates.TemplateResponse(
        request=request,
        name="monitoring_area.html",
        context={
            "operator": getattr(request.state, "operator", None),
            "csrf_token": request.state.csrf_token,
            "area": area,
            "tile_url": area["tile_url"],
            "tile_attribution": area["tile_attribution"],
        },
    )


@router.post("/monitoring-area")
async def monitoring_area_update(request: Request) -> Response:
    """Validate + persist the monitoring-area bbox. The archive applies the new
    bounds within ~60s via its background refresh (no restart needed)."""
    templates = _get_templates()
    pool = get_pool()

    form = await request.form()
    if not form.get("csrf_token") or form.get("csrf_token") != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    errors: dict[str, str] = {}
    vals: dict[str, float] = {}
    for key, lo, hi in (
        ("north", -90.0, 90.0), ("south", -90.0, 90.0),
        ("east", -180.0, 180.0), ("west", -180.0, 180.0),
    ):
        raw = form.get(f"monitor_{key}", "")
        try:
            v = float(raw)
        except (TypeError, ValueError):
            errors[key] = f"{key.title()} must be a number"
            continue
        if not (lo <= v <= hi):
            errors[key] = f"{key.title()} must be between {lo:g} and {hi:g}"
        else:
            vals[key] = v

    if not errors:
        if vals["north"] <= vals["south"]:
            errors["north"] = "North must be greater than South"
        if vals["east"] <= vals["west"]:
            errors["east"] = "East must be greater than West"

    if errors:
        async with pool.acquire() as conn:
            saved = await _read_monitoring_area(conn)
        render_area = {
            "north": form.get("monitor_north") or saved["north"],
            "south": form.get("monitor_south") or saved["south"],
            "east": form.get("monitor_east") or saved["east"],
            "west": form.get("monitor_west") or saved["west"],
        }
        return templates.TemplateResponse(
            request=request,
            name="monitoring_area.html",
            context={
                "operator": getattr(request.state, "operator", None),
                "csrf_token": request.state.csrf_token,
                "area": render_area,
                "tile_url": saved["tile_url"],
                "tile_attribution": saved["tile_attribution"],
                "errors": errors,
            },
            status_code=200,
        )

    async with pool.acquire() as conn:
        old = await conn.fetchrow(
            "SELECT monitor_north, monitor_south, monitor_east, monitor_west "
            "FROM config.system WHERE id = true"
        )
        await conn.execute(
            "UPDATE config.system SET monitor_north=$1, monitor_south=$2, "
            "monitor_east=$3, monitor_west=$4 WHERE id = true",
            vals["north"], vals["south"], vals["east"], vals["west"],
        )
        operator = getattr(request.state, "operator", None)
        await write_audit(
            conn, SYSTEM_UPDATE,
            operator_id=operator.id if operator else None,
            target="monitoring_area",
            before=dict(old) if old else None,
            after={"monitor_north": vals["north"], "monitor_south": vals["south"],
                   "monitor_east": vals["east"], "monitor_west": vals["west"]},
        )

    return RedirectResponse(url="/monitoring-area", status_code=302)


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
    def __init__(self, events: list, next_cursor: str | None, error: str | None = None,
                 total: int | None = None):
        self.events = events
        self.next_cursor = next_cursor
        self.error = error
        self.total = total  # filtered grand total (offset-mode only); None for cursor-mode


# --- v0.7.1 filtering: shared constants + pure helpers ----------------------

# Map a severity label (UI) to the numeric severity values it covers. Numeric
# scale per nws.SEVERITY_MAP (Extreme=4..Minor=1, Unknown=None). "unknown"
# also covers NULL and sev 0 (no assessment) -- handled in the query builder.
SEVERITY_LABELS: dict[str, list[int]] = {
    "critical": [4], "high": [3], "moderate": [2], "low": [1], "unknown": [0],
}
SEVERITY_ORDER = ["critical", "high", "moderate", "low", "unknown"]

TIME_PRESETS: dict[str, timedelta] = {
    "last_15m": timedelta(minutes=15),
    "last_1h": timedelta(hours=1),
    "last_6h": timedelta(hours=6),
    "last_24h": timedelta(hours=24),
    "last_7d": timedelta(days=7),
}
TIME_PRESET_LABELS = {
    "last_15m": "Last 15 min", "last_1h": "Last 1 hour", "last_6h": "Last 6 hours",
    "last_24h": "Last 24 hours", "last_7d": "Last 7 days",
    "active": "Active now", "all": "All time",
}
DEFAULT_TIME = "last_24h"

# Adapter -> domain grouping for the chip-picker (registry-derived; any adapter
# not listed here falls into "Other"). Group order is the display order.
ADAPTER_GROUPS = {
    "Disasters": ["gdacs", "firms", "inciweb", "wfigs_incidents", "wfigs_perimeters"],
    "Weather": ["nws"],
    "Space": ["swpc_alerts", "swpc_kindex", "swpc_protons"],
    "Geophysical": ["usgs_quake", "nwis"],
    "Earth Observation": ["eonet"],
    "Transportation": ["wzdx", "state_511_atis", "tomtom_flow", "tomtom_incidents", "state_511_atis_cameras"],
}
# Same palette the map legend uses, indexed by sorted-adapter position.
EVENTS_PALETTE = [
    "#f59e0b", "#dc2626", "#7c3aed", "#2563eb", "#059669", "#db2777",
    "#0891b2", "#65a30d", "#ea580c", "#4f46e5", "#9333ea", "#0d9488",
]


def _csv_param(params, name: str) -> list[str]:
    """Parse a comma-separated multi-value query param into a clean list."""
    raw = (params.get(name) or "").strip()
    return [v.strip() for v in raw.split(",") if v.strip()] if raw else []


def _query_items(params):
    """(key, value) pairs from Starlette QueryParams (multi-valued) or a plain
    dict (as used in unit tests)."""
    if hasattr(params, "multi_items"):
        return params.multi_items()
    return list(params.items())


def _ilike_escape(term: str) -> str:
    """Escape LIKE wildcards so user input is matched literally (ESCAPE '\\')."""
    return term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _resolve_time(token: str | None, now: datetime):
    """Resolve a time token to (since, until, active, error).

    'all'/None -> no time filter; 'active' -> active-now flag; presets -> since;
    'custom:<from_iso>,<to_iso>' -> explicit range. Pure (clock injected).
    """
    if token in (None, "", "all"):
        return None, None, False, None
    if token == "active":
        return None, None, True, None
    if token in TIME_PRESETS:
        return now - TIME_PRESETS[token], None, False, None
    if token.startswith("custom:"):
        f, _, t = token[len("custom:"):].partition(",")
        try:
            since = datetime.fromisoformat(f.replace("Z", "+00:00")) if f else None
            until = datetime.fromisoformat(t.replace("Z", "+00:00")) if t else None
        except ValueError:
            return None, None, False, "Invalid datetime in custom time range"
        if since and until and since > until:
            return None, None, False, "time 'from' must be before 'to'"
        return since, until, False, None
    return None, None, False, f"Unknown time preset: {token}"


def _class_adapter_names(data_class: str) -> list[str]:
    """Registry-derived adapter names for a data_class ('event' | 'telemetry')."""
    return sorted(
        name for name, cls in discover_adapters().items()
        if getattr(cls, "data_class", "event") == data_class
    )


def _adapter_filter_options(data_class: str | None = None):
    """Registry-derived (flat, grouped) adapter options for the chip-picker.

    flat: [{name, display_name, color}] sorted by name. grouped:
    [(group_label, [{value,label,color}])]. Colors are keyed to the FULL sorted
    registry (stable per-adapter across the /events and /telemetry tabs). When
    data_class is given, only that class's adapters are returned.
    """
    reg = discover_adapters()
    ordered = sorted(reg.values(), key=lambda c: c.name)
    color_by_name = {
        cls.name: EVENTS_PALETTE[i % len(EVENTS_PALETTE)] for i, cls in enumerate(ordered)
    }
    if data_class is not None:
        ordered = [c for c in ordered if getattr(c, "data_class", "event") == data_class]
    shown = {c.name for c in ordered}
    flat = [
        {"name": cls.name, "display_name": cls.display_name, "color": color_by_name[cls.name]}
        for cls in ordered
    ]
    grouped, grouped_names = [], set()
    for group_label, names in ADAPTER_GROUPS.items():
        items = [
            {"value": n, "label": reg[n].display_name, "color": color_by_name[n]}
            for n in names if n in reg and n in shown
        ]
        if items:
            grouped.append((group_label, items))
            grouped_names.update(items_n["value"] for items_n in items)
    leftover = [
        {"value": c.name, "label": c.display_name, "color": color_by_name[c.name]}
        for c in ordered if c.name not in grouped_names
    ]
    if leftover:
        grouped.append(("Other", leftover))
    return flat, grouped


def _build_active_pills(parsed: dict, adapter_total: int) -> list[dict]:
    """Server-side active-filter pills (descriptors) from parsed filter state.

    Each pill: {"key": <param name to clear>, "label": <human text>}.
    """
    pills: list[dict] = []
    if parsed.get("q"):
        pills.append({"key": "q", "label": f'Search: "{parsed["q"]}"'})
    if parsed.get("adapters"):
        pills.append({"key": "adapter",
                      "label": f'Adapters: {len(parsed["adapters"])} of {adapter_total}'})
    if parsed.get("categories"):
        pills.append({"key": "category", "label": f'Categories: {len(parsed["categories"])}'})
    if parsed.get("event_types"):
        pills.append({"key": "event_type",
                      "label": "Event types: " + ", ".join(parsed["event_types"])})
    if parsed.get("severities"):
        rank = {lbl: i for i, lbl in enumerate(SEVERITY_ORDER)}
        labs = sorted(parsed["severities"], key=lambda s: rank.get(s, 99))
        pills.append({"key": "severity", "label": "Severity: " + ", ".join(labs)})
    token = parsed.get("time_token")
    if token and token != "all":
        label = "Custom range" if token.startswith("custom:") else TIME_PRESET_LABELS.get(token, token)
        pills.append({"key": "time", "label": label})
    return pills


PER_PAGE_OPTIONS = [25, 50, 100, 250]


def _build_pagination(total: int | None, offset: int, limit: int) -> dict:
    """Compute offset-mode paginator state for the GUI table.

    Returns page/total_pages, the 1-based showing range (start..end), prev/next
    offsets, and a windowed list of page descriptors -- {page, offset, current}
    dicts interleaved with {"ellipsis": True} markers (1 ... 4 5 [6] 7 8 ... 47).
    """
    total = total or 0
    limit = max(1, limit)
    total_pages = max(1, (total + limit - 1) // limit)
    page = min(offset // limit + 1, total_pages)
    window = {1, total_pages}
    for p in range(page - 2, page + 3):
        if 1 <= p <= total_pages:
            window.add(p)
    pages, prev_p = [], 0
    for p in sorted(window):
        if prev_p and p - prev_p > 1:
            pages.append({"ellipsis": True})
        pages.append({"page": p, "offset": (p - 1) * limit, "current": p == page})
        prev_p = p
    return {
        "total": total, "offset": offset, "limit": limit,
        "page": page, "total_pages": total_pages,
        "start": offset + 1 if total else 0,
        "end": min(offset + limit, total),
        "prev_offset": offset - limit if page > 1 else None,
        "next_offset": offset + limit if page < total_pages else None,
        "pages": pages,
        "per_page_options": PER_PAGE_OPTIONS,
    }


def _parse_events_params(params, default_time: str | None = None,
                         default_offset: int | None = None,
                         default_show_removed: bool = True) -> tuple[dict | None, str | None]:
    """
    Parse and validate events query parameters.

    Multi-value filters (adapter, category, event_type, severity) are
    comma-separated. `time` is a single token (preset / active / all /
    custom:from,to); legacy since/until are still honored for the JSON API.
    `default_time` (GUI only) supplies the time token when none is given.

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

    if limit < 1 or limit > 250:
        return None, "limit must be between 1 and 250"

    # Offset pagination (GUI). Presence of `offset` selects offset-mode in
    # _fetch_events; its absence keeps the cursor path for events.json consumers.
    offset = None
    offset_str = params.get("offset")
    if offset_str is not None and offset_str != "":
        try:
            offset = int(offset_str)
        except ValueError:
            return None, f"Invalid offset value: {offset_str}"
        if offset < 0:
            return None, "offset must be >= 0"
    elif default_offset is not None:
        # GUI defaults to offset-mode (page 1) even when the URL omits offset.
        offset = default_offset

    # Multi-value filters (comma-separated; a single value is just a CSV of
    # length 1, preserving the old single-value API). Unknown severity labels
    # are ignored rather than erroring.
    q = (params.get("q") or "").strip()[:200] or None
    adapters = _csv_param(params, "adapter")
    categories = _csv_param(params, "category")
    event_types = _csv_param(params, "event_type")
    severities = [s for s in _csv_param(params, "severity") if s in SEVERITY_LABELS]

    # Time: a single token wins; otherwise legacy since/until (JSON API); then
    # the GUI-supplied default_time when nothing else is given.
    time_token = params.get("time") or None
    since = until = None
    active = False
    if time_token:
        since, until, active, terr = _resolve_time(time_token, datetime.now(timezone.utc))
        if terr:
            return None, terr
    else:
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
        if since and until and since > until:
            return None, "since must be before or equal to until"
        if since is None and until is None and default_time:
            time_token = default_time
            since, until, active, _ = _resolve_time(time_token, datetime.now(timezone.utc))

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

        # Defense in depth: the map JS normalizes coordinates, but a degenerate
        # or out-of-range bbox (e.g. Leaflet pan-past-dateline artifacts like
        # east=411.3, west=-608.2) must never reach ST_MakeEnvelope. Treat an
        # invalid envelope as "no bbox" rather than erroring or querying a bogus
        # envelope that silently matches the wrong rows.
        if not (
            -90 <= bbox["south"] < bbox["north"] <= 90
            and -180 <= bbox["west"] < bbox["east"] <= 180
        ):
            bbox = None

    # Map-filter toggle (v0.7.2): the bbox only constrains the query when the
    # operator has explicitly enabled "Filter table by map view". When off
    # (default), ignore any region_* params (e.g. from a bookmarked URL) so the
    # table shows all filter-matching events regardless of map zoom.
    map_filter = (params.get("map_filter") or "").lower() in ("1", "true", "on")
    if not map_filter:
        bbox = None

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

    # Tombstone visibility: the GUI default-hides `*.removed` events; the JSON
    # API default-includes them. An explicit ?show_removed= overrides the default.
    sr = params.get("show_removed")
    if sr is not None and sr != "":
        show_removed = sr.lower() in ("1", "true", "on")
    else:
        show_removed = default_show_removed

    return {
        "limit": limit,
        "show_removed": show_removed,
        "offset": offset,
        "q": q,
        "adapters": adapters,
        "categories": categories,
        "event_types": event_types,
        "severities": severities,
        "time_token": time_token,
        "since": since,
        "until": until,
        "active": active,
        "map_filter": map_filter,
        "bbox": bbox,
        "cursor_time": cursor_time,
        "cursor_id": cursor_id,
    }, None


def _derive_subject(event: dict) -> str | None:
    """Derive an event's plain-text subject for the JSON API.

    Renders the same per-adapter ``_event_summaries/{adapter}.html`` partial
    the /events table uses (falling back to ``_default.html``), so the JSON
    subject carries the same human text as the GUI's Subject cell with no
    duplicated derivation logic. The partials are HTML-autoescaped for the
    table (e.g. ``>`` -> ``&gt;``); we ``html.unescape`` so JSON consumers get
    plain text. Returns ``None`` when the partial yields no text -- an unknown
    adapter, or an event whose source fields don't support a subject (e.g. a
    wfigs row with neither county nor state).
    """
    template = _get_templates().env.select_template(
        [f"_event_summaries/{event.get('adapter')}.html", "_event_summaries/_default.html"]
    )
    return html.unescape(template.render(event=event)).strip() or None


async def _fetch_events(parsed_params: dict) -> EventsQueryResult:
    """
    Fetch events from database using parsed parameters.

    Returns EventsQueryResult with events list, next_cursor, and optional error.
    """
    pool = get_pool()

    limit = parsed_params["limit"]
    q = parsed_params.get("q")
    adapters = parsed_params.get("adapters") or []
    class_adapters = parsed_params.get("class_adapters")  # data_class split (GUI tabs)
    categories = parsed_params.get("categories") or []
    event_types = parsed_params.get("event_types") or []
    severities = parsed_params.get("severities") or []
    since = parsed_params["since"]
    until = parsed_params["until"]
    active = parsed_params.get("active", False)
    bbox = parsed_params["bbox"]
    cursor_time = parsed_params["cursor_time"]
    cursor_id = parsed_params["cursor_id"]
    offset = parsed_params.get("offset")
    offset_mode = offset is not None  # GUI; else cursor-mode (events.json)

    # Build query
    conditions = []
    query_params = []
    param_idx = 1

    if q:
        # Subject and location are both derived from the inner adapter payload
        # (payload->'data'->'data'), so a case-insensitive match over that
        # subtree's text covers both. Parameterized + LIKE-wildcards escaped.
        conditions.append(f"(payload->'data'->'data')::text ILIKE ${param_idx} ESCAPE '\\'")
        query_params.append(f"%{_ilike_escape(q)}%")
        param_idx += 1

    if adapters:
        conditions.append(f"adapter = ANY(${param_idx})")
        query_params.append(adapters)
        param_idx += 1

    if class_adapters is not None:
        # data_class split (/events vs /telemetry); registry-derived name list.
        conditions.append(f"adapter = ANY(${param_idx})")
        query_params.append(class_adapters)
        param_idx += 1

    if categories:
        conditions.append(f"category = ANY(${param_idx})")
        query_params.append(categories)
        param_idx += 1

    if event_types:
        conditions.append(f"split_part(category, '.', 1) = ANY(${param_idx})")
        query_params.append(event_types)
        param_idx += 1

    if severities:
        nums = sorted({n for lbl in severities for n in SEVERITY_LABELS.get(lbl, [])})
        if "unknown" in severities:
            # sev 0 and NULL both read as "unknown" (no assessment).
            conditions.append(f"(severity = ANY(${param_idx}) OR severity IS NULL)")
        else:
            conditions.append(f"severity = ANY(${param_idx})")
        query_params.append(nums)
        param_idx += 1

    if active:
        # Active now: started and not yet ended.
        conditions.append("time <= now() AND (expires IS NULL OR expires > now())")

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

    # GUI hides tombstones by default (events.json includes them). Static
    # literal pattern, no bound param; NULL-category rows are kept.
    if not parsed_params.get("show_removed", True):
        conditions.append("(category IS NULL OR category NOT LIKE '%.removed')")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    # Offset-mode (GUI): exact page slice + grand total in one roundtrip via a
    # window count. Cursor-mode (events.json): fetch limit+1 to detect next page.
    if offset_mode:
        total_select = ", count(*) OVER() AS total_count"
        limit_clause = f"LIMIT ${param_idx} OFFSET ${param_idx + 1}"
        query_params.append(limit)
        query_params.append(offset)
        param_idx += 2
    else:
        total_select = ""
        limit_clause = f"LIMIT ${param_idx}"
        query_params.append(limit + 1)
        param_idx += 1

    query = f"""
        SELECT
            id,
            time,
            received,
            adapter,
            category,
            severity,
            ST_AsGeoJSON(geom) as geometry,
            payload as data,
            regions{total_select}
        FROM public.events
        {where_clause}
        ORDER BY time DESC, id DESC
        {limit_clause}
    """

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *query_params)
    except Exception as e:
        logger.error(f"Database error in _fetch_events: {e}")
        return EventsQueryResult([], None, "Database error")

    total = None
    has_next = False
    if offset_mode:
        total = rows[0].get("total_count", 0) if rows else 0
    else:
        has_next = len(rows) > limit
        if has_next:
            rows = rows[:limit]

    # Build response
    events = []
    for row in rows:
        geometry = None
        if row["geometry"]:
            geometry = json.loads(row["geometry"])

        event = {
            "id": row["id"],
            "time": row["time"].isoformat(),
            "received": row["received"].isoformat(),
            "adapter": row["adapter"],
            "category": row["category"],
            "severity": row.get("severity"),
            "geometry": geometry,
            "data": dict(row["data"]) if row["data"] else {},
            "regions": list(row["regions"]) if row["regions"] else [],
        }
        # Subject is derived from the inner adapter payload by rendering the
        # same _event_summaries partial the /events table uses, so the JSON
        # `subject` matches the GUI's Subject cell. (The CloudEvents envelope
        # has no top-level `subject`; the old `payload->>'subject'` was always
        # null for every consumer.)
        event["subject"] = _derive_subject(event)
        events.append(event)

    # Build next_cursor (cursor-mode only) when there are more results.
    next_cursor = None
    if has_next and events:
        last_event = rows[-1]
        cursor_data = f"{last_event['time'].isoformat()}|{last_event['id']}"
        next_cursor = base64.b64encode(cursor_data.encode("utf-8")).decode("utf-8")

    return EventsQueryResult(events, next_cursor, total=total)


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


def _format_event_time(iso: str | None) -> str:
    """Format an ISO-8601 timestamp as 'MM-DD HH:MM UTC' (24h, no seconds, no
    year) for a single-line, stable-height table cell. The full timestamp
    (with year) stays available in the cell tooltip + the expanded detail row."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return iso
    return dt.strftime("%m-%d %H:%M") + " UTC"


def _decorate_table_events(events: list[dict]) -> None:
    """Add display-only fields used by the HTML events table (in place).

    These are for the table chrome only and are deliberately NOT added in
    _fetch_events, so the /events.json payload is unchanged. adapter_display
    is sourced from the registry (display_name), with the bare name as fallback;
    adapter_color is the same positional palette color the map + legend use.
    """
    display = {cls.name: cls.display_name for cls in discover_adapters().values()}
    color = {a["name"]: a["color"] for a in _adapter_filter_options()[0]}
    for event in events:
        event["geometry_summary"] = _geometry_summary(event.get("geometry"))
        event["time_human"] = _format_event_time(event.get("time"))
        event["adapter_display"] = display.get(event.get("adapter"), event.get("adapter"))
        event["adapter_color"] = color.get(event.get("adapter"), "#888")



# --- Fused fire view (v0.9.14): WFIGS perimeters + nearby FIRMS hotspots -----
# FIRMS hotspots carry no IrwinID, so the link is spatial+temporal: a hotspot is
# "confirmed" (part of a known fire) when it lies within FIRE_FUSE_RADIUS_M of a
# live perimeter AND within FIRE_FUSE_WINDOW_H of it. Hotspots matching no
# perimeter are "unconfirmed" (possible new fire ahead of an official perimeter).
# Read-only: SELECT-only, no DML.
FIRE_FUSE_RADIUS_M = 1000
FIRE_FUSE_WINDOW_H = 72


def _fused_bbox(params: dict) -> tuple[float, float, float, float] | None:
    """Parse optional north/south/east/west into (west, south, east, north), or
    None if absent/degenerate/out-of-range (mirrors _parse_events_params)."""
    try:
        n = float(params["north"])
        so = float(params["south"])
        e = float(params["east"])
        w = float(params["west"])
    except (KeyError, TypeError, ValueError):
        return None
    if -90 <= so < n <= 90 and -180 <= w < e <= 180:
        return (w, so, e, n)
    return None


def _shape_fused_fire(row) -> dict[str, Any]:
    """Shape a confirmed-fire row (perimeter + aggregated hotspots) for JSON."""
    return {
        "id": row["id"],
        "time": row["time"].isoformat(),
        "incident_name": row["incident_name"],
        "irwin_id": row["irwin_id"],
        "acres": row["acres"],
        "cause": row["cause"],
        "geometry": row["geometry"],
        "hotspot_count": row["hotspot_count"],
        "max_frp": row["max_frp"],
        "hotspots": row["hotspots"] or [],
    }


def _shape_unconfirmed_hotspot(row) -> dict[str, Any]:
    """Shape an unconfirmed FIRMS hotspot (no nearby perimeter) for JSON."""
    return {
        "id": row["id"],
        "time": row["time"].isoformat(),
        "geometry": row["geometry"],
        "frp": row["frp"],
        "confidence": row["confidence"],
        "satellite": row["satellite"],
    }


@router.get("/events/fire-fused.json")
async def fire_fused(request: Request) -> Response:
    """Fused fire view: each live WFIGS perimeter with its nearby/contemporaneous
    FIRMS hotspots, plus standalone ("unconfirmed") hotspots. Honors an optional
    north/south/east/west viewport bbox. Read-only."""
    pool = get_pool()
    bbox = _fused_bbox(dict(request.query_params))

    confirmed_sql = """
        SELECT p.id, p.time,
            p.payload->'data'->'data'->'raw'->>'poly_IncidentName' AS incident_name,
            p.payload->'data'->'data'->'raw'->>'attr_IrwinID' AS irwin_id,
            (p.payload->'data'->'data'->'raw'->>'poly_GISAcres')::float AS acres,
            p.payload->'data'->'data'->'raw'->>'attr_FireCause' AS cause,
            ST_AsGeoJSON(p.geom)::jsonb AS geometry,
            count(h.id) AS hotspot_count,
            max((h.payload->'data'->'data'->>'frp')::float) AS max_frp,
            COALESCE(jsonb_agg(jsonb_build_object(
                'geometry', ST_AsGeoJSON(h.geom)::jsonb,
                'frp', h.payload->'data'->'data'->>'frp',
                'confidence', h.payload->'data'->'data'->>'confidence',
                'satellite', h.payload->'data'->'data'->>'satellite',
                'time', h.time
            ) ORDER BY h.time DESC) FILTER (WHERE h.id IS NOT NULL), '[]'::jsonb) AS hotspots
        FROM events p
        LEFT JOIN events h
            ON h.adapter = 'firms' AND h.category NOT LIKE '%.removed' AND h.geom IS NOT NULL
            AND ST_DWithin(p.geom::geography, h.geom::geography, $1)
            AND h.time > p.time - ($2 * interval '1 hour')
        WHERE p.adapter = 'wfigs_perimeters' AND p.category NOT LIKE '%.removed'
            AND p.geom IS NOT NULL{p_bbox}
        GROUP BY p.id, p.time
        ORDER BY hotspot_count DESC
    """
    unconfirmed_sql = """
        SELECT h.id, h.time,
            ST_AsGeoJSON(h.geom)::jsonb AS geometry,
            h.payload->'data'->'data'->>'frp' AS frp,
            h.payload->'data'->'data'->>'confidence' AS confidence,
            h.payload->'data'->'data'->>'satellite' AS satellite
        FROM events h
        WHERE h.adapter = 'firms' AND h.category NOT LIKE '%.removed'
            AND h.geom IS NOT NULL{h_bbox}
            AND NOT EXISTS (
                SELECT 1 FROM events p
                WHERE p.adapter = 'wfigs_perimeters' AND p.category NOT LIKE '%.removed'
                    AND p.geom IS NOT NULL
                    AND ST_DWithin(p.geom::geography, h.geom::geography, $1)
                    AND h.time > p.time - ($2 * interval '1 hour')
            )
        ORDER BY h.time DESC
    """
    args: list[Any] = [FIRE_FUSE_RADIUS_M, FIRE_FUSE_WINDOW_H]
    p_bbox = h_bbox = ""
    if bbox:
        p_bbox = "\n            AND ST_Intersects(p.geom, ST_MakeEnvelope($3, $4, $5, $6, 4326))"
        h_bbox = "\n            AND ST_Intersects(h.geom, ST_MakeEnvelope($3, $4, $5, $6, 4326))"
        args.extend(bbox)

    async with pool.acquire() as conn:
        confirmed = await conn.fetch(confirmed_sql.format(p_bbox=p_bbox), *args)
        unconfirmed = await conn.fetch(unconfirmed_sql.format(h_bbox=h_bbox), *args)

    return JSONResponse({
        "fires": [_shape_fused_fire(r) for r in confirmed],
        "unconfirmed": [_shape_unconfirmed_hotspot(r) for r in unconfirmed],
    })


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

async def _events_query(request: Request, data_class: str):
    """Shared parse + class-scoped fetch for the /events and /telemetry tabs.

    Returns (parsed, error, events, next_cursor, total, class_adapters).
    The data_class split is registry-derived and injected as `class_adapters`,
    which _fetch_events applies as an `adapter = ANY(...)` condition.
    """
    params = request.query_params
    parsed, error = _parse_events_params(params, default_time=DEFAULT_TIME, default_offset=0,
                                         default_show_removed=False)
    class_adapters = _class_adapter_names(data_class)
    if parsed is not None:
        parsed["class_adapters"] = class_adapters

    events, next_cursor, total = [], None, 0
    if not error:
        result = await _fetch_events(parsed)
        if result.error:
            error = result.error
        else:
            events, next_cursor, total = result.events, result.next_cursor, result.total or 0
    _decorate_table_events(events)
    return parsed, error, events, next_cursor, total, class_adapters


def _events_filter_state(parsed: dict | None, params) -> dict:
    pstate = parsed or {}
    return {
        "q": pstate.get("q") or "",
        "adapters": pstate.get("adapters") or [],
        "categories": pstate.get("categories") or [],
        "event_types": pstate.get("event_types") or [],
        "severities": pstate.get("severities") or [],
        "time_token": pstate.get("time_token") or DEFAULT_TIME,
        "region_north": params.get("region_north", ""),
        "region_south": params.get("region_south", ""),
        "region_east": params.get("region_east", ""),
        "region_west": params.get("region_west", ""),
        "map_filter": pstate.get("map_filter", False),
        "limit": str(pstate.get("limit", 50)),
        "show_removed": pstate.get("show_removed", False),
    }


async def _events_page(request: Request, data_class: str, base_path: str) -> HTMLResponse:
    """Full events/telemetry page (shared by /events and /telemetry)."""
    templates = _get_templates()
    operator = getattr(request.state, "operator", None)
    csrf_token = getattr(request.state, "csrf_token", "")
    params = request.query_params

    parsed, error, events, next_cursor, total, class_adapters = \
        await _events_query(request, data_class)

    # System map tiles + DISTINCT filter-option lists, scoped to this data_class.
    pool = get_pool()
    all_categories: list[str] = []
    all_event_types: list[str] = []
    async with pool.acquire() as conn:
        system_row = await conn.fetchrow("SELECT map_tile_url, map_attribution FROM config.system")
        try:
            cat_rows = await conn.fetch(
                "SELECT DISTINCT category FROM events WHERE adapter = ANY($1) ORDER BY 1",
                class_adapters,
            )
            all_categories = [r["category"] for r in cat_rows]
            et_rows = await conn.fetch(
                "SELECT DISTINCT split_part(category, '.', 1) AS et FROM events "
                "WHERE adapter = ANY($1) ORDER BY 1",
                class_adapters,
            )
            all_event_types = [r["et"] for r in et_rows]
        except Exception:
            logger.warning("Failed to load filter options", exc_info=True)

    tile_url = system_row["map_tile_url"] if system_row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
    tile_attribution = system_row["map_attribution"] if system_row else "OpenStreetMap"

    pagination = _build_pagination(total, (parsed or {}).get("offset") or 0,
                                   (parsed or {}).get("limit") or 50)
    adapters_flat, adapters_grouped = _adapter_filter_options(data_class)
    active_pills = _build_active_pills(parsed or {}, len(adapters_flat)) if parsed else []
    query_string = urlencode([(k, v) for k, v in _query_items(params)
                              if k not in ("cursor", "offset")])

    return templates.TemplateResponse(
        request=request,
        name="events_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "base_path": base_path,
            "events": events,
            "next_cursor": next_cursor,
            "pagination": pagination,
            "filter_error": error,
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
            "adapters": adapters_flat,
            "adapters_grouped": adapters_grouped,
            "all_categories": all_categories,
            "all_event_types": all_event_types,
            "severity_order": SEVERITY_ORDER,
            "time_presets": [(t, TIME_PRESET_LABELS[t]) for t in
                             ("last_15m", "last_1h", "last_6h", "last_24h", "last_7d", "active", "all")],
            "filter_state": _events_filter_state(parsed, params),
            "active_pills": active_pills,
            "query_string": query_string,
        },
    )


async def _events_rows_fragment(request: Request, data_class: str, base_path: str) -> HTMLResponse:
    """HTMX rows fragment (shared by /events/rows and /telemetry/rows)."""
    templates = _get_templates()
    params = request.query_params

    parsed, error, events, next_cursor, total, _ = await _events_query(request, data_class)

    pagination = _build_pagination(total, (parsed or {}).get("offset") or 0,
                                   (parsed or {}).get("limit") or 50)
    adapters_flat, _ = _adapter_filter_options(data_class)
    active_pills = _build_active_pills(parsed or {}, len(adapters_flat)) if parsed else []
    query_string = urlencode([(k, v) for k, v in _query_items(params)
                              if k not in ("cursor", "offset")])

    response = templates.TemplateResponse(
        request=request,
        name="_events_rows.html",
        context={
            "base_path": base_path,
            "events": events,
            "next_cursor": next_cursor,
            "pagination": pagination,
            "filter_error": error,
            "active_pills": active_pills,
            "query_string": query_string,
            "oob_pills": True,  # emit an out-of-band #active-pills update on swap
        },
    )
    # Push the bookmarkable full-page URL (not the fragment path).
    response.headers["HX-Push-Url"] = base_path + "?" + str(request.url.query)
    return response


@router.get("/events", response_class=HTMLResponse)
async def events_list(request: Request) -> HTMLResponse:
    """Events feed page (data_class=event): filter form, table, and map."""
    return await _events_page(request, "event", "/events")


@router.get("/events/rows", response_class=HTMLResponse)
async def events_rows(request: Request) -> HTMLResponse:
    return await _events_rows_fragment(request, "event", "/events")


@router.get("/telemetry", response_class=HTMLResponse)
async def telemetry_list(request: Request) -> HTMLResponse:
    """Telemetry feed page (data_class=telemetry; e.g. NWIS). Same shape as /events."""
    return await _events_page(request, "telemetry", "/telemetry")


@router.get("/telemetry/rows", response_class=HTMLResponse)
async def telemetry_rows(request: Request) -> HTMLResponse:
    return await _events_rows_fragment(request, "telemetry", "/telemetry")


# --- Traffic flow passthrough (Navi, v0.9.4) ---------------------------------
# Drop-in for navi-traffic's /api/traffic/flow/<z>/<x>/<y>.{png,pbf} (no auth --
# tailnet-trusted, exempted in middleware). PNG = passthrough + 60s cache; PBF =
# passthrough + decode-and-persist to CENTRAL_TRAFFIC_FLOW (best-effort; the
# minute-bucketed ids collide with the tomtom_flow poller so the archive's
# (id, time) upsert collapses duplicates). Single-flight is a v0.9.5 candidate.
_TOMTOM_FLOW_URL = "https://api.tomtom.com/maps/orbis/traffic/tile/flow/{z}/{x}/{y}.{fmt}?key={key}&apiVersion=1"
_FLOW_MEDIA = {"png": "image/png", "pbf": "application/x-protobuf"}
_FLOW_CACHE = "public, max-age=60"  # matches TomTom's advertised 60s tile TTL


async def _fetch_tomtom_tile(z: int, x: int, y: int, fmt: str, key: str) -> bytes:
    """Fetch one Orbis flow tile from TomTom. Raises on HTTP/transport error."""
    url = _TOMTOM_FLOW_URL.format(z=z, x=x, y=y, fmt=fmt, key=key)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(
        timeout=timeout, headers={"User-Agent": "Central/0.9 (+tomtom_flow_passthrough)"}
    ) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.read()


async def _persist_flow_tile(pbf: bytes, z: int, x: int, y: int) -> None:
    """Decode a vector tile and publish its segments to CENTRAL_TRAFFIC_FLOW.

    Reuses the polling adapter's decode + minute-bucketed ids so the archive
    dedups poller/passthrough overlap. Caller wraps this best-effort.
    """
    js = get_js()
    if js is None:
        return
    subject = f"central.traffic_flow.{z}.{x}.{y}"
    for ev in decode_flow_tile(pbf, z, x, y, datetime.now(timezone.utc)):
        envelope, msg_id = wrap_event(ev)
        await js.publish(subject, json.dumps(envelope).encode(), headers={"Nats-Msg-Id": msg_id})


async def _flow_passthrough(z: int, x: int, y: int, fmt: str) -> Response:
    """Proxy a TomTom Orbis flow tile; persist segments on .pbf (best-effort)."""
    key = await ConfigStore(get_pool()).get_api_key("tomtom")
    if not key:
        return Response(
            content=json.dumps({"error": "tomtom api key not configured"}),
            status_code=503, media_type="application/json",
        )
    try:
        tile = await _fetch_tomtom_tile(z, x, y, fmt, key)
    except Exception as exc:
        logger.warning("flow passthrough upstream failed",
                       extra={"tile": [z, x, y], "fmt": fmt, "error": str(exc).replace(key, "<KEY>")})
        return Response(
            content=json.dumps({"error": "upstream tile fetch failed"}),
            status_code=502, media_type="application/json",
        )
    if fmt == "pbf":
        try:
            await _persist_flow_tile(tile, z, x, y)
        except Exception:  # storage is secondary to serving the tile
            logger.exception("flow passthrough persist failed", extra={"tile": [z, x, y]})
    return Response(content=tile, media_type=_FLOW_MEDIA[fmt], headers={"Cache-Control": _FLOW_CACHE})


@router.get("/api/traffic/flow/{z}/{x}/{y}.png")
async def traffic_flow_png(z: int, x: int, y: int) -> Response:
    return await _flow_passthrough(z, x, y, "png")


@router.get("/api/traffic/flow/{z}/{x}/{y}.pbf")
async def traffic_flow_pbf(z: int, x: int, y: int) -> Response:
    return await _flow_passthrough(z, x, y, "pbf")
