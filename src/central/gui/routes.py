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
from central.gui.db import get_pool

router = APIRouter()

# Streams to display on dashboard
DASHBOARD_STREAMS = ["CENTRAL_WX", "CENTRAL_FIRE", "CENTRAL_QUAKE", "CENTRAL_META"]

# Email validation regex (simple but effective)
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _get_valid_satellites() -> list[str]:
    """Get valid satellite identifiers from firms adapter."""
    from central.adapters.firms import SATELLITE_SHORT
    return list(SATELLITE_SHORT.keys())


def _get_valid_feeds() -> set[str]:
    """Get valid feed values from usgs_quake adapter."""
    from central.adapters.usgs_quake import VALID_FEEDS
    return VALID_FEEDS


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


@router.get("/setup/operator", response_class=HTMLResponse)
async def setup_operator_form(
    request: Request,
) -> HTMLResponse:
    """Render the setup operator form (step 1)."""
    templates = _get_templates()
    pool = get_pool()
    settings = get_settings()
    csrf_token, signed_token = generate_pre_auth_csrf(settings.csrf_secret)

    # Check if operator already exists
    existing_operator = None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT username FROM config.operators ORDER BY id LIMIT 1"
        )
        if row:
            existing_operator = {"username": row["username"]}

    response = templates.TemplateResponse(
        request=request,
        name="setup_operator.html",
        context={
            "csrf_token": csrf_token,
            "error": None,
            "form_data": None,
            "existing_operator": existing_operator,
        },
    )
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
    templates = _get_templates()
    pool = get_pool()

    # Validate CSRF
    settings = get_settings()
    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not validate_pre_auth_csrf(request, form_csrf, settings.csrf_secret):
        raise CsrfValidationError("Invalid CSRF token")

    # Check if operator already exists (single-operator-per-install design)
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM config.operators")
        if count > 0:
            # Operator already exists — render confirmation page
            existing = await conn.fetchrow(
                "SELECT username FROM config.operators ORDER BY id LIMIT 1"
            )
            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="setup_operator.html",
                context={
                    "csrf_token": csrf_token,
                    "error": None,
                    "form_data": None,
                    "existing_operator": {"username": existing["username"]},
                },
            )
            return response

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
        csrf_token = request.state.csrf_token
        response = templates.TemplateResponse(
            request=request,
            name="setup_operator.html",
            context={
                "csrf_token": csrf_token,
                "error": error,
                "form_data": {"username": username},
                "existing_operator": None,
            },
            status_code=200,
        )
        return response

    # Create operator
    password_hash = hash_password(password)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO config.operators (username, password_hash)
            VALUES ($1, $2)
            RETURNING id
            """,
            username,
            password_hash,
        )
        operator_id = row["id"]

        # Write audit log
        await write_audit(
            conn,
            OPERATOR_CREATE,
            operator_id=operator_id,
            target=username,
        )

        # Get session lifetime
        sysrow = await conn.fetchrow(
            "SELECT session_lifetime_days FROM config.system WHERE id = true"
        )
        lifetime_days = sysrow["session_lifetime_days"] if sysrow else 90

        # Create session
        token, expires_at, _ = await create_session(conn, operator_id, lifetime_days)

    # Redirect to next step with session cookie
    response = RedirectResponse(url="/setup/system", status_code=302)
    _set_session_cookie(response, token, lifetime_days * 86400)
    return response


@router.get("/setup/system", response_class=HTMLResponse)
async def setup_system_form(
    request: Request,

) -> HTMLResponse:
    """Render the system settings form (step 2)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
        )
        system = {
            "map_tile_url": row["map_tile_url"] if row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "map_attribution": row["map_attribution"] if row else "&copy; OpenStreetMap contributors",
        }

    csrf_token = request.state.csrf_token
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
    return response


@router.post("/setup/system")
async def setup_system_submit(
    request: Request,

) -> Response:
    """Process the system settings form (step 2)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
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

    async with pool.acquire() as conn:
        if errors:
            row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            system = {
                "map_tile_url": row["map_tile_url"] if row else "",
                "map_attribution": row["map_attribution"] if row else "",
            }

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="setup_system.html",
                context={
                    "csrf_token": csrf_token,
                    "error": None,
                    "errors": errors,
                    "form_data": form_data,
                    "system": system,
                },
                status_code=200,
            )
            return response

        # Get current values for audit
        old_row = await conn.fetchrow(
            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
        )
        before = {
            "map_tile_url": old_row["map_tile_url"] if old_row else None,
            "map_attribution": old_row["map_attribution"] if old_row else None,
        }

        # Update system settings
        await conn.execute(
            """
            UPDATE config.system
            SET map_tile_url = $1, map_attribution = $2
            WHERE id = true
            """,
            map_tile_url,
            map_attribution,
        )

        # Write audit log
        await write_audit(
            conn,
            SYSTEM_UPDATE,
            operator_id=operator.id,
            target="system",
            before=before,
            after={"map_tile_url": map_tile_url, "map_attribution": map_attribution},
        )

    return RedirectResponse(url="/setup/keys", status_code=302)


@router.get("/setup/keys", response_class=HTMLResponse)
async def setup_keys_form(
    request: Request,

) -> HTMLResponse:
    """Render the API keys form (step 3)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    from central.crypto import encrypt

    templates = _get_templates()
    pool = get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT alias, created_at FROM config.api_keys ORDER BY alias"
        )
        keys = [{"alias": row["alias"], "created_at": row["created_at"]} for row in rows]

    csrf_token = request.state.csrf_token
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
    return response


@router.post("/setup/keys")
async def setup_keys_submit(
    request: Request,

) -> Response:
    """Process the API keys form (step 3)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
    action = form.get("action", "add")

    # If action is "next", redirect to adapters step
    if action == "next":
        return RedirectResponse(url="/setup/adapters", status_code=302)

    from central.crypto import encrypt

    templates = _get_templates()
    pool = get_pool()

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

    # Validate plaintext_key
    if not plaintext_key:
        errors["plaintext_key"] = "API key is required"
    elif len(plaintext_key) > 4096:
        errors["plaintext_key"] = "API key must be at most 4096 characters"

    async with pool.acquire() as conn:
        if not errors:
            # Check if alias already exists
            existing = await conn.fetchrow(
                "SELECT alias FROM config.api_keys WHERE alias = $1",
                alias,
            )
            if existing:
                errors["alias"] = "An API key with this alias already exists"

        keys = await conn.fetch(
            "SELECT alias, created_at FROM config.api_keys ORDER BY alias"
        )
        keys = [{"alias": row["alias"], "created_at": row["created_at"]} for row in keys]

        if errors:
            csrf_token = request.state.csrf_token
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
            return response

        # Encrypt the key
        encrypted_value = encrypt(plaintext_key.encode())

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

        # Refresh keys list
        keys = await conn.fetch(
            "SELECT alias, created_at FROM config.api_keys ORDER BY alias"
        )
        keys = [{"alias": row["alias"], "created_at": row["created_at"]} for row in keys]

    # Re-render with success message
    csrf_token = request.state.csrf_token
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
    return response


@router.get("/setup/adapters", response_class=HTMLResponse)
async def setup_adapters_form(
    request: Request,

) -> HTMLResponse:
    """Render the adapters configuration form (step 4)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, enabled, cadence_s, settings
            FROM config.adapters
            ORDER BY name
            """
        )
        adapters = []
        for row in rows:
            settings = row["settings"] or {}
            adapters.append({
                "name": row["name"],
                "enabled": row["enabled"],
                "cadence_s": row["cadence_s"],
                "settings": settings,
            })

        # Get API keys for dropdown
        api_keys = await conn.fetch(
            "SELECT alias FROM config.api_keys ORDER BY alias"
        )

        # Get map tile settings
        sys_row = await conn.fetchrow(
            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
        )
        tile_url = sys_row["map_tile_url"] if sys_row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="setup_adapters.html",
        context={
            "csrf_token": csrf_token,
            "adapters": adapters,
            "api_keys": [{"alias": k["alias"]} for k in api_keys],
            "valid_satellites": _get_valid_satellites(),
            "valid_feeds": sorted(_get_valid_feeds()),
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
            "error": None,
            "errors": None,
            "form_data": None,
        },
    )
    return response


@router.post("/setup/adapters")
async def setup_adapters_submit(
    request: Request,

) -> Response:
    """Process the adapters configuration form (step 4)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    form = await request.form()
    errors: dict[str, str] = {}

    async with pool.acquire() as conn:
        # Get current adapters
        rows = await conn.fetch(
            """
            SELECT name, enabled, cadence_s, settings
            FROM config.adapters
            ORDER BY name
            """
        )

        for row in rows:
            adapter_name = row["name"]
            current_settings = row["settings"] or {}
            new_settings = dict(current_settings)

            # Parse enabled
            enabled = f"{adapter_name}_enabled" in form

            # Parse cadence
            cadence_str = form.get(f"{adapter_name}_cadence_s", "")
            try:
                cadence_s = int(cadence_str)
                if cadence_s < 60 or cadence_s > 3600:
                    errors[f"{adapter_name}_cadence_s"] = "Cadence must be between 60 and 3600 seconds"
            except ValueError:
                errors[f"{adapter_name}_cadence_s"] = "Cadence must be a valid integer"
                cadence_s = row["cadence_s"]

            # Adapter-specific validation
            if adapter_name == "nws":
                contact_email = form.get(f"{adapter_name}_contact_email", "").strip()
                if enabled:
                    if not contact_email:
                        errors[f"{adapter_name}_contact_email"] = "Contact email is required when enabled"
                    elif not EMAIL_REGEX.match(contact_email):
                        errors[f"{adapter_name}_contact_email"] = "Invalid email format"
                    else:
                        new_settings["contact_email"] = contact_email
                else:
                    new_settings["contact_email"] = contact_email if contact_email else current_settings.get("contact_email")

            elif adapter_name == "firms":
                api_key_alias = form.get(f"{adapter_name}_api_key_alias", "").strip()
                satellites = form.getlist(f"{adapter_name}_satellites")

                if api_key_alias:
                    key_exists = await conn.fetchrow(
                        "SELECT 1 FROM config.api_keys WHERE alias = $1",
                        api_key_alias,
                    )
                    if not key_exists:
                        errors[f"{adapter_name}_api_key_alias"] = f"API key alias '{api_key_alias}' does not exist"
                    else:
                        new_settings["api_key_alias"] = api_key_alias
                else:
                    new_settings["api_key_alias"] = None

                # Validate satellites
                valid_sats = set(_get_valid_satellites())
                invalid_sats = [s for s in satellites if s not in valid_sats]
                if invalid_sats:
                    errors[f"{adapter_name}_satellites"] = f"Invalid satellites: {', '.join(invalid_sats)}"
                else:
                    new_settings["satellites"] = satellites

            elif adapter_name == "usgs_quake":
                feed = form.get(f"{adapter_name}_feed", "").strip()
                valid_feeds = _get_valid_feeds()
                if feed not in valid_feeds:
                    errors[f"{adapter_name}_feed"] = f"Invalid feed"
                else:
                    new_settings["feed"] = feed

            # Region validation
            region_north_str = form.get(f"{adapter_name}_region_north", "").strip()
            region_south_str = form.get(f"{adapter_name}_region_south", "").strip()
            region_east_str = form.get(f"{adapter_name}_region_east", "").strip()
            region_west_str = form.get(f"{adapter_name}_region_west", "").strip()

            try:
                region_north = float(region_north_str)
                region_south = float(region_south_str)
                region_east = float(region_east_str)
                region_west = float(region_west_str)

                if not (-90 <= region_south < region_north <= 90):
                    errors[f"{adapter_name}_region"] = "Invalid latitude: south must be less than north, both between -90 and 90"
                elif not (-180 <= region_west < region_east <= 180):
                    errors[f"{adapter_name}_region"] = "Invalid longitude: west must be less than east, both between -180 and 180"
                else:
                    new_settings["region"] = {
                        "north": region_north,
                        "south": region_south,
                        "east": region_east,
                        "west": region_west,
                    }
            except ValueError:
                errors[f"{adapter_name}_region"] = "Region coordinates must be valid numbers"

            # Store parsed data for re-render on error or update
            if not errors.get(f"{adapter_name}_cadence_s"):
                # Update adapter
                await conn.execute(
                    """
                    UPDATE config.adapters
                    SET enabled = $1, cadence_s = $2, settings = $3, updated_at = now()
                    WHERE name = $4
                    """,
                    enabled,
                    cadence_s,
                    new_settings,
                    adapter_name,
                )

        # If any errors, re-render
        if errors:
            adapters = []
            rows = await conn.fetch(
                """
                SELECT name, enabled, cadence_s, settings
                FROM config.adapters
                ORDER BY name
                """
            )
            for row in rows:
                settings = row["settings"] or {}
                adapters.append({
                    "name": row["name"],
                    "enabled": row["enabled"],
                    "cadence_s": row["cadence_s"],
                    "settings": settings,
                })

            api_keys = await conn.fetch(
                "SELECT alias FROM config.api_keys ORDER BY alias"
            )

            sys_row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            tile_url = sys_row["map_tile_url"] if sys_row else "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="setup_adapters.html",
                context={
                    "csrf_token": csrf_token,
                    "adapters": adapters,
                    "api_keys": [{"alias": k["alias"]} for k in api_keys],
                    "valid_satellites": _get_valid_satellites(),
                    "valid_feeds": sorted(_get_valid_feeds()),
                    "tile_url": tile_url,
                    "tile_attribution": tile_attribution,
                    "error": "Please fix the errors below.",
                    "errors": errors,
                    "form_data": form,
                },
                status_code=200,
            )
            return response

    return RedirectResponse(url="/setup/finish", status_code=302)


@router.get("/setup/finish", response_class=HTMLResponse)
async def setup_finish_form(
    request: Request,

) -> HTMLResponse:
    """Render the finish setup page (step 5)."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    templates = _get_templates()
    pool = get_pool()

    async with pool.acquire() as conn:
        # Get counts
        operator_count = await conn.fetchval("SELECT COUNT(*) FROM config.operators")
        key_count = await conn.fetchval("SELECT COUNT(*) FROM config.api_keys")

        # Get system settings
        sys_row = await conn.fetchrow(
            "SELECT map_tile_url FROM config.system WHERE id = true"
        )
        system = {
            "map_tile_url": sys_row["map_tile_url"] if sys_row else "",
        }

        # Get adapters
        rows = await conn.fetch(
            """
            SELECT name, enabled, cadence_s
            FROM config.adapters
            ORDER BY name
            """
        )
        adapters = [
            {
                "name": row["name"],
                "enabled": row["enabled"],
                "cadence_s": row["cadence_s"],
            }
            for row in rows
        ]

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="setup_finish.html",
        context={
            "csrf_token": csrf_token,
            "operator_count": operator_count,
            "key_count": key_count,
            "system": system,
            "adapters": adapters,
        },
    )
    return response


@router.post("/setup/finish")
async def setup_finish_submit(
    request: Request,

) -> Response:
    """Complete the setup wizard."""
    # Require authentication for this step
    operator = getattr(request.state, "operator", None)
    if operator is None:
        return RedirectResponse(url="/setup/operator", status_code=302)

    pool = get_pool()

    form = await request.form()
    form_csrf = form.get("csrf_token", "")
    if not form_csrf or form_csrf != request.state.csrf_token:
        raise CsrfValidationError("Invalid CSRF token")

    async with pool.acquire() as conn:
        # Mark setup complete
        await conn.execute(
            "UPDATE config.system SET setup_complete = true WHERE id = true"
        )

        # Write audit log
        await write_audit(
            conn,
            SETUP_COMPLETE,
            operator_id=operator.id,
            target="system",
        )

    return RedirectResponse(url="/", status_code=302)


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

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at
            FROM config.adapters
            ORDER BY name
            """
        )

    adapters = []
    for row in rows:
        settings = row["settings"] or {}
        adapters.append({
            "name": row["name"],
            "enabled": row["enabled"],
            "cadence_s": row["cadence_s"],
            "settings": settings,
            "paused_at": row["paused_at"],
            "updated_at": row["updated_at"],
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

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at
            FROM config.adapters
            WHERE name = $1
            """,
            name,
        )

        if row is None:
            return Response(status_code=404, content="Adapter not found")

        # Get API keys for firms dropdown
        api_keys = await conn.fetch(
            "SELECT alias FROM config.api_keys ORDER BY alias"
        )

        # Get map tile settings from config.system
        sys_row = await conn.fetchrow(
            "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
        )
        tile_url = sys_row["map_tile_url"] if sys_row else "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

    settings = row["settings"] or {}
    adapter = {
        "name": row["name"],
        "enabled": row["enabled"],
        "cadence_s": row["cadence_s"],
        "settings": settings,
        "paused_at": row["paused_at"],
        "updated_at": row["updated_at"],
    }

    csrf_token = request.state.csrf_token
    response = templates.TemplateResponse(
        request=request,
        name="adapters_edit.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "adapter": adapter,
            "errors": None,
            "form_data": None,
            "api_keys": [{"alias": k["alias"]} for k in api_keys],
            "valid_satellites": _get_valid_satellites(),
            "valid_feeds": sorted(_get_valid_feeds()),
            "tile_url": tile_url,
            "tile_attribution": tile_attribution,
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

    # Parse form data
    form = await request.form()
    enabled = "enabled" in form
    cadence_s_str = form.get("cadence_s", "")

    # Build form_data for re-render on error
    form_data: dict[str, Any] = {
        "enabled": enabled,
        "cadence_s": cadence_s_str,
    }

    errors: dict[str, str] = {}

    # Validate cadence_s
    try:
        cadence_s = int(cadence_s_str)
        if cadence_s < 60 or cadence_s > 3600:
            errors["cadence_s"] = "Cadence must be between 60 and 3600 seconds"
    except ValueError:
        errors["cadence_s"] = "Cadence must be a valid integer"
        cadence_s = 0

    async with pool.acquire() as conn:
        # Get current adapter state
        row = await conn.fetchrow(
            """
            SELECT name, enabled, cadence_s, settings, paused_at, updated_at
            FROM config.adapters
            WHERE name = $1
            """,
            name,
        )

        if row is None:
            return Response(status_code=404, content="Adapter not found")

        current_settings = row["settings"] or {}
        new_settings = dict(current_settings)

        # Adapter-specific validation and settings update
        if name == "nws":
            contact_email = form.get("contact_email", "").strip()
            form_data["contact_email"] = contact_email
            if not contact_email:
                errors["contact_email"] = "Contact email is required"
            elif not EMAIL_REGEX.match(contact_email):
                errors["contact_email"] = "Invalid email format"
            else:
                new_settings["contact_email"] = contact_email

        elif name == "firms":
            api_key_alias = form.get("api_key_alias", "").strip()
            satellites = form.getlist("satellites")
            form_data["api_key_alias"] = api_key_alias
            form_data["satellites"] = satellites

            # Validate api_key_alias if set
            if api_key_alias:
                key_exists = await conn.fetchrow(
                    "SELECT 1 FROM config.api_keys WHERE alias = $1",
                    api_key_alias,
                )
                if not key_exists:
                    errors["api_key_alias"] = f"API key alias '{api_key_alias}' does not exist"
                else:
                    new_settings["api_key_alias"] = api_key_alias
            else:
                new_settings["api_key_alias"] = None

            # Validate satellites
            valid_sats = set(_get_valid_satellites())
            invalid_sats = [s for s in satellites if s not in valid_sats]
            if invalid_sats:
                errors["satellites"] = f"Invalid satellites: {', '.join(invalid_sats)}"
            else:
                new_settings["satellites"] = satellites

        elif name == "usgs_quake":
            feed = form.get("feed", "").strip()
            form_data["feed"] = feed
            valid_feeds = _get_valid_feeds()
            if feed not in valid_feeds:
                errors["feed"] = f"Invalid feed. Must be one of: {', '.join(sorted(valid_feeds))}"
            else:
                new_settings["feed"] = feed

        # Region validation (applies to all adapters)
        region_north_str = form.get("region_north", "").strip()
        region_south_str = form.get("region_south", "").strip()
        region_east_str = form.get("region_east", "").strip()
        region_west_str = form.get("region_west", "").strip()

        form_data["region_north"] = region_north_str
        form_data["region_south"] = region_south_str
        form_data["region_east"] = region_east_str
        form_data["region_west"] = region_west_str

        try:
            region_north = float(region_north_str)
            region_south = float(region_south_str)
            region_east = float(region_east_str)
            region_west = float(region_west_str)

            # Validate latitude bounds
            if not (-90 <= region_south < region_north <= 90):
                errors["region"] = "Invalid latitude: south must be less than north, both between -90 and 90"
            # Validate longitude bounds
            elif not (-180 <= region_west < region_east <= 180):
                errors["region"] = "Invalid longitude: west must be less than east, both between -180 and 180"
            else:
                new_settings["region"] = {
                    "north": region_north,
                    "south": region_south,
                    "east": region_east,
                    "west": region_west,
                }
        except ValueError:
            errors["region"] = "Region coordinates must be valid numbers"

        # If there are errors, re-render the form
        if errors:
            adapter = {
                "name": row["name"],
                "enabled": row["enabled"],
                "cadence_s": row["cadence_s"],
                "settings": current_settings,
                "paused_at": row["paused_at"],
                "updated_at": row["updated_at"],
            }

            api_keys = await conn.fetch(
                "SELECT alias FROM config.api_keys ORDER BY alias"
            )

            # Get map tile settings for re-render
            sys_row = await conn.fetchrow(
                "SELECT map_tile_url, map_attribution FROM config.system WHERE id = true"
            )
            tile_url = sys_row["map_tile_url"] if sys_row else "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            tile_attribution = sys_row["map_attribution"] if sys_row else "&copy; OpenStreetMap contributors"

            csrf_token = request.state.csrf_token
            response = templates.TemplateResponse(
                request=request,
                name="adapters_edit.html",
                context={
                    "operator": operator,
                    "csrf_token": csrf_token,
                    "adapter": adapter,
                    "errors": errors,
                    "form_data": form_data,
                    "api_keys": [{"alias": k["alias"]} for k in api_keys],
                    "valid_satellites": _get_valid_satellites(),
                    "valid_feeds": sorted(_get_valid_feeds()),
                    "tile_url": tile_url,
                    "tile_attribution": tile_attribution,
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
    
    # Parse and validate limit
    limit_str = params.get("limit", "50")
    try:
        limit = int(limit_str)
    except ValueError:
        return JSONResponse(
            {"error": f"Invalid limit value: {limit_str}"},
            status_code=400,
        )
    
    if limit < 1 or limit > 200:
        return JSONResponse(
            {"error": "limit must be between 1 and 200"},
            status_code=400,
        )
    
    # Parse adapter filter
    adapter = params.get("adapter")
    
    # Parse category filter  
    category = params.get("category")
    
    # Parse since/until filters
    since = None
    until = None
    
    since_str = params.get("since")
    if since_str:
        try:
            since = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
        except ValueError:
            return JSONResponse(
                {"error": f"Invalid ISO 8601 datetime for since: {since_str}"},
                status_code=400,
            )
    
    until_str = params.get("until")
    if until_str:
        try:
            until = datetime.fromisoformat(until_str.replace("Z", "+00:00"))
        except ValueError:
            return JSONResponse(
                {"error": f"Invalid ISO 8601 datetime for until: {until_str}"},
                status_code=400,
            )
    
    # Validate since <= until
    if since and until and since > until:
        return JSONResponse(
            {"error": "since must be before or equal to until"},
            status_code=400,
        )
    
    # Parse region bbox
    region_north = params.get("region_north")
    region_south = params.get("region_south")
    region_east = params.get("region_east")
    region_west = params.get("region_west")
    
    region_params = [region_north, region_south, region_east, region_west]
    region_supplied = [p for p in region_params if p is not None]
    
    if len(region_supplied) > 0 and len(region_supplied) < 4:
        return JSONResponse(
            {"error": "Region filter requires all four parameters: region_north, region_south, region_east, region_west"},
            status_code=400,
        )
    
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
            return JSONResponse(
                {"error": "Region parameters must be valid numbers"},
                status_code=400,
            )
    
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
            return JSONResponse(
                {"error": "Invalid cursor"},
                status_code=400,
            )
    
    # Get database pool after validation
    pool = get_pool()
    
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
        logger.error(f"Database error in events_json: {e}")
        return JSONResponse(
            {"error": "Database error"},
            status_code=500,
        )
    
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
    
    return JSONResponse({
        "events": events,
        "next_cursor": next_cursor,
    })
