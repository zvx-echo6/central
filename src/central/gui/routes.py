"""Route handlers for Central GUI."""

import json
import re
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi_csrf_protect import CsrfProtect

from central.gui.auth import (
    create_session,
    delete_session,
    hash_password,
    validate_password,
    verify_password,
)
from central.gui.audit import (
    ADAPTER_UPDATE,
    AUTH_LOGIN,
    AUTH_LOGIN_FAILED,
    AUTH_LOGOUT,
    AUTH_PASSWORD_CHANGE,
    OPERATOR_CREATE,
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
async def index(request: Request, csrf_protect: CsrfProtect = Depends()) -> HTMLResponse:
    """Render the index page."""
    templates = _get_templates()
    operator = getattr(request.state, "operator", None)
    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"operator": operator, "csrf_token": csrf_token},
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
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
                # Get last message from CENTRAL_META for this adapter
                sub = await js.pull_subscribe(
                    f"central.meta.{name}.status",
                    durable=f"dashboard-poll-{name}",
                    stream="CENTRAL_META",
                )
                try:
                    msgs = await sub.fetch(1, timeout=1.0)
                    if msgs:
                        data = json.loads(msgs[0].data.decode())
                        last_poll = data.get("data", {}).get("time", "—")
                        adapters.append({
                            "name": name,
                            "last_poll": last_poll,
                            "status": "✓",
                            "error": None,
                        })
                    else:
                        adapters.append({
                            "name": name,
                            "last_poll": None,
                            "status": None,
                            "error": None,
                        })
                except Exception:
                    adapters.append({
                        "name": name,
                        "last_poll": None,
                        "status": None,
                        "error": None,
                    })
            except Exception:
                adapters.append({
                    "name": name,
                    "last_poll": None,
                    "status": None,
                    "error": "unavailable",
                })

    return templates.TemplateResponse(
        request=request,
        name="_dashboard_polls.html",
        context={"adapters": adapters, "error": error},
    )


@router.get("/setup", response_class=HTMLResponse)
async def setup_form(
    request: Request,
    csrf_protect: CsrfProtect = Depends(),
) -> HTMLResponse:
    """Render the setup form."""
    templates = _get_templates()
    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        request=request,
        name="setup.html",
        context={"csrf_token": csrf_token, "error": None},
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
    return response


@router.post("/setup")
async def setup_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
    csrf_protect: CsrfProtect = Depends(),
) -> Response:
    """Process the setup form."""
    templates = _get_templates()
    pool = get_pool()

    # Validate CSRF
    await csrf_protect.validate_csrf(request)

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
        csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
        response = templates.TemplateResponse(
            request=request,
            name="setup.html",
            context={"csrf_token": csrf_token, "error": error},
            status_code=200,
        )
        csrf_protect.set_csrf_cookie(signed_token, response)
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
        token, expires_at = await create_session(conn, operator_id, lifetime_days)

        # Mark setup complete
        await conn.execute(
            "UPDATE config.system SET setup_complete = true WHERE id = true"
        )

    # Redirect with session cookie
    response = RedirectResponse(url="/", status_code=302)
    _set_session_cookie(response, token, lifetime_days * 86400)
    return response


@router.get("/login", response_class=HTMLResponse)
async def login_form(
    request: Request,
    csrf_protect: CsrfProtect = Depends(),
) -> HTMLResponse:
    """Render the login form."""
    templates = _get_templates()
    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"csrf_token": csrf_token, "error": None},
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
    return response


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_protect: CsrfProtect = Depends(),
) -> Response:
    """Process the login form."""
    templates = _get_templates()
    pool = get_pool()

    # Validate CSRF
    await csrf_protect.validate_csrf(request)

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
            csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
            response = templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"csrf_token": csrf_token, "error": "Invalid username or password"},
                status_code=200,
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        # Verify password
        if not verify_password(password, row["password_hash"]):
            await write_audit(conn, AUTH_LOGIN_FAILED, operator_id=row["id"], target=username)
            csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
            response = templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"csrf_token": csrf_token, "error": "Invalid username or password"},
                status_code=200,
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
            return response

        # Get session lifetime
        sysrow = await conn.fetchrow(
            "SELECT session_lifetime_days FROM config.system WHERE id = true"
        )
        lifetime_days = sysrow["session_lifetime_days"] if sysrow else 90

        # Create session
        token, expires_at = await create_session(conn, row["id"], lifetime_days)

        # Audit login
        await write_audit(conn, AUTH_LOGIN, operator_id=row["id"], target=username)

    # Redirect with session cookie
    response = RedirectResponse(url="/", status_code=302)
    _set_session_cookie(response, token, lifetime_days * 86400)
    return response


@router.post("/logout")
async def logout(
    request: Request,
    csrf_protect: CsrfProtect = Depends(),
) -> Response:
    """Log out the current user."""
    pool = get_pool()

    # Validate CSRF
    await csrf_protect.validate_csrf(request)

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
    csrf_protect: CsrfProtect = Depends(),
) -> HTMLResponse:
    """Render the change password form."""
    templates = _get_templates()
    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        request=request,
        name="change_password.html",
        context={"csrf_token": csrf_token, "error": None, "success": False},
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
    return response


@router.post("/change-password")
async def change_password_submit(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    csrf_protect: CsrfProtect = Depends(),
) -> Response:
    """Process the change password form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Validate CSRF
    await csrf_protect.validate_csrf(request)

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
            csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
            response = templates.TemplateResponse(
                request=request,
                name="change_password.html",
                context={"csrf_token": csrf_token, "error": error, "success": False},
                status_code=200,
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
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
    csrf_protect: CsrfProtect = Depends(),
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
        # asyncpg auto-deserializes jsonb to dict
        settings = row["settings"] if row["settings"] else {}
        if isinstance(settings, str):
            settings = json.loads(settings)
        adapters.append({
            "name": row["name"],
            "enabled": row["enabled"],
            "cadence_s": row["cadence_s"],
            "settings": settings,
            "paused_at": row["paused_at"],
            "updated_at": row["updated_at"],
        })

    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
    response = templates.TemplateResponse(
        request=request,
        name="adapters_list.html",
        context={
            "operator": operator,
            "csrf_token": csrf_token,
            "adapters": adapters,
        },
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
    return response


@router.get("/adapters/{name}", response_class=HTMLResponse)
async def adapters_edit_form(
    request: Request,
    name: str,
    csrf_protect: CsrfProtect = Depends(),
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

    # asyncpg auto-deserializes jsonb to dict
    settings = row["settings"] if row["settings"] else {}
    if isinstance(settings, str):
        settings = json.loads(settings)
    adapter = {
        "name": row["name"],
        "enabled": row["enabled"],
        "cadence_s": row["cadence_s"],
        "settings": settings,
        "paused_at": row["paused_at"],
        "updated_at": row["updated_at"],
    }

    csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
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
        },
    )
    csrf_protect.set_csrf_cookie(signed_token, response)
    return response


@router.post("/adapters/{name}")
async def adapters_edit_submit(
    request: Request,
    name: str,
    csrf_protect: CsrfProtect = Depends(),
) -> Response:
    """Process the adapter edit form."""
    templates = _get_templates()
    pool = get_pool()
    operator = request.state.operator

    # Validate CSRF
    await csrf_protect.validate_csrf(request)

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

        # asyncpg auto-deserializes jsonb to dict
        current_settings = row["settings"] if row["settings"] else {}
        if isinstance(current_settings, str):
            current_settings = json.loads(current_settings)
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

            csrf_token, signed_token = csrf_protect.generate_csrf_tokens()
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
                },
                status_code=200,
            )
            csrf_protect.set_csrf_cookie(signed_token, response)
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
            json.dumps(new_settings),
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
