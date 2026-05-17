"""Route handlers for Central GUI."""

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
    AUTH_LOGIN,
    AUTH_LOGIN_FAILED,
    AUTH_LOGOUT,
    AUTH_PASSWORD_CHANGE,
    OPERATOR_CREATE,
    write_audit,
)
from central.gui.db import get_pool

router = APIRouter()


def _get_templates():
    """Get templates instance (deferred import to avoid circular)."""
    from central.gui import templates
    return templates


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
    return templates.TemplateResponse(
        request=request,
        name="index.html",
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
        context={"csrf_token": signed_token, "error": None},
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
            context={"csrf_token": signed_token, "error": error},
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
        context={"csrf_token": signed_token, "error": None},
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
                context={"csrf_token": signed_token, "error": "Invalid username or password"},
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
                context={"csrf_token": signed_token, "error": "Invalid username or password"},
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
        context={"csrf_token": signed_token, "error": None, "success": False},
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
                context={"csrf_token": signed_token, "error": error, "success": False},
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
