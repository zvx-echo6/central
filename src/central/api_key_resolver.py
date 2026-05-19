"""Resolve an adapter row's effective api-key alias and check existence.

Four call sites share this answer: the /adapters list view, the
/adapters/{name} edit form (GET), the same edit form's POST error
re-render path, and the supervisor's adapter-start precondition.
Previously each inlined a predicate that compared the hardcoded
SourceAdapter class attribute `requires_api_key` against `config.api_keys`,
ignoring the per-row `settings[api_key_field]` value the operator
actually selected. An operator could pick alias "firms_production" via
the form, store a working key under that alias, and still see the
"⚠️ API Key Missing" chip + a disabled enable checkbox + the supervisor
refusing to start the adapter — all from the same root cause.

Two entry points:

- `resolve_api_key_alias`  — sync. Pure function of (cls, settings).
  Returns the alias the row should consult, or None when no key is
  required (no SQL needed). Supervisor uses this directly because it
  already has a ConfigStore and does its own key fetch.

- `adapter_has_resolved_api_key` — async. Wraps the sync resolver with
  the existence SELECT against config.api_keys. The three GUI route
  call sites use this.

Resolution order (same as the adapter's runtime read in __init__):
  1. requires_api_key is None  -> no key needed.
  2. settings[api_key_field] is a non-empty string -> use that.
  3. fall back to the requires_api_key class-attribute default.
"""

from typing import Any

from central.adapter import SourceAdapter


def resolve_api_key_alias(
    adapter_cls: type[SourceAdapter] | None,
    settings: dict | None,
) -> str | None:
    """Return the api-key alias for this adapter row, or None when no key is required.

    Pure function — no IO, no SQL. Same resolution order the adapter's own
    __init__ uses when reading the alias out of config.settings.
    """
    if adapter_cls is None or adapter_cls.requires_api_key is None:
        return None
    field = adapter_cls.api_key_field
    if field and settings:
        value = settings.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return adapter_cls.requires_api_key


async def adapter_has_resolved_api_key(
    conn: Any,
    adapter_cls: type[SourceAdapter] | None,
    settings: dict | None,
) -> tuple[bool, str | None]:
    """Return (has_key, resolved_alias) for one adapter row.

    Args:
        conn: asyncpg connection or any object exposing an awaitable fetchval
            with the same signature.
        adapter_cls: The discovered SourceAdapter subclass for this row, or
            None when the row references an adapter no longer in the codebase.
        settings: The row's jsonb settings dict, or None when the row has no
            settings stored yet.

    Returns:
        (has_key, resolved_alias). When the adapter does not require an api
        key (requires_api_key is None) or no adapter class is available,
        returns (True, None) and no SQL is issued.
    """
    alias = resolve_api_key_alias(adapter_cls, settings)
    if alias is None:
        return True, None
    has_key = await conn.fetchval(
        "SELECT 1 FROM config.api_keys WHERE alias = $1",
        alias,
    )
    return bool(has_key), alias
