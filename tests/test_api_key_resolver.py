"""Tests for central.gui._api_key_resolver.adapter_has_resolved_api_key.

The /adapters list, /adapters/{name} edit, and POST error re-render flows
previously inlined a predicate that compared the SourceAdapter class
attribute `requires_api_key` (default literal string) against
`config.api_keys` — ignoring the per-row `settings[api_key_field]` alias
the operator actually selected. The helper consolidates the resolution
logic; these tests pin it.

No hardcoded adapter-name list literals: keyless-adapter case picks a
real one via central.adapter_discovery. The settings-driven cases use
inline minimal SourceAdapter subclasses because no live adapter today
has the pertinent class-attr/field combinations.
"""

from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from central.adapter import SourceAdapter
from central.adapter_discovery import discover_adapters
from central.api_key_resolver import (
    adapter_has_resolved_api_key,
    resolve_api_key_alias,
)
from central.models import Event


class _FakeConn:
    """Captures the (sql, alias) of each fetchval call and returns a scripted
    result. Mirrors the asyncpg connection surface adapter_has_resolved_api_key
    actually uses."""

    def __init__(self, result: Any) -> None:
        self.result = result
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchval(self, sql: str, *params: Any) -> Any:
        self.calls.append((sql, params))
        return self.result


def _pick_keyless_adapter() -> type[SourceAdapter] | None:
    """First discovered adapter with requires_api_key is None.

    Lets test 1 exercise the no-SQL short-circuit against a real subclass
    without hardcoding the adapter's name.
    """
    for cls in discover_adapters().values():
        if cls.requires_api_key is None:
            return cls
    return None


class _BareKeyAdapter(SourceAdapter):
    """Requires a key but does NOT expose an operator-overridable field.

    Forces the helper to fall back to the class-attribute default.
    """

    name = "_bare_key"
    display_name = "_BareKey"
    description = "Test fixture: class-attr fallback path."
    settings_schema = BaseModel
    requires_api_key = "default"
    api_key_field = None
    default_cadence_s = 60

    async def poll(self) -> AsyncIterator[Event]:  # pragma: no cover
        if False:
            yield  # type: ignore[unreachable]
        return

    async def apply_config(self, new_config) -> None:  # pragma: no cover
        pass

    def subject_for(self, event: Event) -> str:  # pragma: no cover
        return "central.test._bare_key"


class _OperatorOverridableAdapter(SourceAdapter):
    """Requires a key AND exposes an operator-overridable alias field.

    Models the FIRMS-shaped contract that previously misfired.
    """

    name = "_overridable"
    display_name = "_Overridable"
    description = "Test fixture: operator-overridable alias field."
    settings_schema = BaseModel
    requires_api_key = "default"
    api_key_field = "api_key_alias"
    default_cadence_s = 60

    async def poll(self) -> AsyncIterator[Event]:  # pragma: no cover
        if False:
            yield  # type: ignore[unreachable]
        return

    async def apply_config(self, new_config) -> None:  # pragma: no cover
        pass

    def subject_for(self, event: Event) -> str:  # pragma: no cover
        return "central.test._overridable"


@pytest.mark.asyncio
async def test_keyless_adapter_short_circuits_without_sql():
    """An adapter with requires_api_key=None returns (True, None) and the
    helper issues NO SQL — caller can assume the gate is open."""
    keyless = _pick_keyless_adapter()
    if keyless is None:
        pytest.skip("no keyless adapter discovered in central.adapters")
    conn = _FakeConn(result=1)
    has_key, alias = await adapter_has_resolved_api_key(conn, keyless, {})
    assert (has_key, alias) == (True, None)
    assert conn.calls == [], "no SQL expected for keyless adapter"


@pytest.mark.asyncio
async def test_class_attr_fallback_when_no_api_key_field():
    """Adapter exposes requires_api_key but no api_key_field — helper falls
    back to the class-attribute default for the lookup alias."""
    conn = _FakeConn(result=1)
    has_key, alias = await adapter_has_resolved_api_key(conn, _BareKeyAdapter, {})
    assert alias == "default"
    assert has_key is True
    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert "config.api_keys" in sql
    assert params == ("default",)


@pytest.mark.asyncio
async def test_settings_override_takes_precedence_over_class_attr():
    """Operator-selected alias in settings[api_key_field] wins over the
    class-attribute default. Regression guard for the FIRMS-shaped bug:
    requires_api_key='default' but operator stored a key under 'custom'."""
    conn = _FakeConn(result=1)
    settings = {"api_key_alias": "custom"}
    has_key, alias = await adapter_has_resolved_api_key(
        conn, _OperatorOverridableAdapter, settings,
    )
    assert alias == "custom", "operator-selected alias should win"
    assert has_key is True
    assert conn.calls[0][1] == ("custom",), (
        f"SQL must query the operator-selected alias, got {conn.calls[0][1]!r}"
    )


@pytest.mark.asyncio
async def test_missing_settings_field_falls_back_to_class_attr():
    """Operator hasn't visited the form yet — settings dict lacks the
    api_key_field entry. Class-attr default is the safe fallback."""
    conn = _FakeConn(result=1)
    has_key, alias = await adapter_has_resolved_api_key(
        conn, _OperatorOverridableAdapter, {"unrelated": "noise"},
    )
    assert alias == "default"
    assert conn.calls[0][1] == ("default",)

    # Same path when settings is None entirely.
    conn2 = _FakeConn(result=1)
    has_key2, alias2 = await adapter_has_resolved_api_key(
        conn2, _OperatorOverridableAdapter, None,
    )
    assert alias2 == "default"
    assert conn2.calls[0][1] == ("default",)


@pytest.mark.asyncio
async def test_empty_string_settings_field_falls_back_to_class_attr():
    """Empty string in settings does NOT mean "look up alias '' "; it means
    "not set, use default" — same shape the form parser yields for cleared
    text inputs."""
    conn = _FakeConn(result=1)
    has_key, alias = await adapter_has_resolved_api_key(
        conn, _OperatorOverridableAdapter, {"api_key_alias": "   "},
    )
    assert alias == "default", "whitespace-only must fall through to class attr"
    assert conn.calls[0][1] == ("default",)

    conn2 = _FakeConn(result=1)
    has_key2, alias2 = await adapter_has_resolved_api_key(
        conn2, _OperatorOverridableAdapter, {"api_key_alias": ""},
    )
    assert alias2 == "default"
    assert conn2.calls[0][1] == ("default",)


@pytest.mark.asyncio
async def test_resolved_alias_absent_returns_has_key_false():
    """If the resolved alias has no matching row, helper returns
    (False, alias) so caller renders the warning chip with the right alias
    shown in the title attribute."""
    conn = _FakeConn(result=None)
    has_key, alias = await adapter_has_resolved_api_key(
        conn, _OperatorOverridableAdapter, {"api_key_alias": "ghost"},
    )
    assert (has_key, alias) == (False, "ghost")


@pytest.mark.asyncio
async def test_unknown_adapter_cls_short_circuits():
    """Row references a deleted adapter — adapter_cls is None. Helper must
    not crash, must not issue SQL, and must report no missing key (the warning
    is for known adapters; orphaned rows are a separate concern)."""
    conn = _FakeConn(result=None)
    has_key, alias = await adapter_has_resolved_api_key(conn, None, {})
    assert (has_key, alias) == (True, None)
    assert conn.calls == []


# ---------------------------------------------------------------------------
# Supervisor uses the sync resolver directly (no DB round-trip — supervisor
# has its own ConfigStore and calls get_api_key on the resolved alias).
# These tests mirror the route coverage on the sync entry point.
# ---------------------------------------------------------------------------


def test_supervisor_uses_operator_selected_alias():
    """Supervisor's adapter-start precondition must resolve to the same alias
    the GUI's warning predicate resolves to. Regression guard for the second
    half of the FIRMS-shaped bug: routes opened the gate but supervisor still
    refused to start with `missing api key: <class_attr>`."""
    alias = resolve_api_key_alias(
        _OperatorOverridableAdapter, {"api_key_alias": "firms_production"},
    )
    assert alias == "firms_production"


def test_supervisor_falls_back_to_class_attr_when_settings_unset():
    """Operator hasn't touched settings yet — supervisor still finds the
    class-attribute default (preserves pre-bug behavior for fresh installs)."""
    assert resolve_api_key_alias(_OperatorOverridableAdapter, None) == "default"
    assert resolve_api_key_alias(_OperatorOverridableAdapter, {}) == "default"
    assert resolve_api_key_alias(_BareKeyAdapter, {}) == "default"


def test_supervisor_returns_none_for_keyless_adapter():
    """When the adapter doesn't need a key, supervisor should skip the
    `get_api_key` call entirely — the sync resolver returns None to signal."""
    keyless = _pick_keyless_adapter()
    if keyless is None:
        pytest.skip("no keyless adapter discovered in central.adapters")
    assert resolve_api_key_alias(keyless, {}) is None
    assert resolve_api_key_alias(None, {}) is None
