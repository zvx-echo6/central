"""Tests for the SourceAdapter.preview_for_settings hook + framework rendering."""

from collections.abc import AsyncIterator
from pathlib import Path

import jinja2
import pytest
from pydantic import BaseModel

from central.adapter import SourceAdapter
from central.models import Event

TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "src" / "central" / "gui" / "templates"


class _StubSettings(BaseModel):
    pass


class _StubAdapter(SourceAdapter):
    """Minimal SourceAdapter subclass that does NOT override preview_for_settings."""

    name = "stub"
    display_name = "Stub Adapter"
    description = "Test fixture"
    settings_schema = _StubSettings
    default_cadence_s = 60

    def __init__(self) -> None:
        pass

    async def poll(self) -> AsyncIterator[Event]:  # pragma: no cover - never invoked
        if False:
            yield  # type: ignore[unreachable]
        return

    async def apply_config(self, new_config) -> None:  # pragma: no cover
        pass

    def subject_for(self, event: Event) -> str:  # pragma: no cover
        return "stub"


@pytest.mark.asyncio
async def test_default_returns_none():
    """The base SourceAdapter's preview_for_settings default returns None.

    Any adapter that does not override the hook gets a no-op preview, so the
    framework renders the page without a preview block (no crash, no opt-out
    boilerplate required in each adapter).
    """
    adapter = _StubAdapter()
    result = await adapter.preview_for_settings(_StubSettings())
    assert result is None


def _render_partial(**context) -> str:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=jinja2.select_autoescape(["html"]),
    )
    tmpl = env.get_template("_adapter_preview.html")
    return tmpl.render(**context)


def test_partial_renders_list():
    """Given list[dict] with insertion-ordered keys, partial renders a table.

    Framework is duck-typed: columns come from the first dict's keys. No
    adapter-name branches, no per-adapter Jinja.
    """
    rows = [
        {"a": "x1", "b": "y1"},
        {"a": "x2", "b": "y2"},
    ]
    out = _render_partial(preview_rows=rows, preview_error=None)
    # Header row uses first dict's keys in order.
    assert "<th>a</th>" in out
    assert "<th>b</th>" in out
    # Body rows carry every value.
    for cell in ("x1", "y1", "x2", "y2"):
        assert cell in out
    # Row count surfaces in the legend.
    assert "Preview (2 rows)" in out
    # No error banner when preview_error is None.
    assert "Preview Unavailable" not in out


def test_partial_renders_error():
    """When preview_error is set, partial renders the error banner and no table."""
    out = _render_partial(preview_rows=None, preview_error="Preview unavailable: boom")
    assert "Preview unavailable: boom" in out
    # No table when an error is set.
    assert "<table" not in out
    assert "<th>" not in out


def test_partial_renders_nothing_when_both_none():
    """No preview_rows and no preview_error -> partial renders no preview section.

    Lets adapters that don't opt in (e.g. EONET, GDACS) render normally without
    any preview-related markup on the page.
    """
    out = _render_partial(preview_rows=None, preview_error=None).strip()
    assert "<table" not in out
    assert "Preview Unavailable" not in out
    # Either empty or only whitespace/newlines from the template.
    assert "Preview (" not in out
