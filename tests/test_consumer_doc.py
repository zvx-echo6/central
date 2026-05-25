"""Consistency tests for docs/CONSUMER-INTEGRATION.md.

The doc is the consumer contract. These tests catch drift between the doc and
the live code:

  - Every StreamEntry in src/central/streams.py must appear in the doc's
    "Stream layout" table — and vice versa.
  - Every adapter discovered via central.adapter_discovery.discover_adapters()
    must have a per-adapter subsection (### <name>) in the doc — and vice versa.

The doc's subject patterns and per-adapter copy are NOT directly asserted (they
are operator-readable prose, not machine-parsable); the registry-level checks
guard against the most common drift (adding a stream / adapter and forgetting
to document it).
"""

import re
from pathlib import Path

from central.adapter_discovery import discover_adapters
from central.streams import STREAMS

DOC_PATH = Path(__file__).resolve().parents[1] / "docs" / "CONSUMER-INTEGRATION.md"


def _doc_text() -> str:
    assert DOC_PATH.is_file(), f"missing: {DOC_PATH}"
    return DOC_PATH.read_text()


def _stream_layout_rows(doc: str) -> list[tuple[str, str]]:
    """Parse the doc's "Stream layout" table -> list of (stream_name, subject_filter)."""
    section_re = re.compile(
        r"^## 3\. Stream layout\s*\n(.*?)(?=^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '## 3. Stream layout' section"
    section = m.group(1)

    rows: list[tuple[str, str]] = []
    # Each row: | `CENTRAL_XX` | `central.xx.>` | ... |
    row_re = re.compile(r"^\|\s*`([A-Z_]+)`\s*\|\s*`(central\.[a-z_]+\.>)`\s*\|", re.MULTILINE)
    for name, subj in row_re.findall(section):
        rows.append((name, subj))
    return rows


def _per_adapter_subsections(doc: str) -> list[str]:
    """Pull adapter names from the per-adapter section headings: '### <adapter> — ...'.

    Only counts subsections inside '## 6. Per-adapter reference'.
    """
    section_re = re.compile(
        r"^## 6\. Per-adapter reference\s*\n(.*?)(?=^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '## 6. Per-adapter reference' section"
    section = m.group(1)

    heading_re = re.compile(r"^### ([a-z0-9_]+) — ", re.MULTILINE)
    return heading_re.findall(section)


def test_doc_exists():
    assert DOC_PATH.is_file(), f"doc missing: {DOC_PATH}"


def test_stream_table_matches_registry():
    """Every StreamEntry in streams.py must appear in the doc's stream layout table."""
    doc_rows = _stream_layout_rows(_doc_text())
    doc_names = {n for n, _ in doc_rows}
    doc_filters = {f for _, f in doc_rows}

    code_names = {s.name for s in STREAMS}
    code_filters = {s.subject_filter for s in STREAMS}

    assert doc_names == code_names, (
        f"stream-name drift: doc-only={doc_names - code_names}, "
        f"code-only={code_names - doc_names}"
    )
    assert doc_filters == code_filters, (
        f"subject-filter drift: doc-only={doc_filters - code_filters}, "
        f"code-only={code_filters - doc_filters}"
    )


def test_stream_table_name_subject_pairs_consistent():
    """Each (stream_name, subject_filter) pair in the doc must match the registry exactly.

    Catches the case where someone swaps the subject filter on one stream
    without updating its row.
    """
    doc_rows = set(_stream_layout_rows(_doc_text()))
    code_rows = {(s.name, s.subject_filter) for s in STREAMS}
    assert doc_rows == code_rows, (
        f"row drift: doc-only={doc_rows - code_rows}, code-only={code_rows - doc_rows}"
    )


def test_every_adapter_has_a_subsection():
    """Every adapter discovered in central.adapters must have a per-adapter doc subsection."""
    doc_adapters = set(_per_adapter_subsections(_doc_text()))
    code_adapters = set(discover_adapters().keys())
    assert doc_adapters == code_adapters, (
        f"adapter coverage drift: doc-only={doc_adapters - code_adapters}, "
        f"code-only={code_adapters - doc_adapters}"
    )


def test_subsections_appear_in_doc_order_matches_registry_size():
    """Sanity: the count of '### <adapter>' headings inside §6 equals the registry size.

    Independent count check; catches the case where one heading is duplicated.
    """
    doc_adapters = _per_adapter_subsections(_doc_text())
    assert len(doc_adapters) == len(set(doc_adapters)), (
        f"duplicate per-adapter sections: {[a for a in doc_adapters if doc_adapters.count(a) > 1]}"
    )
    assert len(doc_adapters) == len(discover_adapters())
