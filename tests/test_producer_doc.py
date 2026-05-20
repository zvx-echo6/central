"""Consistency tests for docs/PRODUCER-INTEGRATION.md.

The doc is the producer-side contract — what an adapter author implements and
the conventions Central enforces around it. These tests catch drift between
the doc and the live code:

  - Every overridable SourceAdapter method documented in §4 must exist on
    central.adapter.SourceAdapter — and vice versa.
  - The preview_for_settings contract quoted in §11.1 must come from the
    actual SourceAdapter.preview_for_settings docstring.
  - The set of top-level domain tokens documented in §6.1 must equal the set
    derived from central.streams.STREAMS subject_filter prefixes.
  - The verbatim STREAMS snippet quoted in §8 must match the live registry.

Per the doc's own §10.4, NO hardcoded stream / adapter list literals: every
expected value derives from central.streams, central.adapter, or
central.adapter_discovery at runtime.
"""

import inspect
import re
from pathlib import Path

from central.adapter import SourceAdapter
from central.adapter_discovery import discover_adapters
from central.enrichment.geocoder import GEOCODER_FIELDS
from central.streams import STREAMS

# The verbatim design-principle quote that must stay in §2 (Matt, 2026-05-19).
_DESIGN_PRINCIPLE_QUOTE = (
    "Central takes it all and gives it all. It's up to the pipe to do with it"
)

DOC_PATH = Path(__file__).resolve().parents[1] / "docs" / "PRODUCER-INTEGRATION.md"


def _doc_text() -> str:
    assert DOC_PATH.is_file(), f"missing: {DOC_PATH}"
    return DOC_PATH.read_text()


def _documented_override_methods(doc: str) -> set[str]:
    """Extract method names documented under '## 4. The SourceAdapter base class'.

    Looks for the '**`async def <name>(...)`**' / '**`def <name>(...)`**'
    method headings inside §4.
    """
    section_re = re.compile(
        r"^## 4\. The SourceAdapter base class\s*\n(.*?)(?=^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '## 4. The SourceAdapter base class' section"
    section = m.group(1)
    heading_re = re.compile(r"\*\*`(?:async\s+)?def\s+(\w+)\s*\(", re.MULTILINE)
    return set(heading_re.findall(section))


def _sourceadapter_overridable_methods() -> set[str]:
    """Methods on SourceAdapter that an adapter author is expected to implement
    or may override. Excludes Python internals (dunder), the constructor, and
    private helpers.
    """
    methods: set[str] = set()
    for name, member in inspect.getmembers(SourceAdapter):
        if name.startswith("_"):
            continue
        if not (inspect.isfunction(member) or inspect.iscoroutinefunction(member)):
            continue
        methods.add(name)
    return methods


def _streams_domains() -> set[str]:
    """Top-level <domain> tokens derived from STREAMS subject filters
    (central.<domain>.>).
    """
    domain_re = re.compile(r"^central\.([a-z_]+)\.>$")
    out: set[str] = set()
    for s in STREAMS:
        m = domain_re.match(s.subject_filter)
        assert m, f"unexpected subject filter shape: {s.subject_filter!r}"
        out.add(m.group(1))
    return out


def _documented_domains(doc: str) -> set[str]:
    """Domain tokens enumerated in §6.1 as backtick literals (`wx`, `fire`, …)."""
    section_re = re.compile(
        r"`<domain>` is one of ([^.]+)\.",
        re.DOTALL,
    )
    m = section_re.search(doc)
    assert m, "doc missing the '`<domain>` is one of ...' enumeration in §6.1"
    enum_text = m.group(1)
    return set(re.findall(r"`([a-z_]+)`", enum_text))


def test_doc_exists():
    assert DOC_PATH.is_file(), f"doc missing: {DOC_PATH}"


def test_documented_methods_match_sourceadapter_api():
    """Every override-able SourceAdapter method must appear in the §4 contract,
    and the doc may not advertise methods that don't exist."""
    doc_methods = _documented_override_methods(_doc_text())
    code_methods = _sourceadapter_overridable_methods()
    assert doc_methods == code_methods, (
        f"override-method drift: "
        f"doc-only={doc_methods - code_methods}, "
        f"code-only={code_methods - doc_methods}"
    )


def test_preview_hook_contract_matches_docstring():
    """The contract block quoted in §11.1 must come from the live
    SourceAdapter.preview_for_settings docstring.

    Normalizes both sides by collapsing whitespace and stripping the doc's
    Markdown blockquote prefix (`> `).
    """
    doc = _doc_text()
    section_re = re.compile(
        r"^### 11\.1[^\n]*\n(.*?)(?=^### |^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '### 11.1' subsection"
    blockquote = "\n".join(
        line[2:] if line.startswith("> ") else line.lstrip(">").lstrip()
        for line in m.group(1).splitlines()
        if line.lstrip().startswith(">")
    )
    docstring = inspect.getdoc(SourceAdapter.preview_for_settings) or ""

    def norm(s: str) -> str:
        # Strip markdown backticks; collapse whitespace.
        return re.sub(r"\s+", " ", s.replace("`", "")).strip()

    norm_block = norm(blockquote)
    norm_doc = norm(docstring)
    # Bidirectional: every non-empty sentence of the docstring must appear in
    # the doc's blockquote, and the blockquote must not introduce new sentences
    # the docstring lacks.
    sentences = lambda s: {x.strip() for x in re.split(r"(?<=[.:])\s+", s) if x.strip()}
    doc_sents = sentences(norm_block)
    code_sents = sentences(norm_doc)
    assert doc_sents == code_sents, (
        f"preview_for_settings contract drift: "
        f"doc-only={doc_sents - code_sents}, "
        f"code-only={code_sents - doc_sents}"
    )


def test_top_level_domains_match_streams_registry():
    """The §6.1 domain enumeration must equal the domain tokens derived from
    central.streams.STREAMS — bidirectional, no hardcoded list."""
    doc_domains = _documented_domains(_doc_text())
    code_domains = _streams_domains()
    assert doc_domains == code_domains, (
        f"domain-token drift: "
        f"doc-only={doc_domains - code_domains}, "
        f"code-only={code_domains - doc_domains}"
    )


def test_streams_snippet_quotes_live_registry():
    """The §8 verbatim STREAMS snippet must agree with central.streams.STREAMS
    on (name, subject_filter, event_bearing).
    """
    doc = _doc_text()
    section_re = re.compile(
        r"^## 8\. The StreamEntry registry\s*\n(.*?)(?=^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '## 8. The StreamEntry registry' section"
    section = m.group(1)
    # Each documented entry: StreamEntry("NAME", "central.x.>"[, event_bearing=False])
    entry_re = re.compile(
        r'StreamEntry\(\s*"([A-Z_]+)"\s*,\s*"(central\.[a-z_]+\.>)"'
        r'(?:\s*,\s*event_bearing\s*=\s*(False|True))?\s*\)',
    )
    doc_rows: set[tuple[str, str, bool]] = set()
    for name, subj, eb in entry_re.findall(section):
        event_bearing = (eb != "False")  # default True if unspecified
        doc_rows.add((name, subj, event_bearing))
    code_rows = {(s.name, s.subject_filter, s.event_bearing) for s in STREAMS}
    assert doc_rows == code_rows, (
        f"STREAMS snippet drift: "
        f"doc-only={doc_rows - code_rows}, code-only={code_rows - doc_rows}"
    )


def _section(doc: str, header_re: str) -> str:
    """Return the body of the section whose header matches header_re, up to the
    next same-or-higher-level header."""
    m = re.search(header_re + r"\s*\n(.*?)(?=^## |\Z)", doc, re.DOTALL | re.MULTILINE)
    assert m, f"doc missing section matching {header_re!r}"
    return m.group(1)


def test_design_principle_quote_present_in_section_2():
    """§2 must still carry the verbatim Matt quote — the reframe changes the
    translation beneath it, not the quote itself."""
    section = _section(_doc_text(), r"^## 2\. The design principle")
    assert _DESIGN_PRINCIPLE_QUOTE in section, "verbatim design-principle quote missing from §2"


def test_anti_pattern_10_1_section_exists():
    """§10.1 must still exist as a subsection (content reframed to
    'enrichment outside the framework', structure preserved)."""
    doc = _doc_text()
    assert re.search(r"^### 10\.1 ", doc, re.MULTILINE), "doc missing '### 10.1' subsection"


def test_enrichment_contract_section_13_has_all_protocol_references():
    """New §13 must name all four enrichment contract types verbatim."""
    section = _section(_doc_text(), r"^## 13\. Enrichment contract")
    for ref in ("Enricher", "GeocoderEnricher", "GeocoderBackend", "NoOpBackend"):
        assert ref in section, f"§13 missing reference to {ref!r}"


def test_enrichment_coverage_matrix_lists_exactly_geocoder_fields():
    """The §13 per-field coverage matrix must list exactly the canonical
    GEOCODER_FIELDS — derived from code, not hardcoded here."""
    section = _section(_doc_text(), r"^## 13\. Enrichment contract")
    # Matrix rows look like: | `field_name` | ... |
    row_fields = set(re.findall(r"^\|\s*`([a-z_]+)`\s*\|", section, re.MULTILINE))
    assert row_fields == set(GEOCODER_FIELDS), (
        f"coverage-matrix field drift: "
        f"doc-only={row_fields - set(GEOCODER_FIELDS)}, "
        f"code-only={set(GEOCODER_FIELDS) - row_fields}"
    )


def test_no_orphan_adapter_references_in_anti_patterns():
    """Anti-patterns section names two real adapter modules as examples
    (firms, inciweb in §10.4). Those names must still resolve via
    central.adapter_discovery — protects against a silent rename leaving
    dead example references in the doc.
    """
    doc = _doc_text()
    section_re = re.compile(
        r"^## 10\. Anti-patterns.*?\n(.*?)(?=^## )",
        re.DOTALL | re.MULTILINE,
    )
    m = section_re.search(doc)
    assert m, "doc missing '## 10. Anti-patterns' section"
    section = m.group(1)
    quoted = set(re.findall(r'"([a-z][a-z_]*)"', section))
    # Whitelist Python-syntax tokens that incidentally appear in the section;
    # everything else in this set is asserted to be a real adapter name.
    # Derived from STREAMS per §10.4 — stream names appear quoted as examples
    # and would otherwise look like orphan adapter references.
    syntax_tokens = {s.name for s in STREAMS}
    candidate_adapter_names = quoted - {t.lower() for t in syntax_tokens}
    known_adapters = set(discover_adapters().keys())
    orphans = {n for n in candidate_adapter_names if n not in known_adapters}
    assert not orphans, (
        f"anti-patterns section references unknown adapter names: {orphans} "
        f"(known adapters: {sorted(known_adapters)})"
    )
