"""Registry-consistency tests for src/central/streams.py.

These are property tests, not literal restatements. Adding a new stream to the
registry requires no new test code -- every invariant here automatically
covers it.
"""

import re

from central.streams import STREAMS


def test_stream_names_unique():
    names = [s.name for s in STREAMS]
    assert len(names) == len(set(names)), "duplicate stream names in registry"


def test_subject_filters_unique():
    filters = [s.subject_filter for s in STREAMS]
    assert len(filters) == len(set(filters)), "duplicate subject filters in registry"


def test_subject_filter_central_prefix_wildcard():
    pattern = re.compile(r"^central\.[a-z][a-z_]*\.>$")
    for s in STREAMS:
        assert pattern.match(s.subject_filter), (
            f"{s.name}: subject_filter {s.subject_filter!r} does not match /^central\\.[a-z][a-z_]*\\.>$/"
        )


def test_meta_is_only_non_event_bearing():
    """CENTRAL_META is the only non-event-bearing stream today.

    If you're adding a second one, update this test deliberately -- the
    archive will silently skip the new stream, which is rarely what you want.
    """
    non_event = [s for s in STREAMS if not s.event_bearing]
    assert len(non_event) == 1, (
        f"expected exactly one non-event-bearing stream, got {[s.name for s in non_event]}"
    )
    assert non_event[0].name == "CENTRAL_META"


def test_supervisor_stream_subjects_includes_meta():
    """Supervisor creates every stream in JetStream, including META."""
    from central.supervisor import STREAM_SUBJECTS

    assert "CENTRAL_META" in STREAM_SUBJECTS
    assert STREAM_SUBJECTS["CENTRAL_META"] == ["central.meta.>"]


def test_supervisor_stream_subjects_includes_all():
    """Every registry stream appears in supervisor's derived dict with the right filter."""
    from central.supervisor import STREAM_SUBJECTS

    assert set(STREAM_SUBJECTS.keys()) == {s.name for s in STREAMS}
    for s in STREAMS:
        assert STREAM_SUBJECTS[s.name] == [s.subject_filter]


def test_archive_streams_excludes_non_event_bearing():
    """Archive's STREAMS list contains exactly the event_bearing=True entries."""
    from central.archive import STREAMS as ARCHIVE_STREAMS

    expected = [(s.name, s.subject_filter) for s in STREAMS if s.event_bearing]
    assert ARCHIVE_STREAMS == expected
    archive_names = {name for name, _ in ARCHIVE_STREAMS}
    for s in STREAMS:
        if s.event_bearing:
            assert s.name in archive_names
        else:
            assert s.name not in archive_names


def test_dashboard_streams_matches_dashboard_flag():
    """GUI's DASHBOARD_STREAMS matches [s.name for s in STREAMS if s.dashboard]."""
    from central.gui.routes import DASHBOARD_STREAMS

    expected = [s.name for s in STREAMS if s.dashboard]
    assert DASHBOARD_STREAMS == expected
