"""v0.14.2 drift guard: the archive consumer mirrors the per-adapter
``bypass_bbox_filter`` flag as a static module-level set
(``archive._BYPASS_BBOX_ADAPTERS``) because the consumer reads events off the
wire and has no access to the adapter class at runtime. The two truth sources
must never diverge -- if someone adds a global-by-design adapter and sets
``bypass_bbox_filter = True`` but forgets the archive set (or vice versa), this
test fails at CI time.
"""
from central.adapter_discovery import discover_adapters
from central.archive import _BYPASS_BBOX_ADAPTERS


def test_archive_static_set_matches_adapter_class_attr():
    bypass_from_classes = {
        name
        for name, cls in discover_adapters().items()
        if getattr(cls, "bypass_bbox_filter", False)
    }
    assert bypass_from_classes == _BYPASS_BBOX_ADAPTERS, (
        "archive._BYPASS_BBOX_ADAPTERS is out of sync with the adapter classes "
        "that set bypass_bbox_filter=True. Update the set in central.archive to "
        f"match: {sorted(bypass_from_classes)}"
    )
