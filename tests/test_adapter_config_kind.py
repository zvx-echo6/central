"""Tests for AdapterConfig.kind and supervisor._create_adapter kind-based dispatch.

All tests are pure-unit (no DB, no NATS) so they run anywhere.
"""

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Provide required env vars before importing central modules.
os.environ.setdefault("CENTRAL_DB_DSN", "postgresql://test:test@localhost/test")
os.environ.setdefault("CENTRAL_CSRF_SECRET", "testsecret12345678901234567890ab")
os.environ.setdefault("CENTRAL_NATS_URL", "nats://localhost:4222")

from central.config_models import AdapterConfig


_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _cfg(**kwargs) -> AdapterConfig:
    """Build a minimal AdapterConfig with sensible defaults."""
    defaults = {
        "name": "usgs_quake",
        "cadence_s": 120,
        "updated_at": _NOW,
    }
    defaults.update(kwargs)
    return AdapterConfig(**defaults)


# ---------------------------------------------------------------------------
# AdapterConfig.kind back-compat
# ---------------------------------------------------------------------------


class TestAdapterConfigKind:
    def test_kind_defaults_to_name_when_absent(self):
        """When kind is not supplied, it falls back to name (pre-043 rows)."""
        cfg = _cfg(name="usgs_quake")
        assert cfg.kind == "usgs_quake"

    def test_kind_defaults_to_name_when_none(self):
        """When kind is explicitly None (NULL from DB), it falls back to name."""
        cfg = _cfg(name="nws", kind=None)
        assert cfg.kind == "nws"

    def test_kind_preserved_when_set(self):
        """When kind is explicitly supplied, it is kept as-is."""
        cfg = _cfg(name="my_quake_instance", kind="usgs_quake")
        assert cfg.kind == "usgs_quake"
        assert cfg.name == "my_quake_instance"

    def test_name_unchanged_when_kind_differs(self):
        """Instance identity (name) is never overwritten by the kind back-fill."""
        cfg = _cfg(name="my_custom_quake", kind="usgs_quake")
        assert cfg.name == "my_custom_quake"

    def test_kind_equals_name_for_builtin_pattern(self):
        """Built-in adapters: name == kind both before and after the migration."""
        cfg = _cfg(name="nws", kind="nws")
        assert cfg.kind == cfg.name == "nws"


# ---------------------------------------------------------------------------
# supervisor._create_adapter resolves class by kind
# ---------------------------------------------------------------------------


class TestCreateAdapterResolvesbyKind:
    """Unit-test _create_adapter without a live Supervisor (no NATS, no DB)."""

    def _make_supervisor_with_registry(self, registry: dict):
        """Return a minimal Supervisor-like object with _adapters = registry."""
        # Import lazily to avoid triggering the ENRICHMENT_CACHE_DB_PATH side
        # effect before conftest patches it (conftest autouse fixture handles it
        # in the full test run; here we patch manually).
        from unittest.mock import patch
        import central.supervisor as sup_mod

        with tempfile.TemporaryDirectory() as td:
            with patch.object(sup_mod, "ENRICHMENT_CACHE_DB_PATH", Path(td) / "ec.db"):
                with patch.object(sup_mod, "CURSOR_DB_PATH", Path(td) / "cursor.db"):
                    supervisor = MagicMock()
                    supervisor._adapters = registry
                    supervisor._config_store = MagicMock()
                    # Bind the real _create_adapter method to our mock object.
                    supervisor._create_adapter = (
                        sup_mod.Supervisor._create_adapter.__get__(supervisor)
                    )
                    return supervisor

    def _make_mock_adapter_cls(self, class_name: str):
        """Return a mock adapter class whose constructor returns a mock instance."""
        instance = MagicMock()
        instance.name = class_name  # class-level .name attribute

        cls = MagicMock()
        cls.return_value = instance
        return cls

    def test_resolves_class_by_kind_when_kind_differs_from_name(self):
        """_create_adapter uses config.kind (class key), not config.name."""
        usgs_cls = self._make_mock_adapter_cls("usgs_quake")
        registry = {"usgs_quake": usgs_cls}
        sup = self._make_supervisor_with_registry(registry)

        cfg = _cfg(name="my_quake_instance", kind="usgs_quake")
        result = sup._create_adapter(cfg)

        # Class was looked up by kind and instantiated.
        usgs_cls.assert_called_once()
        assert result is usgs_cls.return_value

    def test_builtin_pattern_kind_equals_name_still_works(self):
        """Regression: existing adapters with name == kind construct correctly."""
        usgs_cls = self._make_mock_adapter_cls("usgs_quake")
        registry = {"usgs_quake": usgs_cls}
        sup = self._make_supervisor_with_registry(registry)

        cfg = _cfg(name="usgs_quake", kind="usgs_quake")
        result = sup._create_adapter(cfg)

        usgs_cls.assert_called_once()
        assert result is usgs_cls.return_value

    def test_raises_on_unknown_kind(self):
        """_create_adapter raises ValueError mentioning both kind and instance name."""
        registry = {}
        sup = self._make_supervisor_with_registry(registry)

        cfg = _cfg(name="my_instance", kind="nonexistent_adapter")
        with pytest.raises(ValueError) as exc_info:
            sup._create_adapter(cfg)

        msg = str(exc_info.value)
        assert "nonexistent_adapter" in msg
        assert "my_instance" in msg

    def test_instance_identity_passed_through_to_constructor(self):
        """The adapter constructor receives the full config (name = instance id)."""
        some_cls = self._make_mock_adapter_cls("some_adapter")
        registry = {"some_adapter": some_cls}
        sup = self._make_supervisor_with_registry(registry)

        cfg = _cfg(name="prod_instance_1", kind="some_adapter")
        sup._create_adapter(cfg)

        # Check the config passed to constructor has the instance name.
        call_kwargs = some_cls.call_args
        passed_config = call_kwargs[1].get("config") or call_kwargs[0][0]
        assert passed_config.name == "prod_instance_1"


class TestStartAdapterResolvesByKind:
    """The api-key precondition in _start_adapter also resolves the class by kind.

    _start_adapter is callable directly with mocks (no live DB), mirroring
    tests/test_requires_api_key.py::TestSupervisorApiKeyPrecondition. A generic
    instance where name != kind must still find its class via the registry's
    kind key — otherwise the lookup returns None, resolve_api_key_alias treats
    it as "no key required", and the adapter silently skips its api-key check
    and starts. Resolving by kind keeps the precondition enforced.
    """

    @pytest.mark.asyncio
    async def test_precondition_refuses_when_key_missing_for_instance_named_differently(
        self, tmp_path: Path
    ):
        from central.supervisor import Supervisor
        from central.adapters.firms import FIRMSAdapter

        mock_config_store = MagicMock()
        mock_config_store.get_api_key = AsyncMock(return_value=None)  # key missing
        mock_config_store.set_adapter_last_error = AsyncMock()

        mock_nats = MagicMock()
        mock_nats.publish = AsyncMock()

        supervisor = Supervisor.__new__(Supervisor)
        supervisor._config_store = mock_config_store
        # Registry is keyed by class identity (kind == FIRMSAdapter.name == "firms").
        supervisor._adapters = {"firms": FIRMSAdapter}
        supervisor._adapter_states = {}
        supervisor._nats = mock_nats
        supervisor._cursor_db_path = tmp_path / "cursors.db"
        supervisor._log = MagicMock()

        # Instance name differs from kind — the generic-instance case.
        config = _cfg(
            name="my_firms_instance",
            kind="firms",
            enabled=True,
            cadence_s=300,
            settings={"api_key_alias": "firms", "satellites": ["VIIRS_SNPP_NRT"]},
        )

        await supervisor._start_adapter(config)

        # The class WAS resolved by kind: requires_api_key fired, key was found
        # missing, and the adapter refused to start. Had the lookup used
        # config.name ("my_firms_instance", absent from the registry), the
        # precondition would have been skipped and the adapter would have started.
        mock_config_store.get_api_key.assert_called_once_with("firms")
        mock_config_store.set_adapter_last_error.assert_called_once()
        err_args = mock_config_store.set_adapter_last_error.call_args[0]
        assert err_args[0] == "my_firms_instance"  # error keyed by instance id
        assert "missing api key" in err_args[1].lower()
        # Did not start.
        assert "my_firms_instance" not in supervisor._adapter_states
        mock_nats.publish.assert_not_called()

    def test_start_path_lookup_expression_uses_kind(self):
        """Belt-and-suspenders: the start-path lookup keys on config.kind.

        Direct assertion of the resolution expression (supervisor._adapters.get(
        config.kind)) independent of the heavier _start_adapter flow above.
        """
        from central.adapters.firms import FIRMSAdapter

        registry = {"firms": FIRMSAdapter}
        config = _cfg(name="my_firms_instance", kind="firms")

        # Resolving by kind finds the class; resolving by name would miss.
        assert registry.get(config.kind) is FIRMSAdapter
        assert registry.get(config.name) is None
