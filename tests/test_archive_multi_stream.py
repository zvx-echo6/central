"""Tests for multi-stream archive consumer."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from central.archive import (
    STREAMS,
    consumer_name_for,
    ArchiveConsumer,
)


class TestConsumerNaming:
    """Test consumer naming convention."""

    def test_consumer_name_for_central_wx(self):
        """Consumer name for CENTRAL_WX is archive-central_wx."""
        assert consumer_name_for("CENTRAL_WX") == "archive-central_wx"

    def test_consumer_name_for_central_fire(self):
        """Consumer name for CENTRAL_FIRE is archive-central_fire."""
        assert consumer_name_for("CENTRAL_FIRE") == "archive-central_fire"

    def test_consumer_name_for_central_quake(self):
        """Consumer name for CENTRAL_QUAKE is archive-central_quake."""
        assert consumer_name_for("CENTRAL_QUAKE") == "archive-central_quake"


class TestStreamsConfiguration:
    """Test streams configuration."""

    def test_streams_list_has_five_entries(self):
        """STREAMS list has five event-bearing streams."""
        assert len(STREAMS) == 5

    def test_streams_contains_central_wx(self):
        """STREAMS contains CENTRAL_WX with correct filter."""
        assert ("CENTRAL_WX", "central.wx.>") in STREAMS

    def test_streams_contains_central_fire(self):
        """STREAMS contains CENTRAL_FIRE with correct filter."""
        assert ("CENTRAL_FIRE", "central.fire.>") in STREAMS

    def test_streams_contains_central_quake(self):
        """STREAMS contains CENTRAL_QUAKE with correct filter."""
        assert ("CENTRAL_QUAKE", "central.quake.>") in STREAMS

    def test_streams_contains_central_space(self):
        """STREAMS contains CENTRAL_SPACE with correct filter."""
        assert ("CENTRAL_SPACE", "central.space.>") in STREAMS

    def test_streams_contains_central_disaster(self):
        """STREAMS contains CENTRAL_DISASTER with correct filter."""
        assert ("CENTRAL_DISASTER", "central.disaster.>") in STREAMS

    def test_streams_excludes_central_meta(self):
        """STREAMS does not contain CENTRAL_META (status messages only)."""
        stream_names = [s[0] for s in STREAMS]
        assert "CENTRAL_META" not in stream_names


class TestOrphanedConsumerCleanup:
    """Test cleanup of orphaned 'archive' consumer."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_orphaned_consumer_when_exists(self):
        """Cleanup removes 'archive' consumer from CENTRAL_WX when it exists."""
        consumer = ArchiveConsumer(
            nats_url="nats://localhost:4222",
            postgres_dsn="postgresql://test:test@localhost/test",
        )

        mock_js = AsyncMock()
        mock_js.consumer_info = AsyncMock(return_value=MagicMock())
        mock_js.delete_consumer = AsyncMock()
        consumer._js = mock_js

        await consumer._cleanup_orphaned_consumer()

        mock_js.consumer_info.assert_called_once_with("CENTRAL_WX", "archive")
        mock_js.delete_consumer.assert_called_once_with("CENTRAL_WX", "archive")

    @pytest.mark.asyncio
    async def test_cleanup_handles_not_found_gracefully(self):
        """Cleanup handles NotFoundError when 'archive' consumer doesn't exist."""
        from nats.js.errors import NotFoundError

        consumer = ArchiveConsumer(
            nats_url="nats://localhost:4222",
            postgres_dsn="postgresql://test:test@localhost/test",
        )

        mock_js = AsyncMock()
        mock_js.consumer_info = AsyncMock(side_effect=NotFoundError())
        mock_js.delete_consumer = AsyncMock()
        consumer._js = mock_js

        # Should not raise
        await consumer._cleanup_orphaned_consumer()

        mock_js.consumer_info.assert_called_once_with("CENTRAL_WX", "archive")
        mock_js.delete_consumer.assert_not_called()


class TestEnsureConsumer:
    """Test consumer creation for each stream."""

    @pytest.mark.asyncio
    async def test_ensure_consumer_creates_when_not_exists(self):
        """_ensure_consumer creates consumer when it doesn't exist."""
        from nats.js.errors import NotFoundError

        consumer = ArchiveConsumer(
            nats_url="nats://localhost:4222",
            postgres_dsn="postgresql://test:test@localhost/test",
        )

        mock_js = AsyncMock()
        mock_js.consumer_info = AsyncMock(side_effect=NotFoundError())
        mock_js.add_consumer = AsyncMock()
        consumer._js = mock_js

        await consumer._ensure_consumer(
            "CENTRAL_FIRE", "central.fire.>", "archive-central_fire"
        )

        mock_js.consumer_info.assert_called_once_with(
            "CENTRAL_FIRE", "archive-central_fire"
        )
        mock_js.add_consumer.assert_called_once()
        # Verify the consumer config
        call_args = mock_js.add_consumer.call_args
        assert call_args[0][0] == "CENTRAL_FIRE"
        config = call_args[0][1]
        assert config.durable_name == "archive-central_fire"
        assert config.filter_subject == "central.fire.>"

    @pytest.mark.asyncio
    async def test_ensure_consumer_skips_when_exists(self):
        """_ensure_consumer does nothing when consumer already exists."""
        consumer = ArchiveConsumer(
            nats_url="nats://localhost:4222",
            postgres_dsn="postgresql://test:test@localhost/test",
        )

        mock_js = AsyncMock()
        mock_js.consumer_info = AsyncMock(return_value=MagicMock())
        mock_js.add_consumer = AsyncMock()
        consumer._js = mock_js

        await consumer._ensure_consumer(
            "CENTRAL_QUAKE", "central.quake.>", "archive-central_quake"
        )

        mock_js.consumer_info.assert_called_once_with(
            "CENTRAL_QUAKE", "archive-central_quake"
        )
        mock_js.add_consumer.assert_not_called()
