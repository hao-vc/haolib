"""Test FastStream entrypoint."""

from collections.abc import Callable
from typing import Any

import pytest
from dishka import AsyncContainer
from faststream import FastStream

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
)
from haolib.entrypoints.plugins.faststream import (
    FastStreamDishkaPlugin,
    FastStreamExceptionHandlersPlugin,
)
from tests.integration.entrypoints.conftest import (
    run_entrypoint_briefly,
)


class TestFastStreamEntrypointLifecycle:
    """Test FastStream entrypoint lifecycle methods."""

    @pytest.mark.asyncio
    async def test_startup_validates_config(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that startup validates configuration."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_startup_is_idempotent(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that startup can be called multiple times safely."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.startup()  # Should not raise
        await faststream_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that shutdown can be called multiple times safely."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.shutdown()
        await faststream_entrypoint.shutdown()  # Should not raise

    @pytest.mark.asyncio
    async def test_run_with_startup(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that run works after startup."""
        await run_entrypoint_briefly(faststream_entrypoint)


class TestFastStreamEntrypointBuilder:
    """Test FastStream entrypoint builder methods."""

    def test_use_plugin_dishka_returns_self(
        self, faststream_entrypoint: FastStreamEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that use_plugin with FastStreamDishkaPlugin returns self for chaining."""
        result = faststream_entrypoint.use_plugin(FastStreamDishkaPlugin(mock_container))
        assert result is faststream_entrypoint

    def test_use_plugin_exception_handlers_returns_self(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that use_plugin with FastStreamExceptionHandlersPlugin returns self for chaining."""
        handlers: dict[type[Exception], Callable[..., Any]] = {ValueError: lambda _: None}
        result = faststream_entrypoint.use_plugin(FastStreamExceptionHandlersPlugin(handlers))
        assert result is faststream_entrypoint

    def test_use_plugin_exception_handlers_fails_without_broker(self) -> None:
        """Test that use_plugin with exception handlers fails without broker."""
        app = FastStream()  # App without broker
        entrypoint = FastStreamEntrypoint(app=app)

        with pytest.raises(EntrypointInconsistencyError, match="FastStream broker is not set"):
            entrypoint.use_plugin(FastStreamExceptionHandlersPlugin({ValueError: lambda _: None}))

    def test_builder_pattern_chaining(
        self, faststream_entrypoint: FastStreamEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that use_plugin methods can be chained."""
        result = faststream_entrypoint.use_plugin(FastStreamDishkaPlugin(mock_container)).use_plugin(
            FastStreamExceptionHandlersPlugin({ValueError: lambda _: None})
        )
        assert result is faststream_entrypoint
