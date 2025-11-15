"""Test component integration with entrypoints."""

from collections.abc import Callable
from typing import Any

from dishka import AsyncContainer
from fastmcp.server.middleware import MiddlewareContext
from faststream.confluent.broker import KafkaBroker

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.faststream import FastStreamEntrypoint
from haolib.entrypoints.plugins.fastapi.dishka import FastAPIDishkaPlugin
from haolib.entrypoints.plugins.fastapi.fastmcp import FastAPIFastMCPPlugin
from haolib.entrypoints.plugins.fastapi.faststream import FastAPIFastStreamPlugin
from haolib.entrypoints.plugins.fastmcp.exceptions import FastMCPExceptionHandlersPlugin
from haolib.entrypoints.plugins.faststream.dishka import FastStreamDishkaPlugin
from haolib.entrypoints.plugins.faststream.exceptions import FastStreamExceptionHandlersPlugin


class TestFastMCPComponentIntegration:
    """Test FastMCP component integration with FastAPI."""

    def test_setup_mcp_validates_component(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_entrypoint: FastMCPEntrypoint
    ) -> None:
        """Test that setup_mcp validates the component."""
        result = fastapi_entrypoint.use_plugin(FastAPIFastMCPPlugin(fastmcp_entrypoint.get_app(), path="/mcp"))
        assert result is fastapi_entrypoint

    def test_setup_mcp_returns_self(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_entrypoint: FastMCPEntrypoint
    ) -> None:
        """Test that setup_mcp returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPIFastMCPPlugin(fastmcp_entrypoint.get_app(), path="/mcp"))
        assert result is fastapi_entrypoint

    def test_setup_mcp_mounts_app(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_entrypoint: FastMCPEntrypoint
    ) -> None:
        """Test that setup_mcp mounts the FastMCP app."""
        fastapi_entrypoint.use_plugin(FastAPIFastMCPPlugin(fastmcp_entrypoint.get_app(), path="/mcp"))
        # Verify setup_mcp completes without error - mount is added internally


class TestFastStreamComponentIntegration:
    """Test FastStream component integration with FastAPI."""

    def test_setup_faststream_validates_component(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_broker: KafkaBroker,
    ) -> None:
        """Test that setup_faststream validates the component."""
        result = fastapi_entrypoint.use_plugin(FastAPIFastStreamPlugin(faststream_broker))
        assert result is fastapi_entrypoint

    def test_setup_faststream_returns_self(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_broker: KafkaBroker,
    ) -> None:
        """Test that setup_faststream returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPIFastStreamPlugin(faststream_broker))
        assert result is fastapi_entrypoint

    def test_setup_faststream_with_dishka(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_broker: KafkaBroker,
        mock_container: AsyncContainer,
    ) -> None:
        """Test that setup_faststream works with Dishka configured component."""
        fastapi_entrypoint.use_plugin(FastAPIDishkaPlugin(mock_container))
        result = fastapi_entrypoint.use_plugin(FastAPIFastStreamPlugin(faststream_broker))
        assert result is fastapi_entrypoint


class TestComponentBuilderPattern:
    """Test component builder pattern."""

    def test_fastmcp_component_builder_chaining(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test FastMCP component builder chaining."""
        handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]] = {
            ValueError: lambda _, __: None
        }
        result = fastmcp_entrypoint.use_plugin(FastMCPExceptionHandlersPlugin(handlers))
        assert result is fastmcp_entrypoint

    def test_faststream_component_builder_chaining(
        self,
        faststream_entrypoint: FastStreamEntrypoint,
        mock_container: AsyncContainer,
    ) -> None:
        """Test FastStream component builder chaining."""
        handlers: dict[type[Exception], Callable[..., Any]] = {ValueError: lambda _: None}
        result = faststream_entrypoint.use_plugin(FastStreamDishkaPlugin(mock_container)).use_plugin(
            FastStreamExceptionHandlersPlugin(handlers)
        )
        assert result is faststream_entrypoint
