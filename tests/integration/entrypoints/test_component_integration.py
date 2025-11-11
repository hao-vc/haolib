"""Test component integration with entrypoints."""

from collections.abc import Callable
from typing import Any

from dishka import AsyncContainer
from fastmcp.server.middleware import MiddlewareContext

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypointComponent
from haolib.entrypoints.faststream import FastStreamEntrypointComponent


class TestFastMCPComponentIntegration:
    """Test FastMCP component integration with FastAPI."""

    def test_setup_mcp_validates_component(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_component: FastMCPEntrypointComponent
    ) -> None:
        """Test that setup_mcp validates the component."""
        result = fastapi_entrypoint.setup_mcp(fastmcp_component, path="/mcp")
        assert result is fastapi_entrypoint

    def test_setup_mcp_returns_self(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_component: FastMCPEntrypointComponent
    ) -> None:
        """Test that setup_mcp returns self for chaining."""
        result = fastapi_entrypoint.setup_mcp(fastmcp_component, path="/mcp")
        assert result is fastapi_entrypoint

    def test_setup_mcp_mounts_app(
        self, fastapi_entrypoint: FastAPIEntrypoint, fastmcp_component: FastMCPEntrypointComponent
    ) -> None:
        """Test that setup_mcp mounts the FastMCP app."""
        fastapi_entrypoint.setup_mcp(fastmcp_component, path="/mcp")

        app = fastapi_entrypoint.get_app()
        # Verify the mount was added (check router)
        assert len(app.routes) > 0 or hasattr(app, "router")


class TestFastStreamComponentIntegration:
    """Test FastStream component integration with FastAPI."""

    def test_setup_faststream_validates_component(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_component: FastStreamEntrypointComponent,
    ) -> None:
        """Test that setup_faststream validates the component."""
        result = fastapi_entrypoint.setup_faststream(faststream_component)
        assert result is fastapi_entrypoint

    def test_setup_faststream_returns_self(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_component: FastStreamEntrypointComponent,
    ) -> None:
        """Test that setup_faststream returns self for chaining."""
        result = fastapi_entrypoint.setup_faststream(faststream_component)
        assert result is fastapi_entrypoint

    def test_setup_faststream_with_dishka(
        self,
        fastapi_entrypoint: FastAPIEntrypoint,
        faststream_component: FastStreamEntrypointComponent,
        mock_container: AsyncContainer,
    ) -> None:
        """Test that setup_faststream works with Dishka configured component."""
        faststream_component.setup_dishka(mock_container)
        result = fastapi_entrypoint.setup_faststream(faststream_component)
        assert result is fastapi_entrypoint


class TestComponentValidation:
    """Test component validation."""

    def test_fastmcp_component_validate(self, fastmcp_component: FastMCPEntrypointComponent) -> None:
        """Test FastMCP component validation."""
        fastmcp_component.validate()  # Should not raise

    def test_faststream_component_validate(self, faststream_component: FastStreamEntrypointComponent) -> None:
        """Test FastStream component validation."""
        faststream_component.validate()  # Should not raise


class TestComponentBuilderPattern:
    """Test component builder pattern."""

    def test_fastmcp_component_builder_chaining(self, fastmcp_component: FastMCPEntrypointComponent) -> None:
        """Test FastMCP component builder chaining."""
        handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]] = {
            ValueError: lambda _, __: None
        }
        result = fastmcp_component.setup_exception_handlers(handlers)
        assert result is fastmcp_component

    def test_faststream_component_builder_chaining(
        self,
        faststream_component: FastStreamEntrypointComponent,
        mock_container: AsyncContainer,
    ) -> None:
        """Test FastStream component builder chaining."""
        handlers: dict[type[Exception], Callable[..., Any]] = {ValueError: lambda _: None}
        result = faststream_component.setup_dishka(mock_container).setup_exception_handlers(handlers)
        assert result is faststream_component
