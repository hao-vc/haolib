"""Test FastMCP entrypoint."""

import asyncio
import contextlib
import io
from collections.abc import Callable
from unittest.mock import patch

import pytest
from fastmcp import FastMCP
from fastmcp.server.middleware import MiddlewareContext

from haolib.entrypoints.fastmcp import FastMCPEntrypoint, FastMCPEntrypointComponent


class TestFastMCPEntrypointValidation:
    """Test FastMCP entrypoint validation."""

    def test_validate_succeeds_with_valid_app(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that validation succeeds with a valid FastMCP app."""
        fastmcp_entrypoint.validate()


class TestFastMCPEntrypointLifecycle:
    """Test FastMCP entrypoint lifecycle methods."""

    @pytest.mark.asyncio
    async def test_startup_validates_config(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that startup validates configuration."""
        await fastmcp_entrypoint.startup()
        await fastmcp_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_startup_is_idempotent(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that startup can be called multiple times safely."""
        await fastmcp_entrypoint.startup()
        await fastmcp_entrypoint.startup()  # Should not raise
        await fastmcp_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that shutdown can be called multiple times safely."""
        await fastmcp_entrypoint.startup()
        await fastmcp_entrypoint.shutdown()
        await fastmcp_entrypoint.shutdown()  # Should not raise

    @pytest.mark.asyncio
    async def test_run_with_startup(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that run works after startup.

        Note: FastMCP's run_async() prints a banner immediately when called.
        We mock stdout/stderr to suppress the banner output during testing.
        """
        await fastmcp_entrypoint.startup()

        # Mock stdout and stderr to suppress FastMCP banner
        fake_stdout = io.StringIO()
        fake_stderr = io.StringIO()

        with patch("sys.stdout", fake_stdout), patch("sys.stderr", fake_stderr):
            # Create run task but cancel it immediately to avoid blocking
            run_task = asyncio.create_task(fastmcp_entrypoint.run())

            # Give it a tiny moment to start, then cancel
            await asyncio.sleep(0.01)
            run_task.cancel()

            # Wait for cancellation, suppressing any exceptions
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await run_task

        await fastmcp_entrypoint.shutdown()


class TestFastMCPEntrypointBuilder:
    """Test FastMCP entrypoint builder methods."""

    def test_setup_exception_handlers_returns_self(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that setup_exception_handlers returns self for chaining."""
        handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]] = {
            ValueError: lambda _, __: None
        }
        result = fastmcp_entrypoint.setup_exception_handlers(handlers)
        assert result is fastmcp_entrypoint

    def test_get_app_returns_fastmcp_app(self, fastmcp_entrypoint: FastMCPEntrypoint) -> None:
        """Test that get_app returns the FastMCP app."""
        app = fastmcp_entrypoint.get_app()
        assert isinstance(app, FastMCP)


class TestFastMCPEntrypointComponent:
    """Test FastMCP entrypoint component."""

    def test_validate_succeeds_with_valid_app(self, fastmcp_component: FastMCPEntrypointComponent) -> None:
        """Test that component validation succeeds with valid app."""
        fastmcp_component.validate()

    def test_setup_exception_handlers_returns_self(self, fastmcp_component: FastMCPEntrypointComponent) -> None:
        """Test that setup_exception_handlers returns self for chaining."""
        handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]] = {
            ValueError: lambda _, __: None
        }
        result = fastmcp_component.setup_exception_handlers(handlers)
        assert result is fastmcp_component

    def test_get_app_returns_fastmcp_app(self, fastmcp_component: FastMCPEntrypointComponent) -> None:
        """Test that get_app returns the FastMCP app."""
        app = fastmcp_component.get_app()
        assert isinstance(app, FastMCP)
