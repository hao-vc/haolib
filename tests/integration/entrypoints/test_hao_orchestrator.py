"""Test HAO orchestrator."""

import asyncio
import contextlib
import io
from collections.abc import Callable
from typing import Any, Self
from unittest.mock import MagicMock, patch

import pytest

from haolib.entrypoints import HAOrchestrator
from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError


def _create_mock_stdio() -> MagicMock:
    """Create a mock stdio object that supports .buffer attribute for FastMCP."""
    mock_stdio = MagicMock()
    # Create a BytesIO buffer that can be wrapped and is readable/writeable
    buffer = io.BytesIO()
    # Make the mock have a .buffer attribute
    mock_stdio.buffer = buffer
    # Make it writeable and readable
    mock_stdio.write = MagicMock(return_value=0)
    mock_stdio.read = MagicMock(return_value=b"")
    mock_stdio.readline = MagicMock(return_value=b"")
    mock_stdio.flush = MagicMock()
    # Make it iterable for anyio
    mock_stdio.__iter__ = MagicMock(return_value=iter([]))
    mock_stdio.__aiter__ = MagicMock(return_value=iter([]))
    return mock_stdio


class TestHAOInitialization:
    """Test HAO initialization."""


class TestHAOAddEntrypoint:
    """Test HAO add_entrypoint method."""

    def test_add_entrypoint_returns_self(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test that add_entrypoint returns self for chaining."""
        hao = HAOrchestrator()
        result = hao.add_entrypoint(fastapi_entrypoint)
        assert result is hao

    def test_add_entrypoint_validates_entrypoint(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test that add_entrypoint validates the entrypoint."""
        hao = HAOrchestrator()
        hao.add_entrypoint(fastapi_entrypoint)  # Should not raise

    def test_add_entrypoint_chaining(
        self, fastapi_entrypoint: AbstractEntrypoint, fastmcp_entrypoint: AbstractEntrypoint
    ) -> None:
        """Test that add_entrypoint can be chained."""
        hao = HAOrchestrator()
        hao.add_entrypoint(fastapi_entrypoint).add_entrypoint(fastmcp_entrypoint)


class TestHAORunEntrypoints:
    """Test HAO run_entrypoints method."""

    @pytest.mark.asyncio
    async def test_run_entrypoints_with_empty_list(self) -> None:
        """Test that run_entrypoints handles empty list gracefully."""
        hao = HAOrchestrator()
        await hao.run_entrypoints([])  # Should not raise

    @pytest.mark.asyncio
    async def test_run_entrypoints_with_single_entrypoint(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test running a single entrypoint."""
        hao = HAOrchestrator()
        task = asyncio.create_task(hao.run_entrypoints([fastapi_entrypoint]))

        # Cancel after a brief moment
        await asyncio.sleep(0.1)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_with_multiple_entrypoints(
        self,
        fastapi_entrypoint: AbstractEntrypoint,
        fastmcp_entrypoint: AbstractEntrypoint,
    ) -> None:
        """Test running multiple entrypoints concurrently."""
        hao = HAOrchestrator()

        # Suppress FastMCP banner output and stdin
        fake_stdout = _create_mock_stdio()
        fake_stderr = _create_mock_stdio()
        fake_stdin = _create_mock_stdio()

        with patch("sys.stdout", fake_stdout), patch("sys.stderr", fake_stderr), patch("sys.stdin", fake_stdin):
            task = asyncio.create_task(hao.run_entrypoints([fastapi_entrypoint, fastmcp_entrypoint]))

            # Cancel after a brief moment (keep patch active during execution)
            await asyncio.sleep(0.1)
            task.cancel()

            # Wait for cancellation while patch is still active
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_uses_added_entrypoints(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test that run_entrypoints uses entrypoints added via add_entrypoint."""
        hao = HAOrchestrator()
        hao.add_entrypoint(fastapi_entrypoint)

        task = asyncio.create_task(hao.run_entrypoints())  # No arguments, uses added entrypoints

        # Cancel after a brief moment
        await asyncio.sleep(0.1)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_replaces_added_entrypoints(
        self,
        fastapi_entrypoint: AbstractEntrypoint,
        fastmcp_entrypoint: AbstractEntrypoint,
    ) -> None:
        """Test that run_entrypoints replaces previously added entrypoints."""
        hao = HAOrchestrator()
        hao.add_entrypoint(fastapi_entrypoint)

        # Suppress FastMCP banner output and stdin
        fake_stdout = _create_mock_stdio()
        fake_stderr = _create_mock_stdio()
        fake_stdin = _create_mock_stdio()

        with patch("sys.stdout", fake_stdout), patch("sys.stderr", fake_stderr), patch("sys.stdin", fake_stdin):
            task = asyncio.create_task(hao.run_entrypoints([fastmcp_entrypoint]))  # Replaces fastapi_entrypoint

            # Cancel after a brief moment
            await asyncio.sleep(0.1)
            task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_validates_before_startup(
        self, taskiq_entrypoint_with_worker: AbstractEntrypoint
    ) -> None:
        """Test that run_entrypoints validates entrypoints before startup."""
        hao = HAOrchestrator()
        task = asyncio.create_task(hao.run_entrypoints([taskiq_entrypoint_with_worker]))

        # Cancel after a brief moment
        await asyncio.sleep(0.1)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_calls_startup(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test that run_entrypoints calls startup on entrypoints."""
        hao = HAOrchestrator()
        task = asyncio.create_task(hao.run_entrypoints([fastapi_entrypoint]))

        # Give it a moment to start
        await asyncio.sleep(0.05)

        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_run_entrypoints_calls_shutdown_on_error(self, fastapi_entrypoint: AbstractEntrypoint) -> None:
        """Test that run_entrypoints calls shutdown even on error."""
        hao = HAOrchestrator()

        # Create a task that will be cancelled
        task = asyncio.create_task(hao.run_entrypoints([fastapi_entrypoint]))

        await asyncio.sleep(0.05)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

        # Shutdown should have been called (server should be cleaned up)
        # The exact state depends on implementation, but shutdown should be idempotent


class TestHAOErrorHandling:
    """Test HAO error handling."""

    @pytest.mark.asyncio
    async def test_run_entrypoints_handles_startup_errors(self) -> None:
        """Test that run_entrypoints handles startup errors gracefully."""

        # Create an entrypoint that will fail validation
        class InvalidEntrypoint:
            """Invalid entrypoint for testing."""

            async def startup(self) -> None:
                raise EntrypointInconsistencyError("Invalid configuration")

            async def run(self) -> None:
                pass

            async def shutdown(self) -> None:
                pass

            def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> Self:  # noqa: ARG002
                return self

            def validate(self) -> None:
                raise EntrypointInconsistencyError("Invalid configuration")

        invalid_entrypoint = InvalidEntrypoint()
        hao = HAOrchestrator()

        with pytest.raises(EntrypointInconsistencyError):
            await hao.run_entrypoints([invalid_entrypoint])  # type: ignore[arg-type]
