"""Conftest for entrypoint tests."""

import asyncio
import contextlib
import io
import socket
from asyncio.events import AbstractEventLoop
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from fastapi import FastAPI
from fastmcp import FastMCP
from faststream import FastStream
from faststream.confluent import KafkaBroker
from taskiq import AsyncBroker, TaskiqScheduler
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from haolib.configs.server import ServerConfig
from haolib.entrypoints.abstract import AbstractEntrypoint
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
)
from haolib.entrypoints.plugins.fastapi import FastAPIDishkaPlugin
from haolib.entrypoints.taskiq import TaskiqEntrypoint

# Import container fixture from parent conftest
# This will be available via pytest's fixture system


def _get_free_port() -> int:
    """Get a free port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        return s.getsockname()[1]


@pytest_asyncio.fixture
async def fastapi_app() -> FastAPI:
    """Create a FastAPI app for testing."""
    return FastAPI()


@pytest_asyncio.fixture
async def fastapi_entrypoint(fastapi_app: FastAPI) -> FastAPIEntrypoint:
    """Create a FastAPI entrypoint for testing."""
    # Use a unique port to avoid conflicts
    server_config = ServerConfig(host="localhost", port=_get_free_port())
    return FastAPIEntrypoint(app=fastapi_app, server_config=server_config)


@pytest_asyncio.fixture
async def fastapi_entrypoint_with_dishka(fastapi_app: FastAPI, container: AsyncContainer) -> FastAPIEntrypoint:
    """Create a FastAPI entrypoint with Dishka configured."""
    return FastAPIEntrypoint(app=fastapi_app).use_plugin(FastAPIDishkaPlugin(container))


@pytest_asyncio.fixture
async def fastmcp_app() -> FastMCP:
    """Create a FastMCP app for testing."""
    return FastMCP()


@pytest_asyncio.fixture
async def fastmcp_entrypoint(fastmcp_app: FastMCP) -> FastMCPEntrypoint:
    """Create a FastMCP entrypoint for testing."""
    return FastMCPEntrypoint(fastmcp=fastmcp_app)


@pytest_asyncio.fixture
async def faststream_broker() -> KafkaBroker:
    """Create a FastStream broker for testing."""
    return KafkaBroker()


@pytest_asyncio.fixture
async def faststream_app(faststream_broker: KafkaBroker) -> FastStream:
    """Create a FastStream app for testing."""
    return FastStream(faststream_broker)


@pytest_asyncio.fixture
async def faststream_entrypoint(faststream_app: FastStream) -> FastStreamEntrypoint:
    """Create a FastStream entrypoint for testing."""
    return FastStreamEntrypoint(app=faststream_app)


@pytest_asyncio.fixture
async def taskiq_broker() -> AsyncBroker:
    """Create a Taskiq broker for testing."""
    return RedisStreamBroker(url="redis://localhost:6379")


@pytest_asyncio.fixture
async def taskiq_entrypoint(taskiq_broker: AsyncBroker) -> TaskiqEntrypoint:
    """Create a Taskiq entrypoint for testing."""
    return TaskiqEntrypoint(broker=taskiq_broker)


@pytest_asyncio.fixture
async def taskiq_entrypoint_with_worker(taskiq_entrypoint: TaskiqEntrypoint) -> TaskiqEntrypoint:
    """Create a Taskiq entrypoint with worker configured."""
    return taskiq_entrypoint.setup_worker()


@pytest_asyncio.fixture
async def taskiq_scheduler(taskiq_broker: AsyncBroker) -> TaskiqScheduler:
    """Create a Taskiq scheduler for testing."""
    return TaskiqScheduler(broker=taskiq_broker, sources=[ListRedisScheduleSource(url="redis://localhost:6379")])


@pytest_asyncio.fixture
async def taskiq_entrypoint_with_scheduler(
    taskiq_entrypoint: TaskiqEntrypoint, taskiq_scheduler: TaskiqScheduler
) -> TaskiqEntrypoint:
    """Create a Taskiq entrypoint with scheduler configured."""
    return taskiq_entrypoint.setup_scheduler(taskiq_scheduler)


class MockProvider(Provider):
    """Mock provider for testing."""

    @provide(scope=Scope.APP)
    async def get_container(self) -> AsyncContainer:
        """Get container."""
        return make_async_container(MockProvider())


@pytest_asyncio.fixture
async def mock_container() -> AsyncContainer:
    """Create a mock container for testing."""
    return make_async_container(MockProvider())


@pytest.fixture
def event_loop() -> Generator[AbstractEventLoop, Any]:
    """Create an event loop for testing."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


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


async def run_entrypoint_briefly(entrypoint: AbstractEntrypoint, duration: float = 0.1) -> None:
    """Run an entrypoint briefly for testing purposes.

    Args:
        entrypoint: The entrypoint to run.
        duration: How long to run it (in seconds).

    """
    await entrypoint.startup()

    # Check if this is a FastMCP entrypoint (which prints a banner)
    is_fastmcp = isinstance(entrypoint, FastMCPEntrypoint)

    # Create the run task
    if is_fastmcp:
        # For FastMCP, suppress stdout/stderr/stdin to prevent banner from showing
        # FastMCP needs .buffer attribute and readable stdin, so we use proper mocks
        fake_stdout = _create_mock_stdio()
        fake_stderr = _create_mock_stdio()
        fake_stdin = _create_mock_stdio()
        with patch("sys.stdout", fake_stdout), patch("sys.stderr", fake_stderr), patch("sys.stdin", fake_stdin):
            run_task = asyncio.create_task(entrypoint.run())
            await asyncio.sleep(duration)
            run_task.cancel()
    else:
        run_task = asyncio.create_task(entrypoint.run())
        await asyncio.sleep(duration)
        run_task.cancel()

    # Wait for cancellation to complete, suppressing CancelledError
    with contextlib.suppress(asyncio.CancelledError):
        await run_task

    # If task is still running, force cancel it
    if not run_task.done():
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await run_task

    # Ensure shutdown is called
    await entrypoint.shutdown()
