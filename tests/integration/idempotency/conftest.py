"""Conftest for idempotency."""

import pytest_asyncio
from dishka import AsyncContainer
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.fastapi import FastAPIDishkaPlugin, FastAPIIdempotencyMiddlewarePlugin


@pytest_asyncio.fixture
async def app(container: AsyncContainer) -> FastAPI:
    """Test app."""
    app_instance = FastAPI()
    FastAPIEntrypoint(app=app_instance).use_plugin(FastAPIDishkaPlugin(container)).use_plugin(
        FastAPIIdempotencyMiddlewarePlugin()
    )
    return app_instance


@pytest_asyncio.fixture
async def test_client(app: FastAPI) -> AsyncClient:
    """Test client."""
    return AsyncClient(transport=ASGITransport(app), base_url="http://test")
