"""Conftest for idempotency."""

import pytest_asyncio
from dishka import AsyncContainer
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from haolib.entrypoints.fastapi import FastAPIEntrypoint


@pytest_asyncio.fixture
async def app(container: AsyncContainer) -> FastAPI:
    """Test app."""
    return FastAPIEntrypoint(app=FastAPI()).setup_dishka(container).setup_idempotency_middleware().get_app()


@pytest_asyncio.fixture
async def test_client(app: FastAPI) -> AsyncClient:
    """Test client."""
    return AsyncClient(transport=ASGITransport(app), base_url="http://test")
