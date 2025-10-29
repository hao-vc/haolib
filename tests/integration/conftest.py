"""Conftest."""

import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from fastmcp import FastMCP
from httpx import ASGITransport, AsyncClient
from pydantic import Field
from redis.asyncio import Redis

from haolib.configs.base import BaseConfig
from haolib.configs.idempotency import IdempotencyConfig
from haolib.configs.observability import ObservabilityConfig
from haolib.configs.redis import RedisConfig
from haolib.configs.sqlalchemy import SQLAlchemyConfig
from haolib.dependencies.idempotency import IdempotencyProvider
from haolib.dependencies.redis import RedisProvider
from haolib.dependencies.sqlalchemy import SQLAlchemyProvider
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.exceptions.fastapi import register_exception_handlers


def register_exception_handlers_for_test(app: FastAPI) -> None:
    """Register exception handlers for test."""
    register_exception_handlers(app, should_observe_exceptions=False)


class MockAppConfig(BaseConfig):
    """Mock app config."""

    sqlalchemy: SQLAlchemyConfig
    redis: RedisConfig
    idempotency: IdempotencyConfig
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)


class MockProvider(Provider):
    """Mock provider."""

    @provide(scope=Scope.APP)
    async def app_config(self) -> MockAppConfig:
        """App config."""
        return MockAppConfig.from_env()

    @provide(scope=Scope.APP)
    async def sqlalchemy_config(self, app_config: MockAppConfig) -> SQLAlchemyConfig:
        """SQLAlchemy config."""
        return app_config.sqlalchemy

    @provide(scope=Scope.APP)
    async def redis_config(self, app_config: MockAppConfig) -> RedisConfig:
        """Redis config."""
        return app_config.redis

    @provide(scope=Scope.APP)
    async def idempotency_config(self, app_config: MockAppConfig) -> IdempotencyConfig:
        """Idempotency config."""
        return app_config.idempotency


@pytest_asyncio.fixture
async def container() -> AsyncContainer:
    """Test container."""
    return make_async_container(MockProvider(), SQLAlchemyProvider(), RedisProvider(), IdempotencyProvider())


@pytest_asyncio.fixture()
async def clean_redis(container: AsyncContainer) -> None:
    """Clean Redis for testing."""
    async with container(scope=Scope.REQUEST) as nested_container:
        await (await nested_container.get(Redis)).flushdb()


@pytest_asyncio.fixture
async def app(container: AsyncContainer) -> FastAPI:
    """Test app."""
    return (
        FastAPIEntrypoint(app=FastAPI())
        .setup_dishka(container)
        .setup_exception_handlers(should_observe_exceptions=False)
        .setup_idempotency_middleware()
        .get_app()
    )


@pytest_asyncio.fixture
async def app_with_observability(container: AsyncContainer) -> FastAPI:
    """Test app."""
    return (
        FastAPIEntrypoint(app=FastAPI())
        .setup_dishka(container)
        .setup_exception_handlers(should_observe_exceptions=True)
        .setup_idempotency_middleware()
        .get_app()
    )


@pytest_asyncio.fixture
async def app_with_mcp_and_mcp(container: AsyncContainer) -> tuple[FastAPI, FastMCP]:
    """Test app."""

    app = FastAPI()
    fastapi_entrypoint = (
        FastAPIEntrypoint(app=FastAPI())
        .setup_dishka(container)
        .setup_exception_handlers(should_observe_exceptions=False)
        .setup_idempotency_middleware()
    )

    @app.post("/hello")
    async def hello(body: Request) -> str:
        return "hello"

    fastmcp_entrypoint = FastMCPEntrypoint(fastmcp=FastMCP.from_fastapi(app=app))
    fastapi_entrypoint.setup_mcp(fastmcp_entrypoint, "/mcp")

    return fastapi_entrypoint.get_app(), fastmcp_entrypoint.get_app()


@pytest_asyncio.fixture
async def test_client(app: FastAPI) -> AsyncClient:
    """Test client."""
    return AsyncClient(transport=ASGITransport(app), base_url="http://test")


@pytest_asyncio.fixture
async def test_client_without_raise_server_exceptions(app: FastAPI) -> TestClient:
    """Test client with raise server exceptions."""
    return TestClient(app, raise_server_exceptions=False)


@pytest_asyncio.fixture
async def test_client_without_raise_server_exceptions_with_observability(app_with_observability: FastAPI) -> TestClient:
    """Test client with raise server exceptions and observability."""
    return TestClient(app_with_observability, raise_server_exceptions=False)
