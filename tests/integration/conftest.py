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
from haolib.configs.s3 import S3Config
from haolib.configs.sqlalchemy import SQLAlchemyConfig
from haolib.dependencies.dishka.redis import RedisProvider
from haolib.dependencies.dishka.sqlalchemy import SQLAlchemyProvider
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.plugins.fastapi import (
    FastAPIDishkaPlugin,
    FastAPIExceptionHandlersPlugin,
    FastAPIFastMCPPlugin,
    FastAPIIdempotencyMiddlewarePlugin,
)
from haolib.exceptions.base.fastapi import FastAPIBaseException
from haolib.exceptions.handlers.fastapi import (
    fastapi_base_exception_handler,
    fastapi_unknown_exception_handler,
    fastapi_unknown_exception_handler_with_observability,
)
from haolib.web.idempotency.storages.abstract import AbstractIdempotencyKeysStorage
from haolib.web.idempotency.storages.redis import RedisIdempotencyKeysStorage


class MockAppConfig(BaseConfig):
    """Mock app config."""

    s3: S3Config
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

    @provide(scope=Scope.REQUEST)
    async def idempotency_keys_storage(
        self, redis: Redis, idempotency_config: IdempotencyConfig
    ) -> AbstractIdempotencyKeysStorage:
        """Idempotency keys storage."""
        return RedisIdempotencyKeysStorage(redis=redis, ttl=idempotency_config.ttl)

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
    return make_async_container(SQLAlchemyProvider(), RedisProvider(), MockProvider())


@pytest_asyncio.fixture()
async def clean_redis(container: AsyncContainer) -> None:
    """Clean Redis for testing."""
    async with container(scope=Scope.REQUEST) as nested_container:
        await (await nested_container.get(Redis)).flushdb()


@pytest_asyncio.fixture
async def app(container: AsyncContainer) -> FastAPI:
    """Test app."""
    app_instance = FastAPI()
    FastAPIEntrypoint(app=app_instance).use_plugin(FastAPIDishkaPlugin(container)).use_plugin(
        FastAPIExceptionHandlersPlugin(
            exception_handlers={
                Exception: fastapi_unknown_exception_handler,
                FastAPIBaseException: fastapi_base_exception_handler,
            }
        )
    ).use_plugin(FastAPIIdempotencyMiddlewarePlugin())
    return app_instance


@pytest_asyncio.fixture
async def app_with_observability(container: AsyncContainer) -> FastAPI:
    """Test app."""
    app_instance = FastAPI()
    FastAPIEntrypoint(app=app_instance).use_plugin(FastAPIDishkaPlugin(container)).use_plugin(
        FastAPIExceptionHandlersPlugin(
            exception_handlers={
                Exception: fastapi_unknown_exception_handler_with_observability,
                FastAPIBaseException: fastapi_base_exception_handler,
            }
        )
    ).use_plugin(FastAPIIdempotencyMiddlewarePlugin())
    return app_instance


@pytest_asyncio.fixture
async def app_with_mcp_and_mcp(container: AsyncContainer) -> tuple[FastAPI, FastMCP]:
    """Test app."""

    app = FastAPI()
    fastapi_entrypoint = (
        FastAPIEntrypoint(app=FastAPI())
        .use_plugin(FastAPIDishkaPlugin(container))
        .use_plugin(
            FastAPIExceptionHandlersPlugin(
                exception_handlers={
                    Exception: fastapi_unknown_exception_handler,
                    FastAPIBaseException: fastapi_base_exception_handler,
                }
            )
        )
        .use_plugin(FastAPIIdempotencyMiddlewarePlugin())
    )

    @app.post("/hello")
    async def hello(body: Request) -> str:
        return "hello"

    fastmcp_entrypoint = FastMCPEntrypoint(fastmcp=FastMCP.from_fastapi(app=app))
    fastapi_entrypoint.use_plugin(FastAPIFastMCPPlugin(fastmcp_entrypoint.get_app(), "/mcp"))

    return app, fastmcp_entrypoint.get_app()


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
