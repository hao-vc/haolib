"""Test entrypoints."""

from datetime import timedelta

import pytest
from dishka import AsyncContainer
from fastapi import FastAPI
from fastmcp import FastMCP
from faststream.confluent import KafkaBroker
from redis.asyncio import Redis
from taskiq import TaskiqScheduler
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from haolib.configs.cors import CORSConfig
from haolib.entrypoints import HAOrchestrator
from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.fastapi.cors import FastAPICORSMiddlewarePlugin
from haolib.entrypoints.plugins.fastapi.dishka import FastAPIDishkaPlugin
from haolib.entrypoints.plugins.fastapi.exceptions import FastAPIExceptionHandlersPlugin
from haolib.entrypoints.plugins.fastapi.fastmcp import FastAPIFastMCPPlugin
from haolib.entrypoints.plugins.fastapi.faststream import FastAPIFastStreamPlugin
from haolib.entrypoints.plugins.fastapi.idempotency import FastAPIIdempotencyMiddlewarePlugin
from haolib.entrypoints.plugins.fastapi.observability import FastAPIObservabilityPlugin
from haolib.entrypoints.plugins.taskiq.dishka import TaskiqDishkaPlugin
from haolib.entrypoints.taskiq import TaskiqEntrypoint
from haolib.observability.setupper import ObservabilitySetupper
from haolib.web.idempotency.storages.abstract import AbstractIdempotencyKeysStorage
from haolib.web.idempotency.storages.redis import RedisIdempotencyKeysStorage
from tests.utils import ensure_successful_run


def test_fastapi_entrypoint_full_default_setup(container: AsyncContainer) -> None:
    """Test entrypoints."""
    app = FastAPI()
    faststream_broker = KafkaBroker()
    fastmcp = FastMCP()
    taskiq_broker = RedisStreamBroker(url="redis://localhost:6379")
    taskiq_entrypoint = (
        TaskiqEntrypoint(broker=taskiq_broker)
        .use_plugin(TaskiqDishkaPlugin(container))
        .setup_worker()
        .setup_scheduler(
            TaskiqScheduler(broker=taskiq_broker, sources=[ListRedisScheduleSource(url="redis://localhost:6379")])
        )
    )

    fastapi_entrypoint = (
        FastAPIEntrypoint(app=app)
        .use_plugin(FastAPIDishkaPlugin(container))
        .use_plugin(FastAPIObservabilityPlugin(ObservabilitySetupper().setup_logging().setup_tracing().setup_metrics()))
        .use_plugin(FastAPIFastStreamPlugin(faststream_broker))
        .use_plugin(FastAPICORSMiddlewarePlugin())
        .use_plugin(
            FastAPIFastMCPPlugin(fastmcp, path="/mcp"),
        )
        .use_plugin(FastAPIExceptionHandlersPlugin({Exception: lambda *_: None}))
    )

    ensure_successful_run(HAOrchestrator(), [fastapi_entrypoint, taskiq_entrypoint])


def test_fastapi_entrypoint_cors_middleware_setup_with_default_config() -> None:
    """Test entrypoints."""
    app = FastAPI()
    fastapi_entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPICORSMiddlewarePlugin())

    ensure_successful_run(HAOrchestrator(), [fastapi_entrypoint])


def test_fastapi_entrypoint_cors_middleware_setup_with_custom_config() -> None:
    """Test entrypoints."""
    app = FastAPI()
    fastapi_entrypoint = FastAPIEntrypoint(app=app).use_plugin(
        FastAPICORSMiddlewarePlugin(cors_config=CORSConfig(allow_origins=["https://example.com"]))
    )

    ensure_successful_run(HAOrchestrator(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_with_container(container: AsyncContainer) -> None:
    """Test entrypoints."""
    app = FastAPI()

    fastapi_entrypoint = (
        FastAPIEntrypoint(app=app)
        .use_plugin(FastAPIDishkaPlugin(container))
        .use_plugin(FastAPIIdempotencyMiddlewarePlugin())
    )

    ensure_successful_run(HAOrchestrator(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_with_storage_factory(container: AsyncContainer) -> None:
    """Test entrypoints."""
    app = FastAPI()

    redis = Redis.from_url("redis://localhost:6379")

    async def idempotency_keys_storage_factory() -> AbstractIdempotencyKeysStorage:
        return RedisIdempotencyKeysStorage(redis=redis, ttl=timedelta(seconds=300))

    fastapi_entrypoint = (
        FastAPIEntrypoint(app=app)
        .use_plugin(FastAPIDishkaPlugin(container))
        .use_plugin(
            FastAPIIdempotencyMiddlewarePlugin(idempotency_keys_storage_factory=idempotency_keys_storage_factory)
        )
    )

    ensure_successful_run(HAOrchestrator(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_without_container_and_storage_factory() -> None:
    """Test entrypoints."""
    app = FastAPI()

    with pytest.raises(EntrypointInconsistencyError):
        FastAPIEntrypoint(app=app).use_plugin(FastAPIIdempotencyMiddlewarePlugin())
