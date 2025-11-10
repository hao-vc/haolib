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
from haolib.entrypoints import HAO
from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypointComponent
from haolib.entrypoints.faststream import FastStreamEntrypointComponent
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
        .setup_dishka(container)
        .setup_worker()
        .setup_scheduler(
            TaskiqScheduler(broker=taskiq_broker, sources=[ListRedisScheduleSource(url="redis://localhost:6379")])
        )
    )

    fastapi_entrypoint = (
        FastAPIEntrypoint(app=app)
        .setup_dishka(container)
        .setup_observability(ObservabilitySetupper().setup_logging().setup_tracing().setup_metrics())
        .setup_faststream(FastStreamEntrypointComponent(broker=faststream_broker).setup_dishka(container))
        .setup_cors_middleware()
        .setup_mcp(
            FastMCPEntrypointComponent(fastmcp=fastmcp).setup_exception_handlers(
                {
                    Exception: lambda *_: None,
                }
            ),
            path="/mcp",
        )
        .setup_exception_handlers()
    )

    ensure_successful_run(HAO(), [fastapi_entrypoint, taskiq_entrypoint])


def test_fastapi_entrypoint_cors_middleware_setup_with_default_config() -> None:
    """Test entrypoints."""
    app = FastAPI()
    fastapi_entrypoint = FastAPIEntrypoint(app=app).setup_cors_middleware()

    ensure_successful_run(HAO(), [fastapi_entrypoint])


def test_fastapi_entrypoint_cors_middleware_setup_with_custom_config() -> None:
    """Test entrypoints."""
    app = FastAPI()
    fastapi_entrypoint = FastAPIEntrypoint(app=app).setup_cors_middleware(
        cors_config=CORSConfig(allow_origins=["https://example.com"])
    )

    ensure_successful_run(HAO(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_with_container(container: AsyncContainer) -> None:
    """Test entrypoints."""
    app = FastAPI()

    fastapi_entrypoint = FastAPIEntrypoint(app=app).setup_dishka(container).setup_idempotency_middleware()

    ensure_successful_run(HAO(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_with_storage_factory(container: AsyncContainer) -> None:
    """Test entrypoints."""
    app = FastAPI()

    redis = Redis.from_url("redis://localhost:6379")

    async def idempotency_keys_storage_factory() -> AbstractIdempotencyKeysStorage:
        return RedisIdempotencyKeysStorage(redis=redis, ttl=timedelta(seconds=300))

    fastapi_entrypoint = (
        FastAPIEntrypoint(app=app)
        .setup_dishka(container)
        .setup_idempotency_middleware(idempotency_keys_storage_factory=idempotency_keys_storage_factory)
    )

    ensure_successful_run(HAO(), [fastapi_entrypoint])


def test_fastapi_entrypoint_idempotency_middleware_setup_without_container_and_storage_factory() -> None:
    """Test entrypoints."""
    app = FastAPI()

    with pytest.raises(EntrypointInconsistencyError):
        FastAPIEntrypoint(app=app).setup_idempotency_middleware()
