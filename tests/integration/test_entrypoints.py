"""Test entrypoints."""

import asyncio

import pytest
from dishka import AsyncContainer
from fastapi import FastAPI
from fastmcp import FastMCP
from faststream.confluent import KafkaBroker
from taskiq import TaskiqScheduler
from taskiq_redis import ListRedisScheduleSource, RedisStreamBroker

from haolib.entrypoints import HAO
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.faststream import FastStreamEntrypoint
from haolib.entrypoints.taskiq import TaskiqEntrypoint


@pytest.mark.asyncio
async def test_fastapi_entrypoint(container: AsyncContainer) -> None:
    """Test entrypoints."""
    hao = HAO()
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
    faststream_entrypoint = FastStreamEntrypoint(broker=faststream_broker).setup_dishka(container)
    fastmcp_entrypoint = FastMCPEntrypoint(fastmcp=fastmcp)
    fastapi_entrypoint = FastAPIEntrypoint(app=app).setup_dishka(container).setup_observability()
    fastapi_entrypoint.setup_faststream(faststream_entrypoint)
    fastapi_entrypoint.setup_mcp(fastmcp_entrypoint, path="/mcp")

    task = asyncio.create_task(hao.run_entrypoints([fastapi_entrypoint, taskiq_entrypoint]))

    await asyncio.sleep(1)
    task.cancel()
