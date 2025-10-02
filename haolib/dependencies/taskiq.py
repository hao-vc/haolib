"""Taskiq dependencies."""

from collections.abc import AsyncGenerator

from dishka import Provider, Scope, provide
from taskiq import AsyncBroker, ScheduleSource

from haolib.background.taskiq import TaskiqAsyncWorkerWithScheduler
from haolib.configs.taskiq import TaskiqConfig


class TaskiqProvider(Provider):
    """Taskiq provider."""

    @provide(scope=Scope.APP)
    async def taskiq_async_worker_with_scheduler(
        self, taskiq_broker: AsyncBroker, taskiq_sources: list[ScheduleSource], taskiq_config: TaskiqConfig
    ) -> AsyncGenerator[TaskiqAsyncWorkerWithScheduler]:
        """Get taskiq async worker with scheduler."""
        async with TaskiqAsyncWorkerWithScheduler(taskiq_broker, taskiq_sources, taskiq_config) as worker:
            yield worker
