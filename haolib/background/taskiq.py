"""Taskiq worker."""

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Self

from taskiq import AsyncBroker, ScheduleSource, TaskiqScheduler
from taskiq.api import run_receiver_task, run_scheduler_task

from haolib.configs.taskiq import TaskiqConfig

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from types import TracebackType


class TaskiqAsyncWorkerWithScheduler:
    """Taskiq worker."""

    def __init__(
        self, taskiq_broker: AsyncBroker, taskiq_sources: list[ScheduleSource], taskiq_config: TaskiqConfig
    ) -> None:
        """Initialize taskiq worker."""

        self._broker = taskiq_broker

        self._taskiq_sources = taskiq_sources

        self._taskiq_config = taskiq_config

        self._scheduler = TaskiqScheduler(broker=self._broker, sources=self._taskiq_sources)

    async def startup(self) -> None:
        """Startup."""
        await self._broker.startup()

        self._worker_task = asyncio.create_task(
            run_receiver_task(self._broker, ack_time=self._taskiq_config.worker.ack_time)
        )

        self._scheduler_task = asyncio.create_task(
            run_scheduler_task(
                scheduler=self._scheduler,
                interval=self._taskiq_config.scheduler.schedule_interval,
            )
        )

    async def cancel_worker_task(self) -> None:
        """Cancel worker task."""
        with contextlib.suppress(asyncio.CancelledError):
            self._worker_task.cancel()
            await self._worker_task

    async def cancel_scheduler_task(self) -> None:
        """Cancel scheduler task."""
        with contextlib.suppress(asyncio.CancelledError):
            self._scheduler_task.cancel()
            await self._scheduler_task

    async def shutdown(self) -> None:
        """Shutdown."""

        await self.cancel_worker_task()

        await self.cancel_scheduler_task()

        await self._broker.shutdown()

    async def __aenter__(self) -> Self:
        """Enter context manager."""
        await self.startup()

        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit context manager."""

        await self.shutdown()
