"""Taskiq entrypoint."""

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any, Self

from dishka.integrations.taskiq import setup_dishka as setup_dishka_taskiq
from taskiq.api import run_receiver_task, run_scheduler_task

from haolib.entrypoints.base import Entrypoint
from haolib.entrypoints.exceptions import EntrypointsInconsistencyError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from types import TracebackType

    from dishka import AsyncContainer
    from taskiq import AsyncBroker, TaskiqScheduler


class TaskiqEntrypointWorker:
    """Taskiq entrypoint worker."""

    def __init__(
        self,
        broker: AsyncBroker,
        scheduler: TaskiqScheduler | None = None,
        should_run_worker: bool = True,
        worker_args: Sequence[Any] | None = None,
        worker_kwargs: Mapping[str, Any] | None = None,
        scheduler_args: Sequence[Any] | None = None,
        scheduler_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize the Taskiq entrypoint worker."""
        self._broker = broker
        self._scheduler = scheduler
        self._should_run_worker = should_run_worker
        self._worker_args = worker_args
        self._worker_kwargs = worker_kwargs
        self._scheduler_args = scheduler_args
        self._scheduler_kwargs = scheduler_kwargs

    async def startup(self) -> None:
        """Startup."""
        if self._scheduler is None and not self._should_run_worker:
            raise EntrypointsInconsistencyError("Both scheduler and worker are not set.")

        await self._broker.startup()

        if self._should_run_worker:
            self._worker_task = asyncio.create_task(
                run_receiver_task(self._broker, *(self._worker_args or ()), **(self._worker_kwargs or {}))
            )

        if self._scheduler is not None:
            self._scheduler_task = asyncio.create_task(
                run_scheduler_task(self._scheduler, *(self._scheduler_args or ()), **(self._scheduler_kwargs or {}))
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


class TaskiqEntrypoint(Entrypoint):
    """One threaded taskiq entrypoint."""

    def __init__(self, broker: AsyncBroker) -> None:
        """Initialize the One threaded taskiq entrypoint."""
        self._broker = broker
        self._should_run_worker = False
        self._scheduler: TaskiqScheduler | None = None
        self._worker_args: Sequence[Any] | None = None
        self._worker_kwargs: Mapping[str, Any] | None = None
        self._scheduler_args: Sequence[Any] | None = None
        self._scheduler_kwargs: Mapping[str, Any] | None = None

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup dishka."""
        setup_dishka_taskiq(container, self._broker)

        return self

    def setup_worker(self, *args: Any, **kwargs: Any) -> Self:
        """Setup worker.

        Args:
            *args: The arguments to pass to the run_receiver_task function.
            **kwargs: The keyword arguments to pass to the run_receiver_task function.

        """
        self._should_run_worker = True

        self._worker_args = args
        self._worker_kwargs = kwargs

        return self

    def setup_scheduler(self, taskiq_scheduler: TaskiqScheduler, *args: Any, **kwargs: Any) -> Self:
        """Setup scheduler.

        Args:
            taskiq_scheduler: The taskiq scheduler.
            *args: The arguments to pass to the run_scheduler_task function.
            **kwargs: The keyword arguments to pass to the run_scheduler_task function.

        """
        self._scheduler = taskiq_scheduler
        self._scheduler_args = args
        self._scheduler_kwargs = kwargs

        return self

    async def run(self) -> None:
        """Run the taskiq entrypoint."""

        async with TaskiqEntrypointWorker(
            self._broker,
            self._scheduler,
            self._should_run_worker,
            self._worker_args,
            self._worker_kwargs,
            self._scheduler_args,
            self._scheduler_kwargs,
        ):
            pass
