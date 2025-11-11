"""Taskiq entrypoint."""

import asyncio
import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from dishka.integrations.taskiq import setup_dishka as setup_dishka_taskiq
from taskiq.api import run_receiver_task, run_scheduler_task

from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError

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
        self.worker_task: asyncio.Task[Any] | None = None
        self.scheduler_task: asyncio.Task[Any] | None = None

    async def startup(self) -> None:
        """Startup."""
        if self._scheduler is None and not self._should_run_worker:
            raise EntrypointInconsistencyError("Both scheduler and worker are not set.")

        await self._broker.startup()

        if self._should_run_worker:
            self.worker_task = asyncio.create_task(
                run_receiver_task(self._broker, *(self._worker_args or ()), **(self._worker_kwargs or {}))
            )

        if self._scheduler is not None:
            self.scheduler_task = asyncio.create_task(
                run_scheduler_task(self._scheduler, *(self._scheduler_args or ()), **(self._scheduler_kwargs or {}))
            )

    async def cancel_worker_task(self) -> None:
        """Cancel worker task."""
        if self.worker_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                self.worker_task.cancel()

                await self.worker_task

            self.worker_task = None

    async def cancel_scheduler_task(self) -> None:
        """Cancel scheduler task."""
        if self.scheduler_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                self.scheduler_task.cancel()

                await self.scheduler_task

            self.scheduler_task = None

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


class TaskiqEntrypoint(AbstractEntrypoint):
    """Taskiq entrypoint implementation.

    Provides a builder-pattern interface for configuring and running Taskiq
    task queue workers and schedulers with features like dependency injection
    and exception handling.

    Example:
        ```python
        from taskiq import AsyncBroker
        from taskiq_redis import RedisStreamBroker

        broker = RedisStreamBroker(url="redis://localhost:6379")
        entrypoint = (
            TaskiqEntrypoint(broker=broker)
            .setup_dishka(container)
            .setup_worker()
            .setup_scheduler(scheduler)
        )

        await entrypoint.run()
        ```

    Attributes:
        _broker: The Taskiq async broker instance.
        _should_run_worker: Whether to run the worker task.
        _scheduler: Optional Taskiq scheduler instance.
        _worker_args: Positional arguments for worker task.
        _worker_kwargs: Keyword arguments for worker task.
        _scheduler_args: Positional arguments for scheduler task.
        _scheduler_kwargs: Keyword arguments for scheduler task.
        _exception_handlers: Optional exception handlers dictionary.
        _worker: Optional TaskiqEntrypointWorker instance (created during startup).

    """

    def __init__(self, broker: AsyncBroker) -> None:
        """Initialize the Taskiq entrypoint.

        Args:
            broker: The Taskiq async broker instance.

        Raises:
            EntrypointInconsistencyError: If broker is None.

        """
        self._broker = broker
        self._should_run_worker = False
        self._scheduler: TaskiqScheduler | None = None
        self._worker_args: Sequence[Any] | None = None
        self._worker_kwargs: Mapping[str, Any] | None = None
        self._scheduler_args: Sequence[Any] | None = None
        self._scheduler_kwargs: Mapping[str, Any] | None = None
        self._exception_handlers: dict[type[Exception], Callable[[Exception], None]] | None = None
        self._worker: TaskiqEntrypointWorker | None = None

    def validate(self) -> None:
        """Validate Taskiq entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Specifically checks:
        - At least one of worker or scheduler is configured (configuration consistency)

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        # Broker is guaranteed by type system (required in __init__)
        # Check configuration consistency: must have worker or scheduler
        if not self._should_run_worker and self._scheduler is None:
            raise EntrypointInconsistencyError("Taskiq entrypoint must have either worker or scheduler configured.")

    async def startup(self) -> None:
        """Startup the Taskiq entrypoint.

        Creates and initializes the TaskiqEntrypointWorker. This method is called
        before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        self._worker = TaskiqEntrypointWorker(
            self._broker,
            self._scheduler,
            self._should_run_worker,
            self._worker_args,
            self._worker_kwargs,
            self._scheduler_args,
            self._scheduler_kwargs,
        )

        await self._worker.startup()

    async def shutdown(self) -> None:
        """Shutdown the Taskiq entrypoint.

        Cleans up resources and stops the worker and scheduler. This method
        is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        if self._worker is not None:
            await self._worker.shutdown()
            self._worker = None

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup Dishka dependency injection.

        Configures Dishka to work with the Taskiq broker, enabling dependency
        injection in task handlers.

        Args:
            container: The Dishka async container instance.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from dishka import make_async_container

            container = make_async_container(...)
            entrypoint.setup_dishka(container)
            ```

        """
        setup_dishka_taskiq(container, self._broker)

        return self

    def setup_worker(self, *args: Any, **kwargs: Any) -> Self:
        """Setup worker task.

        Configures the entrypoint to run a worker that processes tasks from
        the broker queue.

        Args:
            *args: The arguments to pass to the run_receiver_task function.
            **kwargs: The keyword arguments to pass to the run_receiver_task function.

        Returns:
            Self for method chaining.

        Example:
            ```python
            entrypoint.setup_worker(max_async_tasks=10)
            ```

        """
        self._should_run_worker = True

        self._worker_args = args
        self._worker_kwargs = kwargs

        return self

    def setup_scheduler(self, taskiq_scheduler: TaskiqScheduler, *args: Any, **kwargs: Any) -> Self:
        """Setup scheduler task.

        Configures the entrypoint to run a scheduler that executes scheduled tasks.

        Args:
            taskiq_scheduler: The Taskiq scheduler instance.
            *args: The arguments to pass to the run_scheduler_task function.
            **kwargs: The keyword arguments to pass to the run_scheduler_task function.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from taskiq import TaskiqScheduler
            from taskiq_redis import ListRedisScheduleSource

            scheduler = TaskiqScheduler(
                broker=broker,
                sources=[ListRedisScheduleSource(url="redis://localhost:6379")]
            )
            entrypoint.setup_scheduler(scheduler)
            ```

        """
        self._scheduler = taskiq_scheduler
        self._scheduler_args = args
        self._scheduler_kwargs = kwargs

        return self

    def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[[Exception], None]]) -> Self:
        """Setup exception handlers.

        Configures exception handlers for worker and scheduler tasks.
        Handlers are called when exceptions occur during task execution.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.

        Returns:
            Self for method chaining.

        Example:
            ```python
            def handle_error(exc: Exception):
                logger.error(f"Task error: {exc}")

            entrypoint.setup_exception_handlers({
                ValueError: handle_error
            })
            ```

        """
        self._exception_handlers = exception_handlers

        return self

    async def run(self) -> None:
        """Run the Taskiq entrypoint.

        Waits for the worker and/or scheduler tasks to complete. This method
        runs indefinitely until cancelled or an error occurs. The tasks are
        already started during startup().

        Raises:
            EntrypointInconsistencyError: If entrypoint was not started via startup().
            Exception: Any exception that occurs during execution (unless handled).

        """
        if self._worker is None:
            raise EntrypointInconsistencyError("Taskiq entrypoint must be started via startup() before run()")

        # Collect tasks to wait on
        tasks_to_wait: list[asyncio.Task[Any]] = []
        if self._worker.worker_task is not None:
            tasks_to_wait.append(self._worker.worker_task)
        if self._worker.scheduler_task is not None:
            tasks_to_wait.append(self._worker.scheduler_task)

        if not tasks_to_wait:
            raise EntrypointInconsistencyError("No tasks to run in Taskiq entrypoint")

        if self._exception_handlers is None:
            # Run without exception handling - wait for tasks
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks_to_wait)
        else:
            # Run with exception handling
            try:
                await asyncio.gather(*tasks_to_wait)
            except Exception as exc:
                exception_handler = next(
                    (handler for exc_type, handler in self._exception_handlers.items() if isinstance(exc, exc_type)),
                    None,
                )
                if exception_handler is None:
                    raise

                exception_handler(exc)
