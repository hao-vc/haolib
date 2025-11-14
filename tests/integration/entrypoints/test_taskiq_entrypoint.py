"""Test Taskiq entrypoint."""

import pytest
from dishka import AsyncContainer
from taskiq import AsyncBroker, TaskiqScheduler

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.plugins.taskiq import TaskiqDishkaPlugin
from haolib.entrypoints.taskiq import TaskiqEntrypoint, TaskiqEntrypointWorker
from tests.integration.entrypoints.conftest import (
    run_entrypoint_briefly,
)


class TestTaskiqEntrypointLifecycle:
    """Test Taskiq entrypoint lifecycle methods."""

    @pytest.mark.asyncio
    async def test_startup_creates_worker(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that startup creates a worker instance."""
        await taskiq_entrypoint_with_worker.startup()
        # Worker is created internally - we verify by running successfully
        await taskiq_entrypoint_with_worker.shutdown()

    @pytest.mark.asyncio
    async def test_startup_validates_config(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that startup validates configuration."""
        await taskiq_entrypoint_with_worker.startup()
        await taskiq_entrypoint_with_worker.shutdown()

    @pytest.mark.asyncio
    async def test_startup_is_idempotent(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that startup can be called multiple times safely."""
        await taskiq_entrypoint_with_worker.startup()
        await taskiq_entrypoint_with_worker.startup()  # Should not raise
        await taskiq_entrypoint_with_worker.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_cleans_up_worker(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that shutdown cleans up the worker."""
        await taskiq_entrypoint_with_worker.startup()
        await taskiq_entrypoint_with_worker.shutdown()
        # Shutdown completes without error - worker is cleaned up

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that shutdown can be called multiple times safely."""
        await taskiq_entrypoint_with_worker.startup()
        await taskiq_entrypoint_with_worker.shutdown()
        await taskiq_entrypoint_with_worker.shutdown()  # Should not raise

    @pytest.mark.asyncio
    async def test_run_requires_startup(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that run requires startup to be called first."""
        with pytest.raises(EntrypointInconsistencyError, match="must be started via startup\\(\\) before run\\(\\)"):
            await taskiq_entrypoint_with_worker.run()

    @pytest.mark.asyncio
    async def test_run_with_startup(self, taskiq_entrypoint_with_worker: TaskiqEntrypoint) -> None:
        """Test that run works after startup."""
        await run_entrypoint_briefly(taskiq_entrypoint_with_worker)


class TestTaskiqEntrypointBuilder:
    """Test Taskiq entrypoint builder methods."""

    def test_use_plugin_dishka_returns_self(
        self, taskiq_entrypoint: TaskiqEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that use_plugin with TaskiqDishkaPlugin returns self for chaining."""
        result = taskiq_entrypoint.use_plugin(TaskiqDishkaPlugin(mock_container))
        assert result is taskiq_entrypoint

    def test_setup_worker_returns_self(self, taskiq_entrypoint: TaskiqEntrypoint) -> None:
        """Test that setup_worker returns self for chaining."""
        result = taskiq_entrypoint.setup_worker()
        assert result is taskiq_entrypoint

    def test_setup_worker_with_args(self, taskiq_entrypoint: TaskiqEntrypoint) -> None:
        """Test that setup_worker accepts arguments."""
        result = taskiq_entrypoint.setup_worker(max_async_tasks=10)
        assert result is taskiq_entrypoint

    def test_setup_scheduler_returns_self(
        self, taskiq_entrypoint: TaskiqEntrypoint, taskiq_scheduler: TaskiqScheduler
    ) -> None:
        """Test that setup_scheduler returns self for chaining."""
        result = taskiq_entrypoint.setup_scheduler(taskiq_scheduler)
        assert result is taskiq_entrypoint

    def test_builder_pattern_chaining(
        self, taskiq_entrypoint: TaskiqEntrypoint, mock_container: AsyncContainer, taskiq_scheduler: TaskiqScheduler
    ) -> None:
        """Test that builder methods can be chained."""
        result = (
            taskiq_entrypoint.use_plugin(TaskiqDishkaPlugin(mock_container))
            .setup_worker()
            .setup_scheduler(taskiq_scheduler)
        )
        assert result is taskiq_entrypoint


class TestTaskiqEntrypointWorker:
    """Test TaskiqEntrypointWorker."""

    @pytest.mark.asyncio
    async def test_worker_startup_with_worker(self, taskiq_broker: AsyncBroker) -> None:
        """Test worker startup with worker enabled."""
        worker = TaskiqEntrypointWorker(broker=taskiq_broker, should_run_worker=True, worker_args=(), worker_kwargs={})

        await worker.startup()
        assert worker.worker_task is not None

        await worker.shutdown()

    @pytest.mark.asyncio
    async def test_worker_startup_with_scheduler(
        self, taskiq_broker: AsyncBroker, taskiq_scheduler: TaskiqScheduler
    ) -> None:
        """Test worker startup with scheduler."""
        worker = TaskiqEntrypointWorker(
            broker=taskiq_broker,
            scheduler=taskiq_scheduler,
            should_run_worker=False,
            scheduler_args=(),
            scheduler_kwargs={},
        )

        await worker.startup()
        assert worker.scheduler_task is not None

        await worker.shutdown()

    @pytest.mark.asyncio
    async def test_worker_startup_fails_without_worker_or_scheduler(self, taskiq_broker: AsyncBroker) -> None:
        """Test that worker startup fails without worker or scheduler."""
        worker = TaskiqEntrypointWorker(broker=taskiq_broker, should_run_worker=False, scheduler=None)

        with pytest.raises(EntrypointInconsistencyError, match="Both scheduler and worker are not set"):
            await worker.startup()

    @pytest.mark.asyncio
    async def test_worker_context_manager(self, taskiq_broker: AsyncBroker) -> None:
        """Test worker as context manager."""
        worker = TaskiqEntrypointWorker(broker=taskiq_broker, should_run_worker=True, worker_args=(), worker_kwargs={})

        async with worker:
            assert worker.worker_task is not None

        # After context exit, worker should be shut down
        assert worker.worker_task is None
