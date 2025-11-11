"""Entrypoints for the application."""

from asyncio import TaskGroup
from collections.abc import Sequence
from typing import Self

from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError


class HAOrchestrator:
    """HAOrchestrator allows you to create a HAO (Humanlessly Autonomously Orchestrated) application.

    Orchestrates multiple entrypoints with proper lifecycle management, including
    startup, execution, and graceful shutdown. Handles errors and ensures proper
    resource cleanup.

    Lifecycle:
        1. Validation - Validates all entrypoints before starting
        2. Startup - Calls startup() on all entrypoints in order
        3. Execution - Runs all entrypoints concurrently using TaskGroup
        4. Shutdown - Calls shutdown() on all entrypoints in reverse order

    Example:
        ```python
        from haolib.entrypoints import HAOrchestrator
        from haolib.entrypoints.fastapi import FastAPIEntrypoint
        from haolib.entrypoints.taskiq import TaskiqEntrypoint

        from haolib.entrypoints.plugins.fastapi import FastAPIDishkaPlugin

        fastapi_entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIDishkaPlugin(container))
        taskiq_entrypoint = TaskiqEntrypoint(broker=broker).setup_worker()

        hao = HAOrchestrator().add_entrypoint(fastapi_entrypoint).add_entrypoint(taskiq_entrypoint)
        await hao.run_entrypoints()
        ```

    Attributes:
        _entrypoints: List of entrypoints to orchestrate.

    """

    def __init__(self) -> None:
        """Initialize the HAOrchestrator."""
        self._entrypoints: list[AbstractEntrypoint] = []

    def add_entrypoint(self, entrypoint: AbstractEntrypoint) -> Self:
        """Add an entrypoint to be orchestrated.

        Args:
            entrypoint: The entrypoint to add. Must implement AbstractEntrypoint protocol.

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If entrypoint validation fails.

        Example:
            ```python
            hao = HAOrchestrator()
            hao.add_entrypoint(fastapi_entrypoint).add_entrypoint(taskiq_entrypoint)
            ```

        """
        self._validate_entrypoint(entrypoint)
        self._entrypoints.append(entrypoint)
        return self

    def _validate_entrypoint(self, entrypoint: AbstractEntrypoint) -> None:
        """Validate an entrypoint before adding it.

        Args:
            entrypoint: The entrypoint to validate.

        Raises:
            EntrypointInconsistencyError: If entrypoint is invalid.

        """

        try:
            entrypoint.validate()
        except Exception as e:
            raise EntrypointInconsistencyError("Entrypoint validation failed") from e

    async def run_entrypoints(self, entrypoints: Sequence[AbstractEntrypoint] | None = None) -> None:  # noqa: C901, PLR0912
        """Run the entrypoints with full lifecycle management.

        Orchestrates entrypoints through their complete lifecycle:
        1. Validates all entrypoints
        2. Calls startup() on all entrypoints
        3. Runs all entrypoints concurrently
        4. Calls shutdown() on all entrypoints (even if errors occur)

        Args:
            entrypoints: Optional list of entrypoints to run. If None, uses
                entrypoints added via add_entrypoint(). If provided, replaces
                any previously added entrypoints.

        Raises:
            EntrypointInconsistencyError: If any entrypoint validation fails.
            ExceptionGroup: If multiple entrypoints fail during execution.

        Example:
            ```python
            # Using add_entrypoint()
            hao = HAOrchestrator()
            hao.add_entrypoint(fastapi_entrypoint).add_entrypoint(taskiq_entrypoint)
            await hao.run_entrypoints()

            # Or passing directly
            await hao.run_entrypoints()
            ```

        """
        # Use provided entrypoints or previously added ones
        if entrypoints is not None:
            self._entrypoints = list(entrypoints)

        if not self._entrypoints:
            return

        # Validate all entrypoints
        for entrypoint in self._entrypoints:
            self._validate_entrypoint(entrypoint)

        # Startup phase - initialize all entrypoints
        startup_errors: list[Exception] = []
        for entrypoint in self._entrypoints:
            try:
                await entrypoint.startup()
            except Exception as e:
                startup_errors.append(e)

        if startup_errors:
            # Shutdown any entrypoints that started successfully
            for entrypoint in reversed(self._entrypoints[: len(startup_errors)]):
                startup_shutdown_errors: list[Exception] = []
                try:
                    await entrypoint.shutdown()
                except Exception as e:
                    startup_shutdown_errors.append(e)

            if startup_shutdown_errors:
                raise ExceptionGroup("Errors during startup and shutdown", startup_errors + startup_shutdown_errors)

            raise ExceptionGroup("Errors during startup", startup_errors)

        # Execution phase - run all entrypoints concurrently
        async with TaskGroup() as task_group:
            for entrypoint in self._entrypoints:
                task_group.create_task(entrypoint.run())

        # Shutdown phase - cleanup all entrypoints (reverse order)
        shutdown_errors: list[Exception] = []
        for entrypoint in reversed(self._entrypoints):
            try:
                await entrypoint.shutdown()
            except Exception as e:
                shutdown_errors.append(e)

        if shutdown_errors:
            raise ExceptionGroup("Errors during shutdown", shutdown_errors)
