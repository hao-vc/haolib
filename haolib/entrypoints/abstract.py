"""Base entrypoint."""

from types import TracebackType
from typing import Self

from haolib.components.abstract import AbstractComponent, ComponentInconsistencyError
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin, AbstractEntrypointPluginPreset


class AbstractEntrypoint(AbstractComponent[AbstractEntrypointPlugin, AbstractEntrypointPluginPreset]):
    """Abstract entrypoint protocol.

    Defines the interface that all entrypoints must implement. Entrypoints are
    runnable components that can be orchestrated together using the HAO class.

    Lifecycle:
        1. startup() - Called before run() to validate configuration and initialize resources
        2. run() - Main execution method that runs the entrypoint
        3. shutdown() - Called after run() completes or is cancelled to cleanup resources

    Example:
        ```python
        class MyEntrypoint:
            async def startup(self) -> None:
                # Validate configuration, initialize resources
                pass

            async def run(self) -> None:
                # Main execution logic
                pass

            async def shutdown(self) -> None:
                # Cleanup resources
                pass
        ```

    """

    async def startup(self) -> None:
        """Startup the FastAPI entrypoint.

        Initializes the uvicorn server and prepares it for execution.
        Calls startup hooks for all plugins.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """

    async def shutdown(self) -> None:
        """Shutdown the FastAPI entrypoint.

        Cleans up resources and stops the server. Calls shutdown hooks for all plugins.
        This method is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """

    async def run(self) -> None:
        """Run the entrypoint.

        This method is called after startup() and should be overridden by subclasses.
        """

    async def __aenter__(self) -> Self:
        """Enter context manager."""
        await self.startup()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit context manager."""
        await self.shutdown()


class EntrypointInconsistencyError(ComponentInconsistencyError):
    """Entrypoint inconsistency error.

    Raised when an entrypoint or component is misconfigured, has missing
    dependencies, or is in an inconsistent state.

    Example:
        ```python
        raise EntrypointInconsistencyError(
            "Idempotency middleware requires Dishka container to be configured"
        )
        ```

    """

    def __init__(self, message: str) -> None:
        """Initialize the entrypoint inconsistency error.

        Args:
            message: Error message describing the inconsistency.

        """
        super().__init__(message)
