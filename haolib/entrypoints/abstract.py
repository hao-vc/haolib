"""Base entrypoint."""

from typing import Protocol


class AbstractEntrypoint(Protocol):
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
        """Startup the entrypoint.

        Called before run() to validate configuration, initialize resources,
        and perform any necessary setup. This method should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        ...

    async def run(self) -> None:
        """Run the entrypoint.

        Main execution method that runs the entrypoint. This method should
        run indefinitely until cancelled or an error occurs.

        Raises:
            Exception: Any exception that occurs during execution.

        """
        ...

    async def shutdown(self) -> None:
        """Shutdown the entrypoint.

        Called after run() completes or is cancelled to cleanup resources,
        close connections, and perform any necessary teardown. This method
        should be idempotent and safe to call multiple times.

        """
        ...

    def validate(self) -> None:
        """Validate entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. This method should be called before startup().

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        ...


class EntrypointInconsistencyError(Exception):
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
