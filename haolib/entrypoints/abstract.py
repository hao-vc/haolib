"""Base entrypoint."""

from collections.abc import Callable
from typing import Any, Protocol, Self


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

            def setup_exception_handlers(self, ...) -> Self:
                # Optional: setup exception handlers
                return self
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

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[..., Any]], *args: Any, **kwargs: Any
    ) -> Self:
        """Setup exception handlers.

        Optional method to configure exception handling for the entrypoint.
        Not all entrypoints require exception handlers.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
            *args: Additional positional arguments to pass to exception handlers.
            **kwargs: Additional keyword arguments to pass to exception handlers.

        Returns:
            Self for method chaining.

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


class AbstractEntrypointComponent(Protocol):
    """Abstract entrypoint component protocol.

    Components are reusable pieces that can be integrated with entrypoints.
    They provide a standardized interface for cross-entrypoint integration.

    Example:
        ```python
        class MyComponent:
            def validate(self) -> None:
                # Validate component configuration
                pass

            def get_integration_hook(self, target_type: type) -> Callable | None:
                # Return integration function for specific entrypoint type
                pass
        ```

    """

    def validate(self) -> None:
        """Validate component configuration.

        Validates that the component is properly configured and ready for use.
        This method should be called before integration.

        Raises:
            EntrypointInconsistencyError: If component configuration is invalid.

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
