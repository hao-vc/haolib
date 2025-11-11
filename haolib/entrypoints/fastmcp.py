"""FastMCP entrypoint."""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from fastmcp.server.middleware import MiddlewareContext
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware

from haolib.entrypoints.abstract import (
    AbstractEntrypoint,
    AbstractEntrypointComponent,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from fastmcp import FastMCP


class FastMCPEntrypointComponent(AbstractEntrypointComponent):
    """FastMCP entrypoint component for integration with other entrypoints.

    This component provides a standardized interface for integrating FastMCP
    applications with other entrypoint types (e.g., FastAPI). It encapsulates
    FastMCP configuration and provides integration hooks.

    Example:
        ```python
        from fastmcp import FastMCP

        fastmcp = FastMCP()
        component = FastMCPEntrypointComponent(fastmcp=fastmcp).setup_exception_handlers({
            ValueError: lambda exc, ctx: logger.error(f"Value error: {exc}")
        })

        fastapi_entrypoint.setup_mcp(component, path="/mcp")
        ```

    Attributes:
        _fastmcp: The FastMCP application instance.

    """

    def __init__(self, fastmcp: FastMCP) -> None:
        """Initialize the FastMCP entrypoint component.

        Args:
            fastmcp: The FastMCP application instance to wrap.

        """
        self._fastmcp = fastmcp

    def validate(self) -> None:
        """Validate FastMCP component configuration.

        Validates that the component is properly configured and ready for use.

        """
        # FastMCP app is guaranteed by type system (required in __init__)
        # No validation needed

    def get_app(self) -> FastMCP:
        """Get the FastMCP application instance.

        Returns:
            The FastMCP application instance.

        """
        return self._fastmcp

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]]
    ) -> Self:
        """Setup exception handlers for the FastMCP app.

        Configures exception handling middleware for the FastMCP application.
        Handlers are called when exceptions occur during request processing.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
                Handlers receive the exception and middleware context.

        Returns:
            Self for method chaining.

        Example:
            ```python
            def handle_error(exc: Exception, ctx: MiddlewareContext):
                logger.error(f"Error in {ctx}: {exc}")

            component.setup_exception_handlers({
                ValueError: handle_error
            })
            ```

        """

        def error_callback(exc: Exception, context: MiddlewareContext) -> None:
            """Error callback."""
            exception_handler = next(
                (handler for exc_type, handler in exception_handlers.items() if isinstance(exc, exc_type)), None
            )
            if exception_handler is None:
                raise exc

            exception_handler(exc, context)

        self._fastmcp.add_middleware(ErrorHandlingMiddleware(error_callback=error_callback))

        return self


class FastMCPEntrypoint(AbstractEntrypoint):
    """FastMCP entrypoint implementation.

    Provides a builder-pattern interface for configuring and running FastMCP
    applications with features like exception handling.

    Example:
        ```python
        from fastmcp import FastMCP

        fastmcp = FastMCP()
        entrypoint = (
            FastMCPEntrypoint(fastmcp=fastmcp)
            .setup_exception_handlers({
                ValueError: lambda exc, ctx: logger.error(f"Error: {exc}")
            })
        )

        await entrypoint.run()
        ```

    Attributes:
        _fastmcp: The FastMCP application instance.
        _run_args: Positional arguments to pass to fastmcp.run_async().
        _run_kwargs: Keyword arguments to pass to fastmcp.run_async().

    """

    def __init__(self, fastmcp: FastMCP, *args: Any, **kwargs: Any) -> None:
        """Initialize the FastMCP entrypoint.

        Args:
            fastmcp: The FastMCP application instance.
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """

        self._fastmcp = fastmcp
        self._run_args = args
        self._run_kwargs = kwargs

    def validate(self) -> None:
        """Validate FastMCP entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available.

        """
        # FastMCP app is guaranteed by type system (required in __init__)
        # No validation needed

    async def startup(self) -> None:
        """Startup the FastMCP entrypoint.

        Prepares the FastMCP application for execution. This method is called
        before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        logger.info("FastMCP entrypoint starting")

    async def shutdown(self) -> None:
        """Shutdown the FastMCP entrypoint.

        Cleans up resources and stops the FastMCP application. This method
        is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        if self._fastmcp is not None:
            logger.info("Shutting down FastMCP entrypoint")
            # FastMCP handles cleanup automatically when run_async() is cancelled
            # Additional cleanup can be added here if needed

    def get_app(self) -> FastMCP:
        """Get the FastMCP application instance.

        Returns:
            The FastMCP application instance.

        """
        return self._fastmcp

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]]
    ) -> Self:
        """Setup exception handlers for the FastMCP app.

        Configures exception handling middleware for request processing.
        Handlers are called when exceptions occur during request handling.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
                Handlers receive the exception and middleware context.

        Returns:
            Self for method chaining.

        Example:
            ```python
            def handle_error(exc: Exception, ctx: MiddlewareContext):
                logger.error(f"Error in {ctx}: {exc}")

            entrypoint.setup_exception_handlers({
                ValueError: handle_error
            })
            ```

        """

        def error_callback(exc: Exception, context: MiddlewareContext) -> None:
            """Error callback."""
            exception_handler = next(
                (handler for exc_type, handler in exception_handlers.items() if isinstance(exc, exc_type)), None
            )
            if exception_handler is None:
                raise exc

            exception_handler(exc, context)

        self._fastmcp.add_middleware(ErrorHandlingMiddleware(error_callback=error_callback))

        return self

    async def run(self) -> None:
        """Run the FastMCP entrypoint.

        Starts the FastMCP application and begins processing requests.
        This method runs indefinitely until cancelled or an error occurs.

        Raises:
            Exception: Any exception that occurs during execution.

        """
        await self._fastmcp.run_async(*self._run_args, **self._run_kwargs)
