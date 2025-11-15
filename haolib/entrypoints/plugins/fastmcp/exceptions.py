"""FastMCP exceptions plugin."""

from collections.abc import Callable

from fastmcp.server.middleware import MiddlewareContext
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware

from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin


class FastMCPExceptionHandlersPlugin(AbstractEntrypointPlugin[FastMCPEntrypoint]):
    """Plugin for adding exception handlers to FastMCP entrypoints.

    Example:
        ```python
        exception_handlers = {
            ValueError: lambda exc, ctx: logger.error(f"Error: {exc}")
        }
        entrypoint = FastMCPEntrypoint(fastmcp=fastmcp).use_plugin(
            FastMCPExceptionHandlersPlugin(exception_handlers)
        )
        ```

    """

    def __init__(
        self, exception_handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]]
    ) -> None:
        """Initialize the FastMCP exception handlers plugin.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.

        """
        self._exception_handlers = exception_handlers

    def apply(self, component: FastMCPEntrypoint) -> FastMCPEntrypoint:
        """Apply exception handlers to the entrypoint.

        Args:
            component: The FastMCP entrypoint to configure.

        Returns:
            The configured entrypoint.

        """

        def error_callback(exc: Exception, context: MiddlewareContext) -> None:
            """Error callback."""
            exception_handler = next(
                (handler for exc_type, handler in self._exception_handlers.items() if isinstance(exc, exc_type)), None
            )
            if exception_handler is None:
                raise exc

            exception_handler(exc, context)

        component.get_app().add_middleware(ErrorHandlingMiddleware(error_callback=error_callback))
        return component
