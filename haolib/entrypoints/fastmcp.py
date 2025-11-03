"""FastMCP entrypoint."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from fastmcp.server.middleware import MiddlewareContext
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware

from haolib.entrypoints.abstract import AbstractEntrypoint

if TYPE_CHECKING:
    from fastmcp import FastMCP


class FastMCPEntrypointComponent:
    """FastMCP entrypoint component to use in integration with other entrypoints."""

    def __init__(self, fastmcp: FastMCP) -> None:
        """Initialize the FastMCP entrypoint component."""
        self._fastmcp = fastmcp

    def get_app(self) -> FastMCP:
        """Get the FastMCP app."""
        return self._fastmcp

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]]
    ) -> Self:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.

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
    """FastMCP entrypoint."""

    def __init__(self, fastmcp: FastMCP, *args: Any, **kwargs: Any) -> None:
        """Initialize the FastMCP entrypoint.

        Args:
            fastmcp: The FastMCP app.
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """
        self._fastmcp = fastmcp
        self._run_args = args
        self._run_kwargs = kwargs

    def get_app(self) -> FastMCP:
        """Get the FastMCP app."""
        return self._fastmcp

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[[Exception, MiddlewareContext], None]]
    ) -> Self:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.

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

        Args:
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """
        await self._fastmcp.run_async(*self._run_args, **self._run_kwargs)
