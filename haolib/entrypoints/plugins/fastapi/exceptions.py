"""FastAPI exceptions plugin."""

from collections.abc import Callable
from typing import Any

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.exceptions.base.fastapi import FastAPIBaseException
from haolib.exceptions.handlers.fastapi import fastapi_base_exception_handler, fastapi_unknown_exception_handler


class FastAPIExceptionHandlersPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding exception handlers to FastAPI entrypoints.

    Example:
        ```python
        exception_handlers = {
            ValueError: lambda request, exc: JSONResponse(status_code=400, content={"error": str(exc)})
        }
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIExceptionHandlersPlugin(exception_handlers))
        ```

    """

    def __init__(self, exception_handlers: dict[type[Exception], Callable[..., Any]] | None = None) -> None:
        """Initialize the exception handlers plugin.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
                If None, uses default handlers for Exception and FastAPIBaseException.

        """
        if exception_handlers is None:
            exception_handlers = {
                Exception: fastapi_unknown_exception_handler,
                FastAPIBaseException: fastapi_base_exception_handler,
            }
        self._exception_handlers = exception_handlers

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply exception handlers to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        for exception, handler in self._exception_handlers.items():
            component.get_app().add_exception_handler(exception, handler)
        return component
