"""FastStream exceptions plugin."""

from collections.abc import Callable
from typing import Any

from faststream.middlewares.exception import ExceptionMiddleware

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
)
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin


class FastStreamExceptionHandlersPlugin(AbstractEntrypointPlugin[FastStreamEntrypoint]):
    """Plugin for adding exception handlers to FastStream entrypoints.

    Example:
        ```python
        exception_handlers = {ValueError: lambda exc: logger.error(f"Error: {exc}")}
        entrypoint = FastStreamEntrypoint(app=app).use_plugin(
            FastStreamExceptionHandlersPlugin(exception_handlers)
        )
        ```

    """

    def __init__(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> None:
        """Initialize the FastStream exception handlers plugin.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.

        """
        self._exception_handlers = exception_handlers

    def apply(self, component: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply exception handlers to the entrypoint.

        Args:
            component: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        """
        app = component.get_app()
        if app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        app.broker.add_middleware(ExceptionMiddleware(publish_handlers=self._exception_handlers))
        return component

    def validate(self, component: FastStreamEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            component: The entrypoint to validate against.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        """
        app = component.get_app()
        if app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")
