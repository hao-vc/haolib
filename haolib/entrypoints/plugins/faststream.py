"""FastStream entrypoint plugins."""

from collections.abc import Callable
from typing import Any

from dishka import AsyncContainer
from dishka.integrations.faststream import setup_dishka
from faststream.middlewares.exception import ExceptionMiddleware

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
    FastStreamEntrypointBrokerType,
    _setup_observability_to_broker,
)
from haolib.observability.setupper import ObservabilitySetupper


class FastStreamDishkaPlugin:
    """Plugin for adding Dishka dependency injection to FastStream entrypoints.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        entrypoint = FastStreamEntrypoint(app=app).use_plugin(FastStreamDishkaPlugin(container))
        ```

    """

    def __init__(self, container: AsyncContainer) -> None:
        """Initialize the FastStream Dishka plugin.

        Args:
            container: The Dishka async container instance.

        """
        self._container = container

    def apply(self, entrypoint: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply Dishka to the entrypoint.

        Args:
            entrypoint: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        setup_dishka(container=self._container, app=entrypoint.get_app(), finalize_container=False)
        return entrypoint

    def validate(self, entrypoint: FastStreamEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            entrypoint: The entrypoint to validate against.

        """
        # No validation needed - container is required in __init__

    async def on_startup(self, entrypoint: FastStreamEntrypoint) -> None:
        """Startup hook.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        # No startup logic needed

    async def on_shutdown(self, entrypoint: FastStreamEntrypoint) -> None:
        """Shutdown hook.

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        # No shutdown logic needed


class FastStreamExceptionHandlersPlugin:
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

    def apply(self, entrypoint: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply exception handlers to the entrypoint.

        Args:
            entrypoint: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        """
        app = entrypoint.get_app()
        if app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        app.broker.add_middleware(ExceptionMiddleware(publish_handlers=self._exception_handlers))
        return entrypoint

    def validate(self, entrypoint: FastStreamEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            entrypoint: The entrypoint to validate against.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        """
        app = entrypoint.get_app()
        if app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

    async def on_startup(self, entrypoint: FastStreamEntrypoint) -> None:
        """Startup hook.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        # No startup logic needed

    async def on_shutdown(self, entrypoint: FastStreamEntrypoint) -> None:
        """Shutdown hook.

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        # No shutdown logic needed


class FastStreamObservabilityPlugin:
    """Plugin for adding observability to FastStream entrypoints.

    Example:
        ```python
        from haolib.observability.setupper import ObservabilitySetupper
        from haolib.entrypoints.faststream import FastStreamEntrypointBrokerType

        observability = ObservabilitySetupper().setup_logging()
        entrypoint = FastStreamEntrypoint(app=app).use_plugin(
            FastStreamObservabilityPlugin(observability, FastStreamEntrypointBrokerType.CONFLUENT)
        )
        ```

    """

    def __init__(self, observability: ObservabilitySetupper, broker_type: FastStreamEntrypointBrokerType) -> None:
        """Initialize the FastStream observability plugin.

        Args:
            observability: The observability setupper.
            broker_type: The type of the broker (AIOKAFKA, CONFLUENT, NATS, RABBITMQ, REDIS).

        """
        self._observability = observability
        self._broker_type = broker_type

    def apply(self, entrypoint: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply observability to the entrypoint.

        Args:
            entrypoint: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        broker = entrypoint.get_app().broker
        if broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")

        _setup_observability_to_broker(self._observability, self._broker_type, broker)
        return entrypoint

    def validate(self, entrypoint: FastStreamEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            entrypoint: The entrypoint to validate against.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        """
        app = entrypoint.get_app()
        if app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")

    async def on_startup(self, entrypoint: FastStreamEntrypoint) -> None:
        """Startup hook.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        # No startup logic needed

    async def on_shutdown(self, entrypoint: FastStreamEntrypoint) -> None:
        """Shutdown hook.

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        # No shutdown logic needed
