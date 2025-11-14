"""FastStream entrypoint plugins."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dishka import AsyncContainer
from dishka.integrations.faststream import setup_dishka
from faststream.middlewares.exception import ExceptionMiddleware

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
)
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.enums.base import BaseEnum
from haolib.observability.setupper import ObservabilitySetupper

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase as BrokerType


class FastStreamEntrypointBrokerType(BaseEnum):
    """FastStream entrypoint broker type."""

    AIOKAFKA = "AIOKAFKA"
    CONFLUENT = "CONFLUENT"
    NATS = "NATS"
    RABBITMQ = "RABBITMQ"
    REDIS = "REDIS"


def _setup_observability_to_broker(
    observability_settuper: ObservabilitySetupper,
    broker_type: FastStreamEntrypointBrokerType,
    broker: BrokerType[Any, Any],
) -> None:
    """Setup observability.

    Args:
        observability_settuper: The observability settuper.
        broker_type: The type of the broker.
        broker: The broker.

    """
    tracer_provider = observability_settuper.get_tracer_provider()
    if tracer_provider is None:
        raise EntrypointInconsistencyError("Tracer provider is not set.")

    if broker_type == FastStreamEntrypointBrokerType.AIOKAFKA:
        from faststream.kafka.opentelemetry import (  # noqa: PLC0415
            KafkaTelemetryMiddleware as AIOKafkaTelemetryMiddleware,
        )

        broker.add_middleware(AIOKafkaTelemetryMiddleware(tracer_provider=tracer_provider))

    if broker_type == FastStreamEntrypointBrokerType.CONFLUENT:
        from faststream.confluent.opentelemetry import (  # noqa: PLC0415
            KafkaTelemetryMiddleware as ConfluentKafkaTelemetryMiddleware,
        )

        broker.add_middleware(ConfluentKafkaTelemetryMiddleware(tracer_provider=tracer_provider))

    if broker_type == FastStreamEntrypointBrokerType.RABBITMQ:
        from faststream.rabbit.opentelemetry import (  # noqa: PLC0415
            RabbitTelemetryMiddleware as RabbitMQTelemetryMiddleware,
        )

        broker.add_middleware(RabbitMQTelemetryMiddleware(tracer_provider=tracer_provider))

    if broker_type == FastStreamEntrypointBrokerType.NATS:
        from faststream.nats.opentelemetry import (  # noqa: PLC0415
            NatsTelemetryMiddleware as NATSTelemetryMiddleware,
        )

        broker.add_middleware(NATSTelemetryMiddleware(tracer_provider=tracer_provider))

    if broker_type == FastStreamEntrypointBrokerType.REDIS:
        from faststream.redis.opentelemetry import (  # noqa: PLC0415
            RedisTelemetryMiddleware,
        )

        broker.add_middleware(RedisTelemetryMiddleware(tracer_provider=tracer_provider))


class FastStreamDishkaPlugin(AbstractEntrypointPlugin[FastStreamEntrypoint]):
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

    def apply(self, component: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply Dishka to the entrypoint.

        Args:
            component: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        setup_dishka(container=self._container, app=component.get_app(), finalize_container=False)
        return component

    def validate(self, component: FastStreamEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            component: The entrypoint to validate against.

        """
        # No validation needed - container is required in __init__

    async def on_startup(self, component: FastStreamEntrypoint) -> None:
        """Startup hook.

        Args:
            component: The entrypoint that is starting up.

        """
        # No startup logic needed

    async def on_shutdown(self, component: FastStreamEntrypoint) -> None:
        """Shutdown hook.

        Args:
            component: The entrypoint that is shutting down.

        """
        # No shutdown logic needed


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


class FastStreamObservabilityPlugin(AbstractEntrypointPlugin[FastStreamEntrypoint]):
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

    def apply(self, component: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply observability to the entrypoint.

        Args:
            component: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        broker = component.get_app().broker
        if broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")

        _setup_observability_to_broker(self._observability, self._broker_type, broker)
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
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")
