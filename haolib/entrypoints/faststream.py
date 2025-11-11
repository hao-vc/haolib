"""FastStream entrypoint."""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from dishka.integrations.faststream import setup_dishka
from faststream._internal.broker import BrokerUsecase as BrokerType
from faststream.middlewares.exception import ExceptionMiddleware
from faststream.opentelemetry import TelemetryMiddleware

from haolib.entrypoints.abstract import (
    AbstractEntrypoint,
    AbstractEntrypointComponent,
    EntrypointInconsistencyError,
)
from haolib.enums.base import BaseEnum
from haolib.observability.setupper import ObservabilitySetupper

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from dishka import AsyncContainer
    from faststream import FastStream
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


class FastStreamEntrypointComponent(AbstractEntrypointComponent):
    """FastStream entrypoint component for integration with other entrypoints.

    This component provides a standardized interface for integrating FastStream
    brokers with other entrypoint types (e.g., FastAPI). It encapsulates broker
    configuration and provides integration hooks.

    Example:
        ```python
        from faststream.confluent import KafkaBroker

        broker = KafkaBroker()
        component = (
            FastStreamEntrypointComponent(broker=broker)
            .setup_dishka(container)
            .setup_observability(observability, FastStreamEntrypointBrokerType.CONFLUENT)
        )

        fastapi_entrypoint.setup_faststream(component)
        ```

    Attributes:
        _broker: The FastStream broker instance.

    """

    def __init__(self, broker: BrokerType[Any, Any]) -> None:
        """Initialize the FastStream entrypoint component.

        Args:
            broker: The FastStream broker instance to wrap.

        """
        self._broker = broker

    def validate(self) -> None:
        """Validate FastStream component configuration.

        Validates that the component is properly configured and ready for use.

        Raises:
            EntrypointInconsistencyError: If component configuration is invalid.

        """
        # Broker is guaranteed by type system (required in __init__)
        # No validation needed

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup Dishka dependency injection for the broker.

        Configures Dishka to work with the FastStream broker, enabling
        dependency injection in message handlers.

        Args:
            container: The Dishka async container instance.

        Returns:
            Self for method chaining.

        """
        setup_dishka(container=container, broker=self._broker)

        return self

    def setup_telemetry_middleware(self, telemetry_middleware: TelemetryMiddleware) -> Self:
        """Setup custom telemetry middleware.

        Adds a custom TelemetryMiddleware to the broker. Use this if you need
        to use your own telemetry middleware instead of the default observability setup.

        Args:
            telemetry_middleware: The telemetry middleware instance to add.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from faststream.opentelemetry import TelemetryMiddleware

            middleware = TelemetryMiddleware(tracer_provider=my_tracer_provider)
            component.setup_telemetry_middleware(middleware)
            ```

        """
        self._broker.add_middleware(telemetry_middleware)
        return self

    def setup_observability(
        self, observability_settuper: ObservabilitySetupper, broker_type: FastStreamEntrypointBrokerType
    ) -> Self:
        """Setup observability for the broker.

        Configures OpenTelemetry tracing for the FastStream broker based on
        the broker type. This automatically adds the appropriate telemetry
        middleware for the specified broker.

        Args:
            observability_settuper: The observability setupper instance.
                See `haolib.observability.setupper.ObservabilitySetupper` for more details.
            broker_type: The type of the broker (Kafka, RabbitMQ, NATS, etc.).

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If tracer provider is not set in observability setupper.

        Example:
            ```python
            observability = ObservabilitySetupper().setup_tracing()
            component.setup_observability(
                observability,
                FastStreamEntrypointBrokerType.CONFLUENT
            )
            ```

        """
        _setup_observability_to_broker(observability_settuper, broker_type, self._broker)

        return self

    def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> Self:
        """Setup exception handlers for the broker.

        Configures exception handling middleware for the FastStream broker.
        Handlers are called when exceptions occur during message processing.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
                Handlers receive the exception and can perform logging, retries, etc.

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If broker is not set.

        Example:
            ```python
            def handle_value_error(exc: ValueError):
                logger.error(f"Value error: {exc}")

            component.setup_exception_handlers({
                ValueError: handle_value_error
            })
            ```

        """

        self._broker.add_middleware(ExceptionMiddleware(publish_handlers=exception_handlers))

        return self

    def get_broker(self) -> BrokerType[Any, Any]:
        """Get the FastStream broker instance.

        Returns:
            The FastStream broker instance.

        """
        return self._broker


class FastStreamEntrypoint(AbstractEntrypoint):
    """FastStream entrypoint implementation.

    Provides a builder-pattern interface for configuring and running FastStream
    applications with features like dependency injection, observability, and
    exception handling.

    Example:
        ```python
        from faststream import FastStream
        from faststream.confluent import KafkaBroker

        broker = KafkaBroker()
        app = FastStream(broker=broker)

        entrypoint = (
            FastStreamEntrypoint(app=app)
            .setup_dishka(container)
            .setup_observability(observability, FastStreamEntrypointBrokerType.CONFLUENT)
        )

        await entrypoint.run()
        ```

    Attributes:
        _app: The FastStream application instance.
        _run_args: Positional arguments to pass to app.run().
        _run_kwargs: Keyword arguments to pass to app.run().

    """

    def __init__(
        self,
        app: FastStream,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize the FastStream entrypoint.

        Args:
            app: The FastStream application instance.
            *args: The arguments to pass to the FastStream run method.
            **kwargs: The keyword arguments to pass to the FastStream run method.

        """
        self._run_args = args
        self._run_kwargs = kwargs
        self._app: FastStream = app

    def validate(self) -> None:
        """Validate FastStream entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Specifically checks:
        - Broker is configured in the app (runtime state validation)

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        # App is guaranteed by type system, but broker configuration is runtime state
        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")

    async def startup(self) -> None:
        """Startup the FastStream entrypoint.

        Prepares the FastStream application for execution. This method is called
        before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        logger.info("FastStream entrypoint starting")

    async def shutdown(self) -> None:
        """Shutdown the FastStream entrypoint.

        Cleans up resources and stops the FastStream application. This method
        is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        if self._app is not None:
            logger.info("Shutting down FastStream entrypoint")
            # FastStream handles cleanup automatically when run() is cancelled
            # Additional cleanup can be added here if needed

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup Dishka dependency injection.

        Configures Dishka to work with the FastStream application, enabling
        dependency injection in message handlers.

        Args:
            container: The Dishka async container instance.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from dishka import make_async_container

            container = make_async_container(...)
            entrypoint.setup_dishka(container)
            ```

        """
        setup_dishka(container=container, app=self._app, finalize_container=False)

        return self

    def get_app(self) -> FastStream:
        """Get the FastStream application instance.

        Returns:
            The FastStream application instance.

        Raises:
            EntrypointInconsistencyError: If app is not set.

        """
        if self._app is None:
            raise EntrypointInconsistencyError("FastStream app is not set.")

        return self._app

    def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> Self:
        """Setup exception handlers for the FastStream broker.

        Configures exception handling middleware for message processing.
        Handlers are called when exceptions occur during message handling.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If broker is not configured.

        Example:
            ```python
            def handle_error(exc: Exception):
                logger.error(f"Error: {exc}")

            entrypoint.setup_exception_handlers({
                ValueError: handle_error
            })
            ```

        """
        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        self._app.broker.add_middleware(ExceptionMiddleware(publish_handlers=exception_handlers))

        return self

    def setup_observability(
        self, observability_settuper: ObservabilitySetupper, broker_type: FastStreamEntrypointBrokerType
    ) -> Self:
        """Setup observability for the FastStream broker.

        Configures OpenTelemetry tracing for the broker based on the broker type.
        This automatically adds the appropriate telemetry middleware.

        Args:
            observability_settuper: The observability setupper instance.
                See `haolib.observability.setupper.ObservabilitySetupper` for more details.
            broker_type: The type of the broker (Kafka, RabbitMQ, NATS, etc.).

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If broker is not configured or tracer provider is not set.

        Example:
            ```python
            observability = ObservabilitySetupper().setup_tracing()
            entrypoint.setup_observability(
                observability,
                FastStreamEntrypointBrokerType.CONFLUENT
            )
            ```

        """
        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        _setup_observability_to_broker(observability_settuper, broker_type, self._app.broker)

        return self

    async def run(self) -> None:
        """Run the FastStream entrypoint.

        Starts the FastStream application and begins processing messages.
        This method runs indefinitely until cancelled or an error occurs.

        Raises:
            EntrypointInconsistencyError: If entrypoint was not started via startup().
            Exception: Any exception that occurs during execution.

        """
        if self._app is None:
            raise EntrypointInconsistencyError("FastStream app is not set.")

        await self._app.run(*self._run_args, **self._run_kwargs)
