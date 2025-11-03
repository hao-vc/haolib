"""FastStream entrypoint."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

from dishka.integrations.faststream import setup_dishka
from faststream._internal.broker import BrokerUsecase as BrokerType
from faststream.middlewares.exception import ExceptionMiddleware
from faststream.opentelemetry import TelemetryMiddleware

from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError
from haolib.enums.base import BaseEnum
from haolib.observability.setupper import ObservabilitySetupper

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


class FastStreamEntrypointComponent:
    """FastStream entrypoint component to use in integration with other entrypoints."""

    def __init__(self, broker: BrokerType[Any, Any]) -> None:
        """Initialize the FastStream entrypoint component."""
        self._broker = broker

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup dishka."""
        setup_dishka(container=container, broker=self._broker)

        return self

    def setup_telemetry_middleware(self, telemetry_middleware: TelemetryMiddleware) -> Self:
        """Setup telemetry middleware. Should be used if you want to use your own TelemetryMiddleware.

        Args:
            telemetry_middleware: The telemetry middleware.

        """
        self._broker.add_middleware(telemetry_middleware)
        return self

    def setup_observability(
        self, observability_settuper: ObservabilitySetupper, broker_type: FastStreamEntrypointBrokerType
    ) -> Self:
        """Setup observability.

        Args:
            observability_settuper: The observability settuper.
            See `haolib.observability.setupper.ObservabilitySetupper` for more details.
            broker_type: The type of the broker.

        """

        _setup_observability_to_broker(observability_settuper, broker_type, self._broker)

        return self

    def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> None:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.

        """

        if self._broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        self._broker.add_middleware(ExceptionMiddleware(publish_handlers=exception_handlers))

    def get_broker(self) -> BrokerType[Any, Any]:
        """Get the broker."""
        return self._broker


class FastStreamEntrypoint(AbstractEntrypoint):
    """FastStream entrypoint."""

    def __init__(
        self,
        app: FastStream,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize the FastStream entrypoint.

        Args:
            broker: The FastStream broker.
            app: The FastStream app.
            *args: The arguments to pass to the FastStream run method.
            **kwargs: The keyword arguments to pass to the FastStream run method.

        """

        self._run_args = args
        self._run_kwargs = kwargs

        self._app: FastStream = app

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup dishka."""

        setup_dishka(container=container, app=self._app, finalize_container=False)

        return self

    def get_app(self) -> FastStream:
        """Get the FastStream app."""
        if self._app is None:
            raise EntrypointInconsistencyError("App is not set.")

        return self._app

    def setup_exception_handlers(self, exception_handlers: dict[type[Exception], Callable[..., Any]]) -> Self:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.

        """

        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        self._app.broker.add_middleware(ExceptionMiddleware(publish_handlers=exception_handlers))

        return self

    def setup_observability(
        self, observability_settuper: ObservabilitySetupper, broker_type: FastStreamEntrypointBrokerType
    ) -> Self:
        """Setup observability.

        Args:
            observability_settuper: The observability settuper.
            See `haolib.observability.setupper.ObservabilitySetupper` for more details.
            broker_type: The type of the broker.

        """

        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not set.")

        _setup_observability_to_broker(observability_settuper, broker_type, self._app.broker)

        return self

    async def run(self) -> None:
        """Run the FastStream entrypoint.

        Args:
            *args: The arguments to pass to the FastStream run method.
            **kwargs: The keyword arguments to pass to the FastStream run method.

        """
        if self._app is None:
            raise EntrypointInconsistencyError("App is not set.")

        await self._app.run(*self._run_args, **self._run_kwargs)
