"""FastStream observability plugin."""

from typing import TYPE_CHECKING, Any

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
