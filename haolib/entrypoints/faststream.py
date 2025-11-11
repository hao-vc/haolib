"""FastStream entrypoint."""

import logging
from typing import TYPE_CHECKING, Any, Self

from haolib.entrypoints.abstract import (
    AbstractEntrypoint,
    EntrypointInconsistencyError,
)
from haolib.entrypoints.plugins.base import EntrypointPlugin, PluginPreset
from haolib.entrypoints.plugins.helpers import (
    apply_plugin,
    apply_preset,
    call_plugin_shutdown_hooks,
    call_plugin_startup_hooks,
    validate_plugins,
)
from haolib.entrypoints.plugins.registry import PluginRegistry
from haolib.enums.base import BaseEnum
from haolib.observability.setupper import ObservabilitySetupper

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
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


class FastStreamEntrypoint(AbstractEntrypoint):
    """FastStream entrypoint implementation with plugin system.

    Provides a builder-pattern interface for configuring and running FastStream
    applications using plugins for common features like dependency injection,
    observability, and exception handling.

    Example:
        ```python
        from faststream import FastStream
        from faststream.confluent import KafkaBroker
        from haolib.entrypoints.plugins.faststream import (
            FastStreamDishkaPlugin,
            FastStreamObservabilityPlugin,
        )

        broker = KafkaBroker()
        app = FastStream(broker=broker)

        entrypoint = (
            FastStreamEntrypoint(app=app)
            .use_plugin(FastStreamDishkaPlugin(container))
            .use_plugin(
                FastStreamObservabilityPlugin(observability, FastStreamEntrypointBrokerType.CONFLUENT)
            )
        )

        await entrypoint.run()
        ```

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
        self._plugins: list[EntrypointPlugin[Self]] = []
        self._plugin_registry = PluginRegistry()

    def use_plugin(self, plugin: EntrypointPlugin[Self]) -> Self:
        """Add and apply a plugin to the entrypoint.

        Plugins are applied immediately and stored for lifecycle hooks.

        Args:
            plugin: The plugin to add and apply.

        Returns:
            Self for method chaining.

        Example:
            ```python
            entrypoint = FastStreamEntrypoint(app=app).use_plugin(FastStreamDishkaPlugin(container))
            ```

        """
        return apply_plugin(self, plugin, self._plugins, self._plugin_registry)

    def use_preset(self, preset: PluginPreset[Self]) -> Self:
        """Add and apply a plugin preset to the entrypoint.

        Args:
            preset: The plugin preset to apply.

        Returns:
            Self for method chaining.

        """
        return apply_preset(self, preset, self._plugins, self._plugin_registry)

    def validate(self) -> None:
        """Validate FastStream entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Also validates all plugins.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        # App is guaranteed by type system, but broker configuration is runtime state
        if self._app.broker is None:
            raise EntrypointInconsistencyError("FastStream broker is not configured in the app.")

        validate_plugins(self, self._plugins)

    @property
    def plugin_registry(self) -> PluginRegistry:
        """Get the plugin registry for plugin discovery.

        Provides read-only access to the plugin registry, allowing plugins
        to discover other plugins without accessing private attributes.

        Returns:
            The plugin registry instance.

        """
        return self._plugin_registry

    async def startup(self) -> None:
        """Startup the FastStream entrypoint.

        Prepares the FastStream application for execution. Calls startup hooks for all plugins.
        This method is called before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        logger.info("FastStream entrypoint starting")

        await call_plugin_startup_hooks(self, self._plugins)

    async def shutdown(self) -> None:
        """Shutdown the FastStream entrypoint.

        Cleans up resources and stops the FastStream application. Calls shutdown hooks for all plugins.
        This method is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        await call_plugin_shutdown_hooks(self, self._plugins)

        if self._app is not None:
            logger.info("Shutting down FastStream entrypoint")
            # FastStream handles cleanup automatically when run() is cancelled
            # Additional cleanup can be added here if needed

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
