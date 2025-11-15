"""FastStream entrypoint."""

import logging
from typing import TYPE_CHECKING, Any, Self

from haolib.components.events import EventEmitter
from haolib.components.plugins.registry import PluginRegistry
from haolib.entrypoints.abstract import (
    AbstractEntrypoint,
    EntrypointInconsistencyError,
)
from haolib.entrypoints.events.abstract import EntrypointShutdownEvent, EntrypointStartupEvent
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin, AbstractEntrypointPluginPreset
from haolib.entrypoints.plugins.helpers import (
    apply_plugin,
    apply_preset,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from faststream import FastStream


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
        self._events = EventEmitter[Self]()
        self._plugins: list[AbstractEntrypointPlugin[Self]] = []
        self._plugin_registry = PluginRegistry[Self]()

    @property
    def version(self) -> str:
        """Get the version of the FastStream entrypoint."""
        return "1.0.0"

    @property
    def events(self) -> EventEmitter[Self]:
        """Get the event emitter for the entrypoint."""
        return self._events

    def use_plugin(self, plugin: AbstractEntrypointPlugin[Self]) -> Self:
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

    def use_preset(
        self,
        preset: AbstractEntrypointPluginPreset[
            Self,
            AbstractEntrypointPlugin[Self],
        ],
    ) -> Self:
        """Add and apply a plugin preset to the entrypoint.

        Args:
            preset: The plugin preset to apply.

        Returns:
            Self for method chaining.

        """
        return apply_preset(self, preset, self._plugins, self._plugin_registry)

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

        await self.events.emit(EntrypointStartupEvent(component=self))

    async def shutdown(self) -> None:
        """Shutdown the FastStream entrypoint.

        Cleans up resources and stops the FastStream application. Calls shutdown hooks for all plugins.
        This method is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        await self.events.emit(EntrypointShutdownEvent(component=self))

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
