"""Base plugin protocol and preset system."""

from collections.abc import Sequence
from typing import Any, Protocol, TypeVar

from haolib.entrypoints.abstract import AbstractEntrypoint

T = TypeVar("T", bound=AbstractEntrypoint)


class EntrypointPlugin(Protocol[T]):
    """Base protocol for entrypoint plugins.

    Plugins are reusable feature modules that can be applied to entrypoints
    to add functionality without modifying the core entrypoint code.

    Example:
        ```python
        class MyPlugin:
            def apply(self, entrypoint: FastAPIEntrypoint) -> FastAPIEntrypoint:
                # Configure the entrypoint
                return entrypoint

            def validate(self, entrypoint: FastAPIEntrypoint) -> None:
                # Validate plugin configuration
                pass

            async def on_startup(self, entrypoint: FastAPIEntrypoint) -> None:
                # Optional: startup hook
                pass

            async def on_shutdown(self, entrypoint: FastAPIEntrypoint) -> None:
                # Optional: shutdown hook
                pass
        ```

    """

    def apply(self, entrypoint: T) -> T:
        """Apply the plugin to an entrypoint.

        This is called during the builder phase to configure the entrypoint.
        Should return the entrypoint (for chaining) or a new configured instance.

        Args:
            entrypoint: The entrypoint to configure.

        Returns:
            The configured entrypoint (usually self for method chaining).

        """
        ...

    def validate(self, entrypoint: T) -> None:
        """Validate plugin configuration for the entrypoint.

        Called during entrypoint validation phase.

        Args:
            entrypoint: The entrypoint to validate against.

        Raises:
            EntrypointInconsistencyError: If plugin cannot be applied.

        """
        ...

    async def on_startup(self, entrypoint: T) -> None:
        """Called during entrypoint startup phase.

        Optional lifecycle hook for startup logic.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        ...

    async def on_shutdown(self, entrypoint: T) -> None:
        """Called during entrypoint shutdown phase.

        Optional lifecycle hook for cleanup logic.

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        ...


class PluginPreset[T]:
    """Composition of multiple plugins for common use cases.

    Presets allow grouping related plugins together for easy application.

    Example:
        ```python
        production_preset = PluginPreset(
            ObservabilityPlugin(observability),
            HealthCheckPlugin([db_checker]),
            CORSMiddlewarePlugin(),
        )

        entrypoint = FastAPIEntrypoint(app=app).use_preset(production_preset)
        ```

    """

    def __init__(self, *plugins: EntrypointPlugin[Any]) -> None:
        """Initialize the plugin preset.

        Args:
            *plugins: The plugins to compose.

        """
        self._plugins: Sequence[EntrypointPlugin[Any]] = plugins

    def apply(self, entrypoint: T) -> T:
        """Apply all plugins in order.

        Args:
            entrypoint: The entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        result: T = entrypoint
        for plugin in self._plugins:
            result = plugin.apply(result)
        return result

    def validate(self, entrypoint: T) -> None:
        """Validate all plugins.

        Args:
            entrypoint: The entrypoint to validate against.

        Raises:
            EntrypointInconsistencyError: If any plugin validation fails.

        """
        for plugin in self._plugins:
            plugin.validate(entrypoint)

    async def on_startup(self, entrypoint: T) -> None:
        """Call startup hooks for all plugins.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        for plugin in self._plugins:
            if hasattr(plugin, "on_startup"):
                await plugin.on_startup(entrypoint)

    async def on_shutdown(self, entrypoint: T) -> None:
        """Call shutdown hooks for all plugins (reverse order).

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        for plugin in reversed(self._plugins):
            if hasattr(plugin, "on_shutdown"):
                await plugin.on_shutdown(entrypoint)
