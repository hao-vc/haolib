"""Abstract plugin."""

from collections.abc import Sequence

# haolib/components/plugins/metadata.py
from dataclasses import dataclass
from typing import Any, Protocol

from haolib.components.abstract import AbstractComponent


@dataclass
class PluginMetadata:
    """Metadata about a plugin."""

    name: str
    version: str
    author: str | None = None
    description: str | None = None
    homepage: str | None = None
    license_name: str | None = None
    min_component_version: str | None = None
    max_component_version: str | None = None
    extra: dict[str, Any] | None = None


class AbstractPlugin[T_Component: AbstractComponent](Protocol):
    """Base protocol for component plugins.

    Plugins are reusable feature modules that can be applied to entrypoints
    to add functionality without modifying the core component code.

    Example:
        ```python
        class MyPlugin:
            def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
                # Configure the component
                return component

            def validate(self, component: FastAPIEntrypoint) -> None:
                # Validate plugin configuration
                pass

            async def on_startup(self, component: FastAPIEntrypoint) -> None:
                # Optional: startup hook
                pass

            async def on_shutdown(self, component: FastAPIEntrypoint) -> None:
                # Optional: shutdown hook
                pass
        ```

    """

    @property
    def priority(self) -> int:
        """Plugin priority for ordering.

        Lower values execute first. Default is 0.
        Plugins with same priority execute in application order.
        """
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata for versioning and compatibility."""
        return PluginMetadata(
            name=type(self).__name__,
            version="1.0.0",
        )

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[T_Component]]]:
        """Required plugin types that must be applied before this plugin.

        Returns:
            Sequence of plugin types that this plugin depends on.

        """
        return ()

    def apply(self, component: T_Component) -> T_Component:
        """Apply the plugin to an component.

        This is called during the builder phase to configure the component.
        Should return the component (for chaining) or a new configured instance.

        Args:
            component: The component to configure.

        Returns:
            The configured component (usually self for method chaining).

        """
        ...


class AbstractPluginPreset[T_Component: AbstractComponent, T_Plugin: AbstractPlugin]:
    """Composition of multiple plugins for common use cases.

    Presets allow grouping related plugins together for easy application.

    Example:
        ```python
        production_preset = PluginPreset(
            ObservabilityPlugin(observability),
            HealthCheckPlugin([db_checker]),
            CORSMiddlewarePlugin(),
        )

        component = FastAPIEntrypoint(app=app).use_preset(production_preset)
        ```

    """

    @property
    def plugins(self) -> Sequence[T_Plugin]:
        """The plugins in the preset."""
        return self._plugins

    def __init__(self, *plugins: T_Plugin) -> None:
        """Initialize the plugin preset.

        Args:
            *plugins: The plugins to compose.

        """
        self._plugins: Sequence[T_Plugin] = plugins

    def apply(self, component: T_Component) -> T_Component:
        """Apply all plugins in order.

        Args:
            component: The component to configure.

        Returns:
            The configured component.

        """
        result: T_Component = component
        for plugin in self._plugins:
            result = plugin.apply(result)
        return result
