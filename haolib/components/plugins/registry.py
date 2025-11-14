"""Plugin registry for component plugins."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from haolib.components.abstract import AbstractComponent
    from haolib.components.plugins.abstract import AbstractPlugin


class PluginRegistry[T_Component: AbstractComponent]:
    """Registry for component plugins.

    Provides type-safe methods for plugins to discover other plugins.

    Example:
        ```python
        # In a plugin's apply() method
        if entrypoint.plugin_registry.has_plugin(FastAPIDishkaPlugin):
            # FastAPIDishkaPlugin is available
            fastapi_dishka_plugin = entrypoint.plugin_registry.get_plugin(FastAPIDishkaPlugin)
            if fastapi_dishka_plugin is not None:
                # Access plugin's public methods
                container = fastapi_dishka_plugin.get_container()
        ```

    """

    def __init__(self) -> None:
        """Initialize the plugin registry."""
        self._plugins: list[AbstractPlugin[T_Component]] = []

    def add(self, plugin: AbstractPlugin[T_Component]) -> None:
        """Add a plugin to the registry.

        This method should only be called by the component during plugin application.
        Plugins are added to the registry in the order they are applied.

        Args:
            plugin: The plugin to register.

        Note:
            This is an internal method. Plugins are automatically registered when
            using component.use_plugin() or component.use_preset().

        """
        self._plugins.append(plugin)

    def has_plugin[T: AbstractPlugin](self, plugin_type: type[T]) -> bool:
        """Check if a plugin of the given type is registered.

        Args:
            plugin_type: The plugin class to check for.

        Returns:
            True if a plugin of the given type is registered.

        Example:
            ```python
            if registry.has_plugin(FastAPIDishkaPlugin):
                # FastAPIDishkaPlugin is available
            ```

        """
        return any(isinstance(plugin, plugin_type) for plugin in self._plugins)

    def get_plugin[T: AbstractPlugin](self, plugin_type: type[T]) -> T | None:
        """Get a plugin of the given type.

        Returns the first plugin instance that matches the given type.
        If multiple instances of the same type exist, returns the first one added.

        Args:
            plugin_type: The plugin class to retrieve.

        Returns:
            The plugin instance if found, None otherwise.

        Example:
            ```python
            fastapi_dishka_plugin = registry.get_plugin(FastAPIDishkaPlugin)
            if fastapi_dishka_plugin is not None:
                # Use the plugin instance or access plugin's public methods
                container = fastapi_dishka_plugin.get_container()
            ```

        """
        for plugin in self._plugins:
            if isinstance(plugin, plugin_type):
                return cast("T", plugin)
        return None

    def get_all_plugins(self) -> Iterator[AbstractPlugin[T_Component]]:
        """Get all registered plugins.

        Returns an iterator over all registered plugins in the order they were added.
        Each call creates a new iterator, so multiple iterations are independent.

        Returns:
            An iterator over all registered plugins.

        Example:
            ```python
            for plugin in registry.get_all_plugins():
                # Process each plugin
                print(type(plugin).__name__)
            ```

        """
        return iter(self._plugins)
