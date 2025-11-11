"""Plugin registry for entrypoint plugin discovery."""

from collections.abc import Iterator
from typing import Any, TypeVar, cast

from haolib.entrypoints.plugins.base import EntrypointPlugin

T = TypeVar("T", bound=EntrypointPlugin[Any])


class PluginRegistry:
    """Read-only registry for plugin discovery.

    Provides type-safe methods for plugins to discover other plugins
    without accessing private attributes.

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
        self._plugins: list[EntrypointPlugin[Any]] = []

    def add(self, plugin: EntrypointPlugin[Any]) -> None:
        """Add a plugin to the registry.

        This method should only be called by the entrypoint during plugin application.
        Plugins are added to the registry in the order they are applied.

        Args:
            plugin: The plugin to register.

        Note:
            This is an internal method. Plugins are automatically registered when
            using entrypoint.use_plugin() or entrypoint.use_preset().

        """
        self._plugins.append(plugin)

    def has_plugin(self, plugin_type: type[T]) -> bool:
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

    def get_plugin(self, plugin_type: type[T]) -> T | None:
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

    def get_all_plugins(self) -> Iterator[EntrypointPlugin[Any]]:
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
