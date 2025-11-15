"""Plugin registry for component plugins."""

from collections import defaultdict
from collections.abc import Iterator
from typing import TYPE_CHECKING, cast

from haolib.components.abstract import ComponentInconsistencyError
from haolib.components.plugins.versioning import check_version_compatibility

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

    def add(self, new_plugin: AbstractPlugin[T_Component], component_version: str | None = None) -> None:
        """Add a plugin to the registry.

        This method should only be called by the component during plugin application.
        Uses topological sort to order plugins based on dependencies.

        Raises:
            ComponentInconsistencyError: If circular dependencies detected.

        Args:
            new_plugin: The plugin to register.
            component_version: The component version to check for compatibility.

        Note:
            This is an internal method. Plugins are automatically registered when
            using component.use_plugin() or component.use_preset().

        """
        # Check version compatibility if component version is provided
        if component_version is not None:
            metadata = new_plugin.metadata
            check_version_compatibility(
                component_version=component_version,
                plugin_name=metadata.name,
                min_version=metadata.min_component_version,
                max_version=metadata.max_component_version,
            )

        self._plugins.append(new_plugin)

        # Build dependency graph
        plugin_map: dict[type[AbstractPlugin], AbstractPlugin] = {
            type(new_plugin): new_plugin for new_plugin in self._plugins
        }

        # Add already registered plugins to graph
        # Build adjacency list
        graph: dict[type[AbstractPlugin], set[type[AbstractPlugin]]] = defaultdict(set)
        in_degree: dict[type[AbstractPlugin], int] = defaultdict(int)

        for plugin in self._plugins:
            plugin_type = type(plugin)
            in_degree[plugin_type] = 0

            for dep_type in plugin.dependencies:
                if dep_type not in plugin_map:
                    msg = f"{plugin_type.__name__} requires {dep_type.__name__} which is not available"
                    raise ComponentInconsistencyError(msg)
                graph[dep_type].add(plugin_type)
                in_degree[plugin_type] += 1

        # Topological sort
        queue: list[type[AbstractPlugin]] = [plugin_type for plugin_type, degree in in_degree.items() if degree == 0]
        result: list[AbstractPlugin[T_Component]] = []

        while queue:
            # Sort by priority for deterministic ordering
            queue.sort(key=lambda plugin_type: plugin_map[plugin_type].priority)
            current = queue.pop(0)
            result.append(plugin_map[current])

            for dependent in graph[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Check for circular dependencies
        if len(result) != len(self._plugins):
            raise ComponentInconsistencyError("Circular dependency detected in plugin dependencies")

        self._plugins = result

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
