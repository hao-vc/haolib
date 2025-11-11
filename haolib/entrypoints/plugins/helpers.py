"""Helper functions for plugin management in entrypoints."""

from typing import Any, TypeVar

from haolib.entrypoints.abstract import AbstractEntrypoint
from haolib.entrypoints.plugins.base import EntrypointPlugin, PluginPreset
from haolib.entrypoints.plugins.registry import PluginRegistry

T = TypeVar("T", bound=AbstractEntrypoint)


def _register_plugin_if_needed(
    plugin: EntrypointPlugin[Any],
    plugin_registry: PluginRegistry | None,
) -> None:
    """Register a plugin in the registry if registry is provided.

    Args:
        plugin: The plugin to register.
        plugin_registry: Optional plugin registry to register the plugin in.

    """
    if plugin_registry is not None:
        plugin_registry.add(plugin)


def apply_plugin(
    entrypoint: T,
    plugin: EntrypointPlugin[T],
    plugins_list: list[EntrypointPlugin[Any]],
    plugin_registry: PluginRegistry | None = None,
) -> T:
    """Apply a plugin to an entrypoint.

    This function adds the plugin to the plugins list, registers it in the
    registry (if provided), and calls the plugin's apply method.

    Args:
        entrypoint: The entrypoint to configure.
        plugin: The plugin to apply.
        plugins_list: The list to store plugins in (entrypoint._plugins).
        plugin_registry: Optional plugin registry to register plugins in.

    Returns:
        The configured entrypoint.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        plugin = FastAPIDishkaPlugin(container)
        entrypoint = apply_plugin(
            entrypoint,
            plugin,
            entrypoint._plugins,
            entrypoint.plugin_registry,
        )
        ```

    """
    plugins_list.append(plugin)
    _register_plugin_if_needed(plugin, plugin_registry)
    return plugin.apply(entrypoint)


def apply_preset(
    entrypoint: T,
    preset: PluginPreset[T],
    plugins_list: list[EntrypointPlugin[Any]],
    plugin_registry: PluginRegistry | None = None,
) -> T:
    """Apply a plugin preset to an entrypoint.

    This function applies the preset to the entrypoint and registers all
    individual plugins from the preset in the registry (if provided).

    Args:
        entrypoint: The entrypoint to configure.
        preset: The plugin preset to apply.
        plugins_list: The list to store plugins in (entrypoint._plugins).
        plugin_registry: Optional plugin registry to register plugins in.

    Returns:
        The configured entrypoint.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        preset = PluginPreset(
            FastAPIDishkaPlugin(container),
            FastAPICORSMiddlewarePlugin(),
        )
        entrypoint = apply_preset(
            entrypoint,
            preset,
            entrypoint._plugins,
            entrypoint.plugin_registry,
        )
        ```

    """
    result = preset.apply(entrypoint)
    # Store individual plugins from preset for lifecycle hooks
    for preset_plugin in preset._plugins:  # noqa: SLF001
        if preset_plugin not in plugins_list:
            plugins_list.append(preset_plugin)
            _register_plugin_if_needed(preset_plugin, plugin_registry)
    return result


def validate_plugins(entrypoint: AbstractEntrypoint, plugins_list: list[EntrypointPlugin[Any]]) -> None:
    """Validate all plugins applied to an entrypoint.

    Calls the validate method for each plugin in the order they were added.
    If any plugin validation fails, an EntrypointInconsistencyError is raised.

    Args:
        entrypoint: The entrypoint to validate plugins for.
        plugins_list: The list of plugins to validate.

    Raises:
        EntrypointInconsistencyError: If any plugin validation fails.

    Example:
        ```python
        validate_plugins(entrypoint, entrypoint._plugins)
        ```

    """
    for plugin in plugins_list:
        plugin.validate(entrypoint)


async def call_plugin_startup_hooks(entrypoint: AbstractEntrypoint, plugins_list: list[EntrypointPlugin[Any]]) -> None:
    """Call startup hooks for all plugins.

    Calls the on_startup method for each plugin in the order they were added.

    Args:
        entrypoint: The entrypoint that is starting up.
        plugins_list: The list of plugins to call startup hooks for.

    Example:
        ```python
        await call_plugin_startup_hooks(entrypoint, entrypoint._plugins)
        ```

    """
    for plugin in plugins_list:
        await plugin.on_startup(entrypoint)


async def call_plugin_shutdown_hooks(entrypoint: AbstractEntrypoint, plugins_list: list[EntrypointPlugin[Any]]) -> None:
    """Call shutdown hooks for all plugins (reverse order).

    Calls the on_shutdown method for each plugin in reverse order of addition.
    This ensures proper cleanup when plugins depend on each other.

    Args:
        entrypoint: The entrypoint that is shutting down.
        plugins_list: The list of plugins to call shutdown hooks for.

    Example:
        ```python
        await call_plugin_shutdown_hooks(entrypoint, entrypoint._plugins)
        ```

    """
    for plugin in reversed(plugins_list):
        await plugin.on_shutdown(entrypoint)
