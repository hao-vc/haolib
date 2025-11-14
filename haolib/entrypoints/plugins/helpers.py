"""Helper functions for plugin management in entrypoints."""

from haolib.components.plugins.registry import PluginRegistry
from haolib.entrypoints.abstract import AbstractEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin, AbstractEntrypointPluginPreset


def _register_plugin_if_needed[T_Entrypoint: AbstractEntrypoint](
    plugin: AbstractEntrypointPlugin[T_Entrypoint],
    plugin_registry: PluginRegistry[T_Entrypoint] | None,
) -> None:
    """Register a plugin in the registry if registry is provided.

    Args:
        plugin: The plugin to register.
        plugin_registry: Optional plugin registry to register the plugin in.

    """
    if plugin_registry is not None:
        plugin_registry.add(plugin)


def apply_plugin[T_Entrypoint: AbstractEntrypoint](
    entrypoint: T_Entrypoint,
    plugin: AbstractEntrypointPlugin[T_Entrypoint],
    plugins_list: list[AbstractEntrypointPlugin[T_Entrypoint]],
    plugin_registry: PluginRegistry[T_Entrypoint] | None = None,
) -> T_Entrypoint:
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


def apply_preset[T_Entrypoint: AbstractEntrypoint](
    entrypoint: T_Entrypoint,
    preset: AbstractEntrypointPluginPreset[T_Entrypoint, AbstractEntrypointPlugin[T_Entrypoint]],
    plugins_list: list[AbstractEntrypointPlugin[T_Entrypoint]],
    plugin_registry: PluginRegistry[T_Entrypoint] | None = None,
) -> T_Entrypoint:
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
    for preset_plugin in preset.plugins:
        if preset_plugin not in plugins_list:
            plugins_list.append(preset_plugin)
            _register_plugin_if_needed(preset_plugin, plugin_registry)
    return result
