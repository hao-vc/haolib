"""Helper functions for plugin management in entrypoints."""

from haolib.components.abstract import AbstractComponent
from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset
from haolib.components.plugins.registry import PluginRegistry


def _register_plugin_if_needed[T_Component: AbstractComponent](
    plugin: AbstractPlugin[T_Component],
    plugin_registry: PluginRegistry[T_Component] | None,
    component_version: str | None = None,
) -> None:
    """Register a plugin in the registry if registry is provided.

    Args:
        plugin: The plugin to register.
        plugin_registry: Optional plugin registry to register the plugin in.
        component_version: The component version to check for compatibility.

    """
    if plugin_registry is not None:
        plugin_registry.add(plugin, component_version=component_version)


def apply_plugin[T_Component: AbstractComponent](
    component: T_Component,
    plugin: AbstractPlugin[T_Component],
    plugin_registry: PluginRegistry[T_Component] | None = None,
) -> T_Component:
    """Apply a plugin to an component.

    This function adds the plugin to the plugins list, registers it in the
    registry (if provided), and calls the plugin's apply method.

    Args:
        component: The component to configure.
        plugin: The plugin to apply.
        plugin_registry: Optional plugin registry to register plugins in.

    Returns:
        The configured component.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        plugin = FastAPIDishkaPlugin(container)
        component = apply_plugin(
            component,
            plugin,
            component.plugin_registry,
        )
        ```

    """
    component_version = component.version

    _register_plugin_if_needed(plugin, plugin_registry, component_version=component_version)
    return plugin.apply(component)


def apply_preset[T_Component: AbstractComponent](
    component: T_Component,
    preset: AbstractPluginPreset[T_Component, AbstractPlugin[T_Component]],
    plugin_registry: PluginRegistry[T_Component],
) -> T_Component:
    """Apply a plugin preset to an component.

    This function applies the preset to the entrypoint and registers all
    individual plugins from the preset in the registry (if provided).

    Args:
        component: The component to configure.
        preset: The plugin preset to apply.
        plugin_registry: Optional plugin registry to register plugins in.

    Returns:
        The configured component.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        preset = PluginPreset(
            FastAPIDishkaPlugin(container),
            FastAPICORSMiddlewarePlugin(),
        )
        component = apply_preset(
            component,
            preset,
            component.plugin_registry,
        )
        ```

    """
    component_version = component.version

    result = preset.apply(component)
    # Store individual plugins from preset for lifecycle hooks

    for preset_plugin in preset.plugins:
        if not plugin_registry.has_plugin(type(preset_plugin)):
            _register_plugin_if_needed(preset_plugin, plugin_registry, component_version=component_version)
    return result
