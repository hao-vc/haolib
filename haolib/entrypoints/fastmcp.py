"""FastMCP entrypoint."""

import logging
from typing import TYPE_CHECKING, Any, Self

from haolib.entrypoints.abstract import (
    AbstractEntrypoint,
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

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from fastmcp import FastMCP


class FastMCPEntrypoint(AbstractEntrypoint):
    """FastMCP entrypoint implementation with plugin system.

    Provides a builder-pattern interface for configuring and running FastMCP
    applications using plugins for common features like exception handling.

    Example:
        ```python
        from fastmcp import FastMCP
        from haolib.entrypoints.plugins.fastmcp import FastMCPExceptionHandlersPlugin

        fastmcp = FastMCP()
        entrypoint = (
            FastMCPEntrypoint(fastmcp=fastmcp)
            .use_plugin(FastMCPExceptionHandlersPlugin({
                ValueError: lambda exc, ctx: logger.error(f"Error: {exc}")
            }))
        )

        await entrypoint.run()
        ```

    Attributes:
        _fastmcp: The FastMCP application instance.
        _run_args: Positional arguments to pass to fastmcp.run_async().
        _run_kwargs: Keyword arguments to pass to fastmcp.run_async().
        _plugins: List of plugins applied to this entrypoint.

    """

    def __init__(self, fastmcp: FastMCP, *args: Any, **kwargs: Any) -> None:
        """Initialize the FastMCP entrypoint.

        Args:
            fastmcp: The FastMCP application instance.
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """

        self._fastmcp = fastmcp
        self._run_args = args
        self._run_kwargs = kwargs
        self._plugins: list[EntrypointPlugin[Self]] = []
        self._plugin_registry = PluginRegistry()

    def use_plugin(self, plugin: EntrypointPlugin[Self]) -> Self:
        """Add and apply a plugin to the entrypoint.

        Plugins are applied immediately and stored for lifecycle hooks.

        Args:
            plugin: The plugin to add and apply.

        Returns:
            Self for method chaining.

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
        """Validate FastMCP entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Also validates all plugins.

        """
        # FastMCP app is guaranteed by type system (required in __init__)
        # No validation needed

        validate_plugins(self, self._plugins)

    async def startup(self) -> None:
        """Startup the FastMCP entrypoint.

        Prepares the FastMCP application for execution. Calls startup hooks for all plugins.
        This method is called before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        logger.info("FastMCP entrypoint starting")

        await call_plugin_startup_hooks(self, self._plugins)

    async def shutdown(self) -> None:
        """Shutdown the FastMCP entrypoint.

        Cleans up resources and stops the FastMCP application. Calls shutdown hooks for all plugins.
        This method is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        await call_plugin_shutdown_hooks(self, self._plugins)

        if self._fastmcp is not None:
            logger.info("Shutting down FastMCP entrypoint")
            # FastMCP handles cleanup automatically when run_async() is cancelled
            # Additional cleanup can be added here if needed

    def get_app(self) -> FastMCP:
        """Get the FastMCP application instance.

        Returns:
            The FastMCP application instance.

        """
        return self._fastmcp

    @property
    def plugin_registry(self) -> PluginRegistry:
        """Get the plugin registry for plugin discovery.

        Provides read-only access to the plugin registry, allowing plugins
        to discover other plugins without accessing private attributes.

        Returns:
            The plugin registry instance.

        """
        return self._plugin_registry

    async def run(self) -> None:
        """Run the FastMCP entrypoint.

        Starts the FastMCP application and begins processing requests.
        This method runs indefinitely until cancelled or an error occurs.

        Raises:
            Exception: Any exception that occurs during execution.

        """
        await self._fastmcp.run_async(*self._run_args, **self._run_kwargs)
