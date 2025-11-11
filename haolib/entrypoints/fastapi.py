"""FastAPI entrypoint."""

from typing import Any, Self

from fastapi import FastAPI
from uvicorn import Config, Server

from haolib.configs.server import ServerConfig
from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError
from haolib.entrypoints.plugins.base import EntrypointPlugin, PluginPreset
from haolib.entrypoints.plugins.helpers import (
    apply_plugin,
    apply_preset,
    call_plugin_shutdown_hooks,
    call_plugin_startup_hooks,
    validate_plugins,
)
from haolib.entrypoints.plugins.registry import PluginRegistry


class FastAPIEntrypoint(AbstractEntrypoint):
    """FastAPI entrypoint implementation with plugin system.

    Provides a builder-pattern interface for configuring and running FastAPI applications
    using plugins for common features like dependency injection, observability, health checks, and more.

    Example:
        ```python
        from fastapi import FastAPI
        from haolib.entrypoints.fastapi import FastAPIEntrypoint
        from haolib.entrypoints.plugins.fastapi import (
            CORSMiddlewarePlugin,
            DishkaPlugin,
            HealthCheckPlugin,
            ObservabilityPlugin,
        )

        app = FastAPI()
        entrypoint = (
            FastAPIEntrypoint(app=app)
            .use_plugin(DishkaPlugin(container))
            .use_plugin(ObservabilityPlugin(observability))
            .use_plugin(HealthCheckPlugin([db_checker]))
            .use_plugin(CORSMiddlewarePlugin())
        )

        await entrypoint.run()
        ```

    """

    def __init__(self, app: FastAPI, server_config: ServerConfig | None = None) -> None:
        """Initialize the FastAPI entrypoint.

        Args:
            app: The FastAPI application instance.
            server_config: Optional server configuration. If None, uses default configuration.

        """
        self._app = app
        self._container: Any = None  # Set by DishkaPlugin
        self._server_config = server_config or ServerConfig()
        self._idempotency_configured = False  # Set by IdempotencyMiddlewarePlugin
        self._server: Server | None = None
        self._plugins: list[EntrypointPlugin[Self]] = []
        self._plugin_registry = PluginRegistry()

    def use_plugin(self, plugin: EntrypointPlugin[Self]) -> Self:
        """Add and apply a plugin to the entrypoint.

        Plugins are applied immediately and stored for lifecycle hooks.

        Args:
            plugin: The plugin to add and apply.

        Returns:
            Self for method chaining.

        Example:
            ```python
            entrypoint = FastAPIEntrypoint(app=app).use_plugin(DishkaPlugin(container))
            ```

        """
        return apply_plugin(self, plugin, self._plugins, self._plugin_registry)

    def use_preset(self, preset: PluginPreset[Self]) -> Self:
        """Add and apply a plugin preset to the entrypoint.

        Presets allow grouping related plugins together for easy application.

        Args:
            preset: The plugin preset to apply.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from haolib.entrypoints.plugins.base import PluginPreset

            preset = PluginPreset(
                DishkaPlugin(container),
                ObservabilityPlugin(observability),
            )
            entrypoint = FastAPIEntrypoint(app=app).use_preset(preset)
            ```

        """
        return apply_preset(self, preset, self._plugins, self._plugin_registry)

    def validate(self) -> None:
        """Validate FastAPI entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Also validates all plugins.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        validate_plugins(self, self._plugins)

    def get_app(self) -> FastAPI:
        """Get the FastAPI application instance.

        Returns:
            The FastAPI application instance.

        """
        return self._app

    @property
    def plugin_registry(self) -> PluginRegistry:
        """Get the plugin registry for plugin discovery.

        Provides read-only access to the plugin registry, allowing plugins
        to discover other plugins without accessing private attributes.

        Returns:
            The plugin registry instance.

        Example:
            ```python
            # In a plugin's apply() method
            if entrypoint.plugin_registry.has_plugin(DishkaPlugin):
                container = entrypoint.get_container()
            ```

        """
        return self._plugin_registry

    async def startup(self) -> None:
        """Startup the FastAPI entrypoint.

        Initializes the uvicorn server and prepares it for execution.
        Calls startup hooks for all plugins.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        # Create server instance
        self._server = Server(Config(self._app, host=self._server_config.host, port=self._server_config.port))

        await call_plugin_startup_hooks(self, self._plugins)

    async def shutdown(self) -> None:
        """Shutdown the FastAPI entrypoint.

        Cleans up resources and stops the server. Calls shutdown hooks for all plugins.
        This method is called after run() completes or is cancelled. Should be idempotent
        and safe to call multiple times.

        """
        await call_plugin_shutdown_hooks(self, self._plugins)

        if self._server is not None:
            # Server.shutdown() is called automatically when the serve() task is cancelled
            # But we can ensure cleanup here if needed
            self._server = None

    async def run(self, *args: Any, **kwargs: Any) -> None:
        """Run the FastAPI entrypoint.

        Starts the uvicorn server and serves the FastAPI application.
        This method runs indefinitely until cancelled or an error occurs.

        Args:
            *args: The arguments to pass to the Server serve method.
            **kwargs: The keyword arguments to pass to the Server serve method.

        Raises:
            EntrypointInconsistencyError: If entrypoint was not started via startup().
            Exception: Any exception that occurs during server execution.

        """
        if self._server is None:
            raise EntrypointInconsistencyError("FastAPI entrypoint must be started via startup() before run()")

        await self._server.serve(*args, **kwargs)
