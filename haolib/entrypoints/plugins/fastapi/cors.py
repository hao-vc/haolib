"""FastAPI CORS plugin."""

from fastapi.middleware.cors import CORSMiddleware

from haolib.configs.cors import CORSConfig
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin


class FastAPICORSMiddlewarePlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding CORS middleware to FastAPI entrypoints.

    Example:
        ```python
        from haolib.configs.cors import CORSConfig

        cors_config = CORSConfig(allow_origins=["https://example.com"])
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPICORSMiddlewarePlugin(cors_config))
        ```

    """

    def __init__(self, cors_config: CORSConfig | None = None) -> None:
        """Initialize the CORS middleware plugin.

        Args:
            cors_config: The CORS configuration. If None, uses default configuration.

        """
        self._cors_config = cors_config or CORSConfig()
        self._CORSMiddleware = CORSMiddleware

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply CORS middleware to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        component.get_app().add_middleware(
            self._CORSMiddleware,
            allow_origins=self._cors_config.allow_origins,
            allow_methods=self._cors_config.allow_methods,
            allow_headers=self._cors_config.allow_headers,
            allow_credentials=self._cors_config.allow_credentials,
            allow_origin_regex=self._cors_config.allow_origin_regex,
            expose_headers=self._cors_config.expose_headers,
            max_age=self._cors_config.max_age,
        )
        return component
