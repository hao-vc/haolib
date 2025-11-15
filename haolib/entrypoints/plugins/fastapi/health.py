"""FastAPI health plugin."""

from typing import Any, cast

from fastapi import Request
from fastapi.responses import JSONResponse

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.web.health.checkers.abstract import AbstractHealthChecker
from haolib.web.health.handlers.fastapi import (
    FastAPIHealthCheckResponse,
    HealthCheckConfig,
    fastapi_health_check_handler_factory,
)


class FastAPIHealthCheckPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding health check endpoints to FastAPI entrypoints.

    Example:
        ```python
        from haolib.web.health.handlers.fastapi import HealthCheckConfig

        health_checkers = [db_checker, redis_checker]
        config = HealthCheckConfig(route_path="/health", timeout_seconds=5.0)
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIHealthCheckPlugin(health_checkers, config))
        ```

    """

    def __init__(
        self,
        health_checkers: list[AbstractHealthChecker] | None = None,
        config: HealthCheckConfig | None = None,
    ) -> None:
        """Initialize the health check plugin.

        Args:
            health_checkers: List of health checkers to execute. If None, endpoint returns healthy.
            config: Configuration for the health check endpoint. If None, uses default configuration.

        """
        self._health_checkers = health_checkers or []
        self._config = config or HealthCheckConfig()

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply health check endpoint to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        handler = fastapi_health_check_handler_factory(checkers=self._health_checkers, config=self._config)

        # Define responses for OpenAPI specification
        responses = cast(
            "dict[int | str, dict[str, Any]]",
            {
                self._config.status_code_healthy: {
                    "description": "Service is healthy",
                },
                self._config.status_code_unhealthy: {
                    "description": "Service is unhealthy",
                },
                self._config.status_code_degraded: {
                    "description": "Service is degraded",
                },
            },
        )

        # Map status values to HTTP status codes
        status_code_map = {
            "healthy": self._config.status_code_healthy,
            "unhealthy": self._config.status_code_unhealthy,
            "degraded": self._config.status_code_degraded,
        }

        @component.get_app().get(
            self._config.route_path,
            response_model=FastAPIHealthCheckResponse,
            responses=responses,
            summary="Health check",
            description="Check the health status of the service and its dependencies",
            tags=["health"],
        )
        async def health_handler(request: Request) -> JSONResponse:
            """Health check endpoint."""
            response_data = await handler(request)
            status_code = status_code_map.get(response_data.status, self._config.status_code_healthy)

            return JSONResponse(
                content=response_data.model_dump(),
                status_code=status_code,
            )

        return component
