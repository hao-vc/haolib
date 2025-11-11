"""FastAPI entrypoint."""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Self, cast

from dishka import AsyncContainer, Scope
from dishka.integrations.fastapi import setup_dishka as setup_dishka_fastapi
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from uvicorn import Config, Server

from haolib.configs.cors import CORSConfig
from haolib.configs.server import ServerConfig
from haolib.entrypoints.abstract import AbstractEntrypoint, EntrypointInconsistencyError
from haolib.exceptions.base.fastapi import FastAPIBaseException
from haolib.exceptions.handlers.fastapi import fastapi_base_exception_handler, fastapi_unknown_exception_handler
from haolib.observability.setupper import ObservabilitySetupper
from haolib.web.health.core.checker import AbstractHealthChecker
from haolib.web.health.handlers.fastapi import (
    FastAPIHealthCheckResponse,
    HealthCheckConfig,
    fastapi_health_check_handler_factory,
)
from haolib.web.idempotency.fastapi import (
    fastapi_default_idempotency_response_factory,
    fastapi_idempotency_middleware_handler,
)
from haolib.web.idempotency.storages.abstract import AbstractIdempotencyKeysStorage

if TYPE_CHECKING:
    from haolib.entrypoints.fastmcp import FastMCPEntrypointComponent
    from haolib.entrypoints.faststream import FastStreamEntrypointComponent


class FastAPIEntrypoint(AbstractEntrypoint):
    """FastAPI entrypoint."""

    def __init__(self, app: FastAPI, server_config: ServerConfig | None = None) -> None:
        """Initialize the FastAPI entrypoint."""
        self._app = app
        self._container: AsyncContainer | None = None
        self._server_config = server_config or ServerConfig()

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup dishka."""
        setup_dishka_fastapi(container, self._app)

        self._container = container

        return self

    def setup_observability(self, observability: ObservabilitySetupper) -> Self:
        """Setup observability.

        Args:
            observability: The observability setupper.
            See `haolib.observability.setupper.ObservabilitySetupper` for more details.

        """
        observability.instrument_fastapi(self._app)

        return self

    def setup_cors_middleware(self, cors_config: CORSConfig | None = None) -> Self:
        """Setup CORS middleware.

        Args:
            cors_config: The CORS config.
            If None, the default CORS config will be used.
            See `haolib.configs.cors.CORSConfig` for more details.

        """
        if cors_config is None:
            cors_config = CORSConfig()

        self._app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_config.allow_origins,
            allow_methods=cors_config.allow_methods,
            allow_headers=cors_config.allow_headers,
            allow_credentials=cors_config.allow_credentials,
            allow_origin_regex=cors_config.allow_origin_regex,
            expose_headers=cors_config.expose_headers,
            max_age=cors_config.max_age,
        )

        return self

    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[..., Any]] | None = None
    ) -> Self:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.
            If None, the default exception handlers will be used:
            - Unknown exceptions will be handled by
                `haolib.exceptions.fastapi.handlers.fastapi_unknown_exception_handler`
            - FastAPIBaseException exceptions will be handled by
                `haolib.exceptions.fastapi.handlers.fastapi_base_exception_handler`


        """
        if exception_handlers is None:
            exception_handlers = {
                Exception: fastapi_unknown_exception_handler,
                FastAPIBaseException: fastapi_base_exception_handler,
            }

        for exception, handler in exception_handlers.items():
            self._app.add_exception_handler(exception, handler)

        return self

    def setup_health_check(
        self,
        health_checkers: list[AbstractHealthChecker] | None = None,
        config: HealthCheckConfig | None = None,
    ) -> Self:
        """Setup health check endpoint.

        Args:
            health_checkers: List of health checkers to execute. If None, endpoint returns healthy.
            config: Configuration for the health check endpoint.
                If None, uses default configuration.
                See `haolib.web.health.handlers.fastapi.HealthCheckConfig` for details.

        Example:
            ```python
            from haolib.web.health.handlers.fastapi import HealthCheckConfig

            entrypoint.setup_health_check(
                health_checkers=[db_checker, redis_checker],
                config=HealthCheckConfig(
                    route_path="/health",
                    timeout_seconds=5.0,
                    status_code_unhealthy=503,
                )
            )
            ```

        """
        if config is None:
            config = HealthCheckConfig()

        handler = fastapi_health_check_handler_factory(checkers=health_checkers, config=config)

        # Define responses for OpenAPI specification
        # Include all possible status codes with the same response model
        # FastAPI will use response_model for schema, responses dict is for documentation
        responses = cast(
            "dict[int | str, dict[str, Any]]",
            {
                config.status_code_healthy: {
                    "description": "Service is healthy",
                },
                config.status_code_unhealthy: {
                    "description": "Service is unhealthy",
                },
                config.status_code_degraded: {
                    "description": "Service is degraded",
                },
            },
        )

        # Map status values to HTTP status codes
        status_code_map = {
            "healthy": config.status_code_healthy,
            "unhealthy": config.status_code_unhealthy,
            "degraded": config.status_code_degraded,
        }

        @self._app.get(
            config.route_path,
            response_model=FastAPIHealthCheckResponse,
            responses=responses,
            summary="Health check",
            description="Check the health status of the service and its dependencies",
            tags=["health"],
        )
        async def health_handler(request: Request) -> JSONResponse:
            """Health check endpoint."""
            response_data = await handler(request)
            status_code = status_code_map.get(response_data.status, config.status_code_healthy)

            return JSONResponse(
                content=response_data.model_dump(),
                status_code=status_code,
            )

        return self

    def setup_idempotency_middleware(
        self,
        idempotent_response_factory: Callable[[Request, AbstractIdempotencyKeysStorage], Awaitable[Response]]
        | None = None,
        idempotency_keys_storage_factory: Callable[[], Awaitable[AbstractIdempotencyKeysStorage]] | None = None,
    ) -> Self:
        """Setup idempotency middleware for FastAPI.

        Args:
            idempotent_response_factory: The factory callable to create the idempotent response.
            idempotency_keys_storage_factory: The factory callable to create the idempotency keys storage.
                If None, the idempotency keys storage will be extracted from the Dishka container.
                If no container previously setup, an error will be raised.

        """

        if idempotency_keys_storage_factory is not None:

            @self._app.middleware("http")
            async def idempotency_middleware_for_app(
                request: Request,
                call_next: Callable[[Request], Awaitable[Response]],
            ) -> Response:
                """Idempotency middleware for the app."""
                return await fastapi_idempotency_middleware_handler(
                    request,
                    call_next,
                    idempotency_keys_storage=await idempotency_keys_storage_factory(),
                    idempotent_response_factory=idempotent_response_factory
                    or fastapi_default_idempotency_response_factory,
                )

            return self

        container = self._container

        if container is not None:

            @self._app.middleware("http")
            async def idempotency_middleware_for_app(
                request: Request,
                call_next: Callable[[Request], Awaitable[Response]],
            ) -> Response:
                """Idempotency middleware for the app."""
                async with container(scope=Scope.REQUEST) as nested_container:
                    return await fastapi_idempotency_middleware_handler(
                        request,
                        call_next,
                        idempotency_keys_storage=await nested_container.get(AbstractIdempotencyKeysStorage),
                        idempotent_response_factory=idempotent_response_factory
                        or fastapi_default_idempotency_response_factory,
                    )

            return self

        raise EntrypointInconsistencyError(
            "Idempotency middleware cannot be setup without a Dishka container or "
            "a factory function to create the idempotency keys storage"
        )

    def setup_mcp(self, mcp: FastMCPEntrypointComponent, path: str) -> Self:
        """Setup MCP."""
        mcp_app = mcp.get_app().http_app(path=path)

        self._app.router.lifespan_context = mcp_app.lifespan

        self._app.mount(path, mcp_app)

        return self

    def setup_faststream(self, faststream: FastStreamEntrypointComponent) -> Self:  # noqa: ARG002
        """Setup FastStream."""

        # We do nothing with FastStreamEntrypointComponent here
        # because the actual integration happens outside the entrypoint
        # i.e. this way: https://faststream.ag2.ai/latest/faststream/#fastapi-plugin

        return self

    def get_app(self) -> FastAPI:
        """Get the FastAPI app."""
        return self._app

    async def run(self, *args: Any, **kwargs: Any) -> None:
        """Run the FastAPI entrypoint.

        Args:
            *args: The arguments to pass to the Server serve method.
            **kwargs: The keyword arguments to pass to the Server serve method.

        """
        await Server(Config(self._app, host=self._server_config.host, port=self._server_config.port)).serve(
            *args, **kwargs
        )
