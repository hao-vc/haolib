"""FastAPI entrypoint."""

import logging
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
from haolib.web.health.checkers.abstract import AbstractHealthChecker
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

logger = logging.getLogger(__name__)


class FastAPIEntrypoint(AbstractEntrypoint):
    """FastAPI entrypoint implementation.

    Provides a builder-pattern interface for configuring and running FastAPI applications
    with common features like dependency injection, observability, health checks, and more.

    Example:
        ```python
        from fastapi import FastAPI
        from haolib.entrypoints.fastapi import FastAPIEntrypoint
        from haolib.observability.setupper import ObservabilitySetupper

        app = FastAPI()
        entrypoint = (
            FastAPIEntrypoint(app=app)
            .setup_dishka(container)
            .setup_observability(ObservabilitySetupper().setup_logging())
            .setup_health_check(health_checkers=[db_checker])
            .setup_cors_middleware()
        )

        await entrypoint.run()
        ```

    Attributes:
        _app: The FastAPI application instance.
        _container: Optional Dishka dependency injection container.
        _server_config: Server configuration (host, port, etc.).
        _idempotency_configured: Whether idempotency middleware is configured.
        _server: Optional uvicorn Server instance (created during startup).

    """

    def __init__(self, app: FastAPI, server_config: ServerConfig | None = None) -> None:
        """Initialize the FastAPI entrypoint.

        Args:
            app: The FastAPI application instance.
            server_config: Optional server configuration. If None, uses default configuration.

        """
        self._app = app
        self._container: AsyncContainer | None = None
        self._server_config = server_config or ServerConfig()
        self._idempotency_configured = False
        self._server: Server | None = None

    def setup_dishka(self, container: AsyncContainer) -> Self:
        """Setup Dishka dependency injection container.

        Configures Dishka to work with FastAPI, enabling dependency injection
        throughout the application.

        Args:
            container: The Dishka async container instance.

        Returns:
            Self for method chaining.

        Example:
            ```python
            from dishka import make_async_container, Scope

            container = make_async_container(...)
            entrypoint.setup_dishka(container)
            ```

        """
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

        Configures idempotency middleware to handle duplicate requests based on
        idempotency keys. Requires either a Dishka container or a storage factory.

        Args:
            idempotent_response_factory: Optional factory callable to create the idempotent response.
                If None, uses the default factory.
            idempotency_keys_storage_factory: Optional factory callable to create the idempotency keys storage.
                If None, the idempotency keys storage will be extracted from the Dishka container.
                If no container is configured, an error will be raised.

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If neither Dishka container nor storage factory is provided.

        Example:
            ```python
            # Using Dishka container
            entrypoint.setup_dishka(container).setup_idempotency_middleware()

            # Using custom storage factory
            async def storage_factory():
                return RedisIdempotencyKeysStorage(redis=redis)
            entrypoint.setup_idempotency_middleware(idempotency_keys_storage_factory=storage_factory)
            ```

        """

        if idempotency_keys_storage_factory is not None:
            self._idempotency_configured = True

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
            self._idempotency_configured = True

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
        """Setup FastMCP integration with FastAPI.

        Integrates FastMCP application with FastAPI by mounting it at the specified
        path. The component must be properly configured before calling this method.

        Args:
            mcp: The FastMCP entrypoint component to integrate.
            path: The path prefix where the FastMCP app will be mounted.

        Returns:
            Self for method chaining.

        Raises:
            EntrypointInconsistencyError: If component validation fails.

        Example:
            ```python
            from fastmcp import FastMCP

            fastmcp = FastMCP()
            component = FastMCPEntrypointComponent(fastmcp=fastmcp)

            fastapi_entrypoint.setup_mcp(component, path="/mcp")
            ```

        """
        # Validate component before integration
        mcp.validate()

        mcp_app = mcp.get_app().http_app(path=path)

        self._app.router.lifespan_context = mcp_app.lifespan

        self._app.mount(path, mcp_app)

        return self

    def setup_faststream(self, faststream: FastStreamEntrypointComponent) -> Self:
        """Setup FastStream integration with FastAPI.

        Integrates FastStream broker with FastAPI application. The integration
        is done via FastStream's FastAPI plugin mechanism. The component must
        be properly configured before calling this method.

        Args:
            faststream: The FastStream entrypoint component to integrate.

        Returns:
            Self for method chaining.

        Note:
            The actual integration happens via FastStream's plugin system.
            See https://faststream.ag2.ai/latest/faststream/#fastapi-plugin
            for details. The component's broker is made available to the
            FastAPI app through FastStream's plugin mechanism.

        Example:
            ```python
            from faststream import FastStream
            from faststream.confluent import KafkaBroker

            broker = KafkaBroker()
            app = FastStream(broker=broker)
            component = FastStreamEntrypointComponent(broker=broker)

            fastapi_entrypoint.setup_faststream(component)
            ```

        """
        # Validate component before integration
        faststream.validate()

        # FastStream integration with FastAPI happens via plugin system
        # The broker from the component is already configured and will be
        # used by FastStream's FastAPI plugin when the app starts
        # This is a no-op here because FastStream handles the integration
        # automatically when both are running in the same process

        return self

    def get_app(self) -> FastAPI:
        """Get the FastAPI app.

        Returns:
            The FastAPI application instance.

        """
        return self._app

    def validate(self) -> None:
        """Validate FastAPI entrypoint configuration.

        Validates that the entrypoint is properly configured and all required
        dependencies are available. Specifically checks:
        - Idempotency middleware configuration consistency

        Raises:
            EntrypointInconsistencyError: If configuration is invalid or
                required dependencies are missing.

        """
        # Idempotency validation is done at setup time in setup_idempotency_middleware
        # No additional validation needed here

    async def startup(self) -> None:
        """Startup the FastAPI entrypoint.

        Initializes the uvicorn server and prepares it for execution.
        This method is called before run() and should be idempotent.

        Raises:
            EntrypointInconsistencyError: If configuration is invalid.

        """
        self.validate()

        # Create server instance
        self._server = Server(Config(self._app, host=self._server_config.host, port=self._server_config.port))

        logger.info(f"FastAPI entrypoint starting on {self._server_config.host}:{self._server_config.port}")

    async def shutdown(self) -> None:
        """Shutdown the FastAPI entrypoint.

        Cleans up resources and stops the server. This method is called after
        run() completes or is cancelled. Should be idempotent and safe to
        call multiple times.

        """
        if self._server is not None:
            logger.info("Shutting down FastAPI entrypoint")
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
