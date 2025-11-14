"""FastAPI entrypoint plugins."""

from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from dishka import AsyncContainer, Scope
from dishka.integrations.fastapi import setup_dishka as setup_dishka_fastapi
from dishka.integrations.faststream import setup_dishka as setup_dishka_faststream
from fastapi import Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from haolib.configs.cors import CORSConfig
from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
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
    from fastmcp import FastMCP
    from faststream._internal.broker import BrokerUsecase as BrokerType


class FastAPIDishkaPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding Dishka dependency injection to FastAPI entrypoints.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIDishkaPlugin(container))
        ```

    """

    @property
    def priority(self) -> int:
        """Plugin priority for ordering.

        Lower values execute first. Default is 0.
        Plugins with same priority execute in application order.
        """
        return 0

    @property
    def dependencies(self) -> Sequence[type[AbstractEntrypointPlugin[FastAPIEntrypoint]]]:
        """Required plugin types that must be applied before this plugin.

        Returns:
            Sequence of plugin types that this plugin depends on.

        """
        return ()

    def __init__(self, container: AsyncContainer) -> None:
        """Initialize the Dishka plugin.

        Args:
            container: The Dishka async container instance.

        """
        self._container = container

    def get_container(self) -> AsyncContainer:
        """Get the Dishka container instance.

        Returns:
            The Dishka container instance.

        """
        return self._container

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply Dishka to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        setup_dishka_fastapi(self._container, component.get_app())
        component._container = self._container  # noqa: SLF001
        return component


class FastAPIObservabilityPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding observability to FastAPI entrypoints.

    Example:
        ```python
        from haolib.observability.setupper import ObservabilitySetupper

        observability = ObservabilitySetupper().setup_logging()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIObservabilityPlugin(observability))
        ```

    """

    def __init__(self, observability: ObservabilitySetupper) -> None:
        """Initialize the observability plugin.

        Args:
            observability: The observability setupper.

        """
        self._observability = observability

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply observability to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured component.

        """
        self._observability.instrument_fastapi(component.get_app())
        return component


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


class FastAPIExceptionHandlersPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding exception handlers to FastAPI entrypoints.

    Example:
        ```python
        exception_handlers = {
            ValueError: lambda request, exc: JSONResponse(status_code=400, content={"error": str(exc)})
        }
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIExceptionHandlersPlugin(exception_handlers))
        ```

    """

    def __init__(self, exception_handlers: dict[type[Exception], Callable[..., Any]] | None = None) -> None:
        """Initialize the exception handlers plugin.

        Args:
            exception_handlers: Dictionary mapping exception types to handler functions.
                If None, uses default handlers for Exception and FastAPIBaseException.

        """
        if exception_handlers is None:
            exception_handlers = {
                Exception: fastapi_unknown_exception_handler,
                FastAPIBaseException: fastapi_base_exception_handler,
            }
        self._exception_handlers = exception_handlers

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply exception handlers to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        for exception, handler in self._exception_handlers.items():
            component.get_app().add_exception_handler(exception, handler)
        return component


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


class FastAPIIdempotencyMiddlewarePlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding idempotency middleware to FastAPI entrypoints.

    Requires either a Dishka container (via FastAPIDishkaPlugin) or a storage factory.

    Example:
        ```python
        # Using Dishka container
        entrypoint = (
            FastAPIEntrypoint(app=app)
            .use_plugin(FastAPIDishkaPlugin(container))
            .use_plugin(FastAPIIdempotencyMiddlewarePlugin())
        )

        # Using custom storage factory
        async def storage_factory():
            return RedisIdempotencyKeysStorage(redis=redis)
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(
            FastAPIIdempotencyMiddlewarePlugin(idempotency_keys_storage_factory=storage_factory)
        )
        ```

    """

    def __init__(
        self,
        idempotent_response_factory: Callable[[Request, AbstractIdempotencyKeysStorage], Awaitable[Response]]
        | None = None,
        idempotency_keys_storage_factory: Callable[[], Awaitable[AbstractIdempotencyKeysStorage]] | None = None,
    ) -> None:
        """Initialize the idempotency middleware plugin.

        Args:
            idempotent_response_factory: Optional factory callable to create the idempotent response.
                If None, uses the default factory.
            idempotency_keys_storage_factory: Optional factory callable to create the idempotency keys storage.
                If None, the idempotency keys storage will be extracted from the Dishka container.

        """
        self._idempotent_response_factory = idempotent_response_factory or fastapi_default_idempotency_response_factory
        self._idempotency_keys_storage_factory = idempotency_keys_storage_factory

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply idempotency middleware to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        Raises:
            EntrypointInconsistencyError: If neither Dishka container nor storage factory is provided.

        """
        if self._idempotency_keys_storage_factory is not None:
            component._idempotency_configured = True  # noqa: SLF001
            storage_factory = self._idempotency_keys_storage_factory

            @component.get_app().middleware("http")
            async def idempotency_middleware_for_app(
                request: Request,
                call_next: Callable[[Request], Awaitable[Response]],
            ) -> Response:
                """Idempotency middleware for the app."""
                return await fastapi_idempotency_middleware_handler(
                    request,
                    call_next,
                    idempotency_keys_storage=await storage_factory(),
                    idempotent_response_factory=self._idempotent_response_factory,
                )

            return component

        # Check if container exists or if DishkaPlugin is already in registry
        fastapi_dishka_plugin = component.plugin_registry.get_plugin(FastAPIDishkaPlugin)

        container = fastapi_dishka_plugin.get_container() if fastapi_dishka_plugin is not None else None

        # Check if DishkaPlugin is already applied using the public registry
        has_dishka_plugin = component.plugin_registry.has_plugin(FastAPIDishkaPlugin)

        if container is None and not has_dishka_plugin:
            # No container and no DishkaPlugin to add one - validate immediately
            raise EntrypointInconsistencyError(
                "Idempotency middleware cannot be setup without a Dishka container or "
                "a factory function to create the idempotency keys storage"
            )

        # If container is available now, set up middleware immediately
        if container is not None:

            @component.get_app().middleware("http")
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
                        idempotent_response_factory=self._idempotent_response_factory,
                    )

            component._idempotency_configured = True  # noqa: SLF001

        component._idempotency_configured = True  # noqa: SLF001
        return component


class FastAPIFastStreamPlugin[T_BrokerType: BrokerType](AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding FastStream integration to FastAPI entrypoints.

    Example:
        ```python
        from fastapi import FastAPI
        from faststream.confluent.fastapi import KafkaRouter

        router = KafkaRouter()
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIFastStreamPlugin(router=router))
        ```

    """

    def __init__(
        self,
        broker: T_BrokerType,
    ) -> None:
        """Initialize the FastAPI FastStream plugin.

        Args:
            broker: The FastStream broker instance.

        """
        self._broker = broker

    def get_broker(self) -> T_BrokerType:
        """Get the FastStream broker instance.

        Returns:
            The FastStream broker instance.

        """
        return self._broker

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply FastStream integration to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """

        fastapi_dishka_plugin = component.plugin_registry.get_plugin(FastAPIDishkaPlugin)
        if fastapi_dishka_plugin is not None:
            setup_dishka_faststream(container=fastapi_dishka_plugin.get_container(), broker=self._broker)

        return component


class FastAPIFastMCPPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding FastMCP integration to FastAPI entrypoints.

    Example:
        ```python
        from fastmcp import FastMCP

        fastmcp = FastMCP()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIFastMCPPlugin(fastmcp))
        ```

    """

    def __init__(self, fastmcp: FastMCP, path: str = "/mcp") -> None:
        """Initialize the FastAPI FastMCP plugin.

        Args:
            fastmcp: The FastMCP application instance.
            path: The path where the FastMCP application will be mounted.

        """
        self._fastmcp = fastmcp
        self._path = path

    def get_fastmcp(self) -> FastMCP:
        """Get the FastMCP application instance.

        Returns:
            The FastMCP application instance.

        """
        return self._fastmcp

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply FastMCP integration to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        mcp_app = self._fastmcp.http_app(path=self._path)

        component.get_app().router.lifespan_context = mcp_app.lifespan

        component.get_app().mount(self._path, mcp_app)

        return component
