"""FastAPI entrypoint."""

from typing import TYPE_CHECKING, Any, Self

from dishka import AsyncContainer, Scope
from dishka.integrations.fastapi import setup_dishka as setup_dishka_fastapi
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server

from haolib.configs.cors import CORSConfig
from haolib.configs.observability import ObservabilityConfig
from haolib.configs.server import ServerConfig
from haolib.entrypoints.base import Entrypoint
from haolib.entrypoints.exceptions import EntrypointsInconsistencyError
from haolib.entrypoints.fastmcp import FastMCPEntrypoint
from haolib.entrypoints.faststream import FastStreamEntrypoint
from haolib.exceptions.fastapi import register_exception_handlers
from haolib.middlewares.idempotency import IdempotencyKeysStorage, idempotency_middleware
from haolib.observability.fastapi import setup_observability_for_fastapi

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from fastapi import FastAPI, Request, Response


class FastAPIEntrypoint(Entrypoint):
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

    def setup_observability(self, observability_config: ObservabilityConfig | None = None) -> Self:
        """Setup observability."""
        if observability_config is None:
            observability_config = ObservabilityConfig()

        setup_observability_for_fastapi(self._app, config=observability_config)

        return self

    def setup_cors_middleware(self, cors_config: CORSConfig | None = None) -> Self:
        """Setup CORS middleware."""
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

    def setup_exception_handlers(self, should_observe_exceptions: bool = True) -> Self:
        """Setup exception handlers.

        Args:
            should_observe_exceptions: Whether to observe exceptions.

        """
        register_exception_handlers(self._app, should_observe_exceptions=should_observe_exceptions)

        return self

    def setup_idempotency_middleware(self) -> Self:
        """Setup idempotency middleware."""
        if self._container is None:
            raise EntrypointsInconsistencyError("Idempotency middleware cannot be setup without a Dishka container")

        container = self._container

        @self._app.middleware("http")
        async def idempotency_middleware_for_app(
            request: Request,
            call_next: Callable[[Request], Awaitable[Response]],
        ) -> Response:
            """Idempotency middleware for the app."""
            async with container(scope=Scope.REQUEST) as nested_container:
                return await idempotency_middleware(
                    request,
                    call_next,
                    await nested_container.get(IdempotencyKeysStorage),
                )

        return self

    def setup_mcp(self, mcp: FastMCPEntrypoint, path: str) -> Self:
        """Setup MCP."""
        mcp_app = mcp.get_app().http_app(path=path)

        self._app.router.lifespan_context = mcp_app.lifespan

        self._app.mount(path, mcp_app)

        return self

    def setup_faststream(self, faststream: FastStreamEntrypoint) -> Self:
        """Setup FastStream."""

        # We check for broker in FastStreamEntrypoint because the actual integration happens outside the entrypoint
        # i.e. this way: https://faststream.ag2.ai/latest/faststream/#fastapi-plugin

        faststream.get_broker()

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
