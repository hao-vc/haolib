"""FastAPI idempotency plugin."""

from collections.abc import Awaitable, Callable

from dishka import Scope
from fastapi import Request, Response

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.entrypoints.plugins.fastapi.dishka import FastAPIDishkaPlugin
from haolib.web.idempotency.fastapi import (
    fastapi_default_idempotency_response_factory,
    fastapi_idempotency_middleware_handler,
)
from haolib.web.idempotency.storages.abstract import AbstractIdempotencyKeysStorage


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
