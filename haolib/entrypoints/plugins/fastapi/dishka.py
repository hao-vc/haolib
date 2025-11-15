"""FastAPI Dishka plugin."""

from collections.abc import Sequence

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka as setup_dishka_fastapi

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin


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
