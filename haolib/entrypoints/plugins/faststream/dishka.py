"""FastStream Dishka plugin."""

from dishka import AsyncContainer
from dishka.integrations.faststream import setup_dishka

from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
)
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin


class FastStreamDishkaPlugin(AbstractEntrypointPlugin[FastStreamEntrypoint]):
    """Plugin for adding Dishka dependency injection to FastStream entrypoints.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        entrypoint = FastStreamEntrypoint(app=app).use_plugin(FastStreamDishkaPlugin(container))
        ```

    """

    def __init__(self, container: AsyncContainer) -> None:
        """Initialize the FastStream Dishka plugin.

        Args:
            container: The Dishka async container instance.

        """
        self._container = container

    def apply(self, component: FastStreamEntrypoint) -> FastStreamEntrypoint:
        """Apply Dishka to the entrypoint.

        Args:
            component: The FastStream entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        setup_dishka(container=self._container, app=component.get_app(), finalize_container=False)
        return component
