"""Taskiq entrypoint plugins."""

from dishka import AsyncContainer
from dishka.integrations.taskiq import setup_dishka as setup_dishka_taskiq

from haolib.entrypoints.taskiq import TaskiqEntrypoint


class TaskiqDishkaPlugin:
    """Plugin for adding Dishka dependency injection to Taskiq entrypoints.

    Example:
        ```python
        from dishka import make_async_container

        container = make_async_container(...)
        entrypoint = TaskiqEntrypoint(broker=broker).use_plugin(TaskiqDishkaPlugin(container))
        ```

    """

    def __init__(self, container: AsyncContainer) -> None:
        """Initialize the Taskiq Dishka plugin.

        Args:
            container: The Dishka async container instance.

        """
        self._container = container

    def apply(self, entrypoint: TaskiqEntrypoint) -> TaskiqEntrypoint:
        """Apply Dishka to the entrypoint.

        Args:
            entrypoint: The Taskiq entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        setup_dishka_taskiq(self._container, entrypoint.get_broker())
        return entrypoint

    def validate(self, entrypoint: TaskiqEntrypoint) -> None:
        """Validate plugin configuration.

        Args:
            entrypoint: The entrypoint to validate against.

        """
        # No validation needed - container is required in __init__

    async def on_startup(self, entrypoint: TaskiqEntrypoint) -> None:
        """Startup hook.

        Args:
            entrypoint: The entrypoint that is starting up.

        """
        # No startup logic needed

    async def on_shutdown(self, entrypoint: TaskiqEntrypoint) -> None:
        """Shutdown hook.

        Args:
            entrypoint: The entrypoint that is shutting down.

        """
        # No shutdown logic needed
