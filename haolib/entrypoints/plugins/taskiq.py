"""Taskiq entrypoint plugins."""

from dishka import AsyncContainer
from dishka.integrations.taskiq import setup_dishka as setup_dishka_taskiq

from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.entrypoints.taskiq import TaskiqEntrypoint


class TaskiqDishkaPlugin(AbstractEntrypointPlugin[TaskiqEntrypoint]):
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
