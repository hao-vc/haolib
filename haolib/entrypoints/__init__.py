"""Entrypoints for the application."""

from asyncio import TaskGroup

from haolib.entrypoints.base import Entrypoint


class HAO:
    """HAO app."""

    def __init__(self) -> None:
        """Initialize the Humanless Autonomously Orchestrated app.

        Args:
            container: The container.
            app: The FastAPI app.

        """

    async def run_entrypoints(self, entrypoints: list[Entrypoint]) -> None:
        """Run the entrypoints.

        Args:
            entrypoints: The entrypoints to run.

        """

        async with TaskGroup() as task_group:
            for entrypoint in entrypoints:
                task_group.create_task(entrypoint.run())
