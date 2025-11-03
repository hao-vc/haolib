"""Entrypoints for the application."""

from asyncio import TaskGroup

from haolib.entrypoints.abstract import AbstractEntrypoint


class HAO:
    """HAO app."""

    def __init__(self) -> None:
        """Initialize the Humanlessly Autonomously Orchestrated app."""

    async def run_entrypoints(self, entrypoints: list[AbstractEntrypoint]) -> None:
        """Run the entrypoints.

        Args:
            entrypoints: The entrypoints to run. Must implement the AbstractEntrypoint protocol.

        """

        async with TaskGroup() as task_group:
            for entrypoint in entrypoints:
                task_group.create_task(entrypoint.run())
