"""Base entrypoint."""

from typing import Protocol


class Entrypoint(Protocol):
    """Entrypoint."""

    async def run(self) -> None:
        """Run the entrypoint."""
