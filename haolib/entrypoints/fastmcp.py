"""FastMCP entrypoint."""

from typing import TYPE_CHECKING, Any

from haolib.entrypoints.base import Entrypoint

if TYPE_CHECKING:
    from fastmcp import FastMCP


class FastMCPEntrypoint(Entrypoint):
    """FastMCP entrypoint."""

    def __init__(self, fastmcp: FastMCP, *args: Any, **kwargs: Any) -> None:
        """Initialize the FastMCP entrypoint.

        Args:
            fastmcp: The FastMCP app.
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """
        self._fastmcp = fastmcp
        self._run_args = args
        self._run_kwargs = kwargs

    def get_app(self) -> FastMCP:
        """Get the FastMCP app."""
        return self._fastmcp

    async def run(self) -> None:
        """Run the FastMCP entrypoint.

        Args:
            *args: The arguments to pass to the FastMCP run_async method.
            **kwargs: The keyword arguments to pass to the FastMCP run_async method.

        """
        await self._fastmcp.run_async(*self._run_args, **self._run_kwargs)
