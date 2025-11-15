"""FastAPI FastMCP plugin."""

from typing import TYPE_CHECKING

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin

if TYPE_CHECKING:
    from fastmcp import FastMCP


class FastAPIFastMCPPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding FastMCP integration to FastAPI entrypoints.

    Example:
        ```python
        from fastmcp import FastMCP

        fastmcp = FastMCP()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIFastMCPPlugin(fastmcp))
        ```

    """

    def __init__(self, fastmcp: FastMCP, path: str = "/mcp") -> None:
        """Initialize the FastAPI FastMCP plugin.

        Args:
            fastmcp: The FastMCP application instance.
            path: The path where the FastMCP application will be mounted.

        """
        self._fastmcp = fastmcp
        self._path = path

    def get_fastmcp(self) -> FastMCP:
        """Get the FastMCP application instance.

        Returns:
            The FastMCP application instance.

        """
        return self._fastmcp

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply FastMCP integration to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured entrypoint.

        """
        mcp_app = self._fastmcp.http_app(path=self._path)

        component.get_app().router.lifespan_context = mcp_app.lifespan

        component.get_app().mount(self._path, mcp_app)

        return component
