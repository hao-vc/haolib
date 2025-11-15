"""FastAPI observability plugin."""

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.abstract import AbstractEntrypointPlugin
from haolib.observability.setupper import ObservabilitySetupper


class FastAPIObservabilityPlugin(AbstractEntrypointPlugin[FastAPIEntrypoint]):
    """Plugin for adding observability to FastAPI entrypoints.

    Example:
        ```python
        from haolib.observability.setupper import ObservabilitySetupper

        observability = ObservabilitySetupper().setup_logging()
        entrypoint = FastAPIEntrypoint(app=app).use_plugin(FastAPIObservabilityPlugin(observability))
        ```

    """

    def __init__(self, observability: ObservabilitySetupper) -> None:
        """Initialize the observability plugin.

        Args:
            observability: The observability setupper.

        """
        self._observability = observability

    def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
        """Apply observability to the entrypoint.

        Args:
            component: The FastAPI entrypoint to configure.

        Returns:
            The configured component.

        """
        self._observability.instrument_fastapi(component.get_app())
        return component
