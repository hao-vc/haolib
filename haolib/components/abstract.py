"""Abstract component."""

from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self

from haolib.components.events import EventEmitter
from haolib.components.plugins.registry import PluginRegistry

if TYPE_CHECKING:
    from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset


class ComponentInconsistencyError(Exception):
    """Component inconsistency error.

    Raised when a component is misconfigured, has missing dependencies, or is in an inconsistent state.

    """


class AbstractComponent[T_Plugin: AbstractPlugin, T_PluginPreset: AbstractPluginPreset](Protocol):
    """Abstract component."""

    async def __aenter__(self) -> Self:
        """Enter the component context."""
        ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the component context."""
        ...

    @property
    def plugin_registry(self) -> PluginRegistry[Self]:
        """Plugin registry for this component."""
        ...

    def use_plugin(self, plugin: T_Plugin) -> Self:
        """Apply a plugin to this component."""
        ...

    def use_preset(self, preset: T_PluginPreset) -> Self:
        """Apply a plugin preset to this component."""
        ...

    @property
    def events(self) -> EventEmitter[Self]:
        """Event emitter for this component.

        Plugins can subscribe to events in their apply() method.

        Example:
            ```python
            def apply(self, component: FastAPIEntrypoint) -> FastAPIEntrypoint:
                component.events.subscribe("startup", self._on_startup, priority=10)
                component.events.subscribe("shutdown", self._on_shutdown, priority=10)
                return component
            ```

        """
        ...
