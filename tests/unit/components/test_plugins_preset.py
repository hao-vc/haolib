"""Unit tests for plugin preset."""

from collections.abc import Sequence
from types import TracebackType
from typing import Self

from haolib.components.events import EventEmitter
from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset, PluginMetadata
from haolib.components.plugins.registry import PluginRegistry

# Test constants
THREE_PLUGINS = 3
TWO_PLUGINS = 2


class MockComponent:
    """Mock component for testing.

    This class is structurally compatible with AbstractComponent protocol.
    """

    def __init__(self) -> None:
        """Initialize mock component."""
        self._plugin_registry: PluginRegistry[MockComponent] = PluginRegistry[MockComponent]()
        self._events: EventEmitter[MockComponent] = EventEmitter[MockComponent]()
        self.applied_plugins: list[str] = []

    async def __aenter__(self) -> Self:
        """Enter the component context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the component context."""

    @property
    def plugin_registry(self) -> PluginRegistry[MockComponent]:
        """Plugin registry for this component."""
        return self._plugin_registry

    def use_plugin(self, plugin: AbstractPlugin[MockComponent]) -> Self:  # type: ignore[valid-type]
        """Apply a plugin to this component."""
        _ = plugin  # Mark as used
        return self

    def use_preset(self, preset: AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]]) -> Self:  # type: ignore[valid-type]
        """Apply a plugin preset to this component."""
        _ = preset  # Mark as used
        return self

    @property
    def events(self) -> EventEmitter[MockComponent]:
        """Event emitter for this component."""
        return self._events

    @property
    def version(self) -> str:
        """Component version."""
        return "1.0.0"


class MockPlugin1:
    """Mock plugin 1 for testing."""

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPlugin1", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return ()

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        component.applied_plugins.append("MockPlugin1")
        return component


class MockPlugin2:
    """Mock plugin 2 for testing."""

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPlugin2", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return ()

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        component.applied_plugins.append("MockPlugin2")
        return component


class MockPlugin3:
    """Mock plugin 3 for testing."""

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPlugin3", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return ()

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        component.applied_plugins.append("MockPlugin3")
        return component


class TestAbstractPluginPreset:
    """Tests for AbstractPluginPreset."""

    def test_init_empty(self) -> None:
        """Test creating preset with no plugins."""
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]]()  # type: ignore[type-arg]
        assert len(preset.plugins) == 0

    def test_init_single_plugin(self) -> None:
        """Test creating preset with single plugin."""
        plugin = MockPlugin1()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin)  # type: ignore[type-arg]
        assert len(preset.plugins) == 1
        assert preset.plugins[0] == plugin

    def test_init_multiple_plugins(self) -> None:
        """Test creating preset with multiple plugins."""
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        plugin3 = MockPlugin3()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin1, plugin2, plugin3)  # type: ignore[type-arg]
        assert len(preset.plugins) == THREE_PLUGINS
        assert preset.plugins[0] == plugin1
        assert preset.plugins[1] == plugin2
        assert preset.plugins[2] == plugin3

    def test_plugins_property(self) -> None:
        """Test plugins property returns the plugins."""
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin1, plugin2)  # type: ignore[type-arg]
        plugins = preset.plugins
        assert len(plugins) == TWO_PLUGINS
        assert plugins[0] == plugin1
        assert plugins[1] == plugin2

    def test_apply_empty_preset(self) -> None:
        """Test applying empty preset."""
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]]()  # type: ignore[type-arg]
        component = MockComponent()
        result = preset.apply(component)
        assert result == component
        assert len(component.applied_plugins) == 0

    def test_apply_single_plugin(self) -> None:
        """Test applying preset with single plugin."""
        plugin = MockPlugin1()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin)  # type: ignore[type-arg]
        component = MockComponent()
        result = preset.apply(component)
        assert result == component
        assert len(component.applied_plugins) == 1
        assert component.applied_plugins[0] == "MockPlugin1"

    def test_apply_multiple_plugins(self) -> None:
        """Test applying preset with multiple plugins."""
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        plugin3 = MockPlugin3()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin1, plugin2, plugin3)  # type: ignore[type-arg]
        component = MockComponent()
        result = preset.apply(component)
        assert result == component
        assert len(component.applied_plugins) == THREE_PLUGINS
        assert component.applied_plugins[0] == "MockPlugin1"
        assert component.applied_plugins[1] == "MockPlugin2"
        assert component.applied_plugins[2] == "MockPlugin3"

    def test_apply_preserves_order(self) -> None:
        """Test that plugins are applied in the order they were added."""
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin1, plugin2)  # type: ignore[type-arg]
        component = MockComponent()
        preset.apply(component)
        assert component.applied_plugins == ["MockPlugin1", "MockPlugin2"]

    def test_apply_returns_component(self) -> None:
        """Test that apply returns the component."""
        plugin = MockPlugin1()
        preset = AbstractPluginPreset[MockComponent, AbstractPlugin[MockComponent]](plugin)  # type: ignore[type-arg]
        component = MockComponent()
        result = preset.apply(component)
        assert result is component
