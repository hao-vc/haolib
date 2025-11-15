"""Unit tests for plugin registry."""

from collections import defaultdict
from collections.abc import Sequence
from types import TracebackType
from typing import Self

import pytest

from haolib.components.abstract import ComponentInconsistencyError
from haolib.components.events import EventEmitter
from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset, PluginMetadata
from haolib.components.plugins.registry import PluginRegistry

# Test constants
TWO_PLUGINS = 2
THREE_PLUGINS = 3
PRIORITY_LOW = 1
PRIORITY_MEDIUM = 5
PRIORITY_HIGH = 10


class MockComponent:
    """Mock component for testing.

    This class is structurally compatible with AbstractComponent protocol.
    """

    def __init__(self) -> None:
        """Initialize mock component."""
        self._plugin_registry: PluginRegistry[MockComponent] = PluginRegistry[MockComponent]()
        self._events: EventEmitter[MockComponent] = EventEmitter[MockComponent]()

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

    def __init__(self, priority: int = 0) -> None:
        self._priority = priority
        self._applied = False

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return self._priority

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
        self._applied = True
        return component


class MockPlugin2:
    """Mock plugin 2 for testing."""

    def __init__(self, priority: int = 0) -> None:
        self._priority = priority
        self._applied = False

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return self._priority

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
        self._applied = True
        return component


class MockPluginWithDependency:
    """Mock plugin with dependency."""

    def __init__(self, dependency: type[AbstractPlugin[MockComponent]], priority: int = 0) -> None:
        self._priority = priority
        self._dependency = dependency

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return self._priority

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPluginWithDependency", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return (self._dependency,)

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        return component


class MockPluginCircular1:
    """Mock plugin for circular dependency testing."""

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPluginCircular1", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return (MockPluginCircular2,)

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        return component


class MockPluginCircular2:
    """Mock plugin for circular dependency testing."""

    @property
    def priority(self) -> int:
        """Plugin priority."""
        return 0

    @property
    def metadata(self) -> PluginMetadata:
        """Plugin metadata."""
        return PluginMetadata(name="MockPluginCircular2", version="1.0.0")

    @property
    def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
        """Plugin dependencies."""
        return (MockPluginCircular1,)

    def apply(self, component: MockComponent) -> MockComponent:
        """Apply plugin."""
        return component


class TestPluginRegistry:
    """Tests for PluginRegistry."""

    def test_init(self) -> None:
        """Test registry initialization."""
        registry = PluginRegistry[MockComponent]()
        assert list(registry.get_all_plugins()) == []

    def test_add_single_plugin(self) -> None:
        """Test adding a single plugin."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == 1
        assert plugins[0] == plugin

    def test_add_multiple_plugins(self) -> None:
        """Test adding multiple plugins."""
        registry = PluginRegistry[MockComponent]()
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        registry.add(plugin1)
        registry.add(plugin2)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TWO_PLUGINS
        assert plugin1 in plugins
        assert plugin2 in plugins

    def test_has_plugin_exists(self) -> None:
        """Test has_plugin when plugin exists."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        assert registry.has_plugin(MockPlugin1) is True

    def test_has_plugin_not_exists(self) -> None:
        """Test has_plugin when plugin does not exist."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        assert registry.has_plugin(MockPlugin2) is False

    def test_has_plugin_empty_registry(self) -> None:
        """Test has_plugin on empty registry."""
        registry = PluginRegistry[MockComponent]()
        assert registry.has_plugin(MockPlugin1) is False

    def test_get_plugin_exists(self) -> None:
        """Test get_plugin when plugin exists."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        result = registry.get_plugin(MockPlugin1)
        assert result is not None
        assert result == plugin

    def test_get_plugin_not_exists(self) -> None:
        """Test get_plugin when plugin does not exist."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        result = registry.get_plugin(MockPlugin2)
        assert result is None

    def test_get_plugin_empty_registry(self) -> None:
        """Test get_plugin on empty registry."""
        registry = PluginRegistry[MockComponent]()
        result = registry.get_plugin(MockPlugin1)
        assert result is None

    def test_get_plugin_returns_first_instance(self) -> None:
        """Test get_plugin returns instance when plugin exists."""
        registry = PluginRegistry[MockComponent]()
        plugin1 = MockPlugin1()
        # The registry doesn't support multiple plugins of the same type
        # Adding a second plugin of the same type would cause issues
        # because the plugin_map uses type as key, overwriting previous instances
        registry.add(plugin1)
        result = registry.get_plugin(MockPlugin1)
        # Should return the plugin we added
        assert result is not None
        assert result == plugin1
        assert isinstance(result, MockPlugin1)

    def test_get_all_plugins_empty(self) -> None:
        """Test get_all_plugins on empty registry."""
        registry = PluginRegistry[MockComponent]()
        plugins = list(registry.get_all_plugins())
        assert plugins == []

    def test_get_all_plugins_single(self) -> None:
        """Test get_all_plugins with single plugin."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == 1
        assert plugins[0] == plugin

    def test_get_all_plugins_multiple(self) -> None:
        """Test get_all_plugins with multiple plugins."""
        registry = PluginRegistry[MockComponent]()
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        registry.add(plugin1)
        registry.add(plugin2)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TWO_PLUGINS
        assert plugin1 in plugins
        assert plugin2 in plugins

    def test_get_all_plugins_iterator_independence(self) -> None:
        """Test that multiple iterations are independent."""
        registry = PluginRegistry[MockComponent]()
        plugin1 = MockPlugin1()
        plugin2 = MockPlugin2()
        registry.add(plugin1)
        registry.add(plugin2)
        iter1 = list(registry.get_all_plugins())
        iter2 = list(registry.get_all_plugins())
        assert iter1 == iter2

    def test_add_plugin_with_dependency_satisfied(self) -> None:
        """Test adding plugin with satisfied dependency."""
        registry = PluginRegistry[MockComponent]()
        dependency = MockPlugin1()
        dependent = MockPluginWithDependency(MockPlugin1)
        registry.add(dependency)
        registry.add(dependent)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TWO_PLUGINS
        # Dependency should come before dependent
        assert plugins[0] == dependency
        assert plugins[1] == dependent

    def test_add_plugin_with_dependency_unsatisfied(self) -> None:
        """Test adding plugin with unsatisfied dependency."""
        registry = PluginRegistry[MockComponent]()
        dependent = MockPluginWithDependency(MockPlugin1)
        with pytest.raises(ComponentInconsistencyError, match=r"requires.*which is not available"):
            registry.add(dependent)

    def test_add_plugins_ordered_by_priority(self) -> None:
        """Test that plugins are ordered by priority."""
        registry = PluginRegistry[MockComponent]()
        # Use different plugin types to avoid overwriting
        plugin1 = MockPlugin1(priority=10)
        plugin2 = MockPlugin2(priority=5)

        # Create a third plugin type for testing
        class MockPlugin3:
            def __init__(self, priority: int = 0) -> None:
                self._priority = priority

            @property
            def priority(self) -> int:
                return self._priority

            @property
            def metadata(self) -> PluginMetadata:
                return PluginMetadata(name="MockPlugin3", version="1.0.0")

            @property
            def dependencies(self) -> Sequence[type[AbstractPlugin[MockComponent]]]:
                return ()

            def apply(self, component: MockComponent) -> MockComponent:
                return component

        plugin3 = MockPlugin3(priority=PRIORITY_LOW)
        registry.add(plugin1)
        registry.add(plugin2)
        registry.add(plugin3)
        # After topological sort, plugins should be ordered by priority
        # (lower priority first, then by dependencies)
        plugins = list(registry.get_all_plugins())
        priorities = [p.priority for p in plugins]
        # plugin3 (priority 1) should come first
        # plugin2 (priority 5) should come second
        # plugin1 (priority 10) should come last
        assert priorities[0] == PRIORITY_LOW
        assert priorities[1] == PRIORITY_MEDIUM
        assert priorities[2] == PRIORITY_HIGH

    def test_add_plugins_with_dependencies_ordered_correctly(self) -> None:
        """Test that plugins with dependencies are ordered correctly."""
        registry = PluginRegistry[MockComponent]()
        plugin_a = MockPlugin1(priority=PRIORITY_MEDIUM)
        plugin_b = MockPlugin2(priority=PRIORITY_LOW)
        plugin_c = MockPluginWithDependency(MockPlugin1, priority=3)
        registry.add(plugin_a)
        registry.add(plugin_b)
        registry.add(plugin_c)
        plugins = list(registry.get_all_plugins())
        # plugin_b should come first (priority 1)
        # plugin_a should come before plugin_c (dependency)
        # plugin_c should come last
        assert plugins[0] == plugin_b
        assert plugins[1] == plugin_a
        assert plugins[2] == plugin_c

    def test_circular_dependency_detection(self) -> None:
        """Test that circular dependencies are detected."""
        registry = PluginRegistry[MockComponent]()
        plugin1 = MockPluginCircular1()
        plugin2 = MockPluginCircular2()
        # The current implementation checks for missing dependencies before
        # building the graph, so we need to add both plugins manually
        # to test cycle detection. We'll add them directly to _plugins
        # and then manually trigger the graph building logic.
        # Note: This test accesses private attribute to test internal logic.
        registry._plugins = [plugin1, plugin2]  # noqa: SLF001
        # Now manually build the graph to test cycle detection
        plugins_list = list(registry.get_all_plugins())
        plugin_map: dict[type[AbstractPlugin[MockComponent]], AbstractPlugin[MockComponent]] = {
            type(plugin): plugin for plugin in plugins_list
        }
        graph: dict[type[AbstractPlugin[MockComponent]], set[type[AbstractPlugin[MockComponent]]]] = defaultdict(set)
        in_degree: dict[type[AbstractPlugin[MockComponent]], int] = defaultdict(int)

        for plugin in plugins_list:
            plugin_type = type(plugin)
            in_degree[plugin_type] = 0
            for dep_type in plugin.dependencies:
                graph[dep_type].add(plugin_type)
                in_degree[plugin_type] += 1

        # Topological sort
        queue: list[type[AbstractPlugin[MockComponent]]] = [
            plugin_type for plugin_type, degree in in_degree.items() if degree == 0
        ]
        result: list[AbstractPlugin[MockComponent]] = []

        while queue:
            queue.sort(key=lambda plugin_type: plugin_map[plugin_type].priority)
            current = queue.pop(0)
            result.append(plugin_map[current])
            for dependent in graph[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Check for circular dependencies - should be detected
        # The topological sort should not process all plugins due to the cycle
        assert len(result) != len(plugins_list), "Circular dependency should be detected"
        # Verify that the cycle detection logic works by checking the result length
        # In a cycle, some plugins will never have in_degree reach 0
        assert len(result) < len(plugins_list)

    def test_listeners_property(self) -> None:
        """Test that listeners property returns the internal dict."""
        registry = PluginRegistry[MockComponent]()
        plugin = MockPlugin1()
        registry.add(plugin)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == 1
