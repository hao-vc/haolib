"""Test plugin registry functionality."""

from collections.abc import Iterator

from dishka import Provider, Scope, make_async_container, provide
from fastapi import FastAPI

from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.base import PluginPreset
from haolib.entrypoints.plugins.fastapi import (
    FastAPICORSMiddlewarePlugin,
    FastAPIDishkaPlugin,
    FastAPIExceptionHandlersPlugin,
    FastAPIHealthCheckPlugin,
)
from haolib.entrypoints.plugins.registry import PluginRegistry

# Constants for test values
EMPTY_PLUGIN_COUNT = 0
SINGLE_PLUGIN_COUNT = 1
DOUBLE_PLUGIN_COUNT = 2
TRIPLE_PLUGIN_COUNT = 3
FIRST_PLUGIN_INDEX = 0
SECOND_PLUGIN_INDEX = 1
THIRD_PLUGIN_INDEX = 2


class MockProvider(Provider):
    """Mock provider for testing."""

    @provide(scope=Scope.APP)
    def get_mock_value(self) -> str:
        """Get a mock value."""
        return "mock"


class TestPluginRegistryInitialization:
    """Test PluginRegistry initialization."""

    def test_init_creates_empty_registry(self) -> None:
        """Test that registry is initialized empty."""
        registry = PluginRegistry()
        assert list(registry.get_all_plugins()) == []
        assert registry.has_plugin(FastAPIDishkaPlugin) is False
        assert registry.get_plugin(FastAPIDishkaPlugin) is None


class TestPluginRegistryAdd:
    """Test PluginRegistry.add() method."""

    def test_add_registers_single_plugin(self) -> None:
        """Test that add() registers a single plugin."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.get_plugin(FastAPIDishkaPlugin) is plugin

    def test_add_registers_multiple_different_plugins(self) -> None:
        """Test that add() can register multiple different plugins."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        dishka_plugin = FastAPIDishkaPlugin(container)
        cors_plugin = FastAPICORSMiddlewarePlugin()
        exception_plugin = FastAPIExceptionHandlersPlugin({})
        registry.add(dishka_plugin)
        registry.add(cors_plugin)
        registry.add(exception_plugin)
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is True
        assert registry.has_plugin(FastAPIExceptionHandlersPlugin) is True
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TRIPLE_PLUGIN_COUNT

    def test_add_allows_duplicate_plugin_instances(self) -> None:
        """Test that add() allows adding the same plugin instance multiple times."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        registry.add(plugin)  # Add same instance again
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == DOUBLE_PLUGIN_COUNT
        assert plugins.count(plugin) == DOUBLE_PLUGIN_COUNT

    def test_add_allows_multiple_instances_of_same_type(self) -> None:
        """Test that add() allows multiple instances of the same plugin type."""
        registry = PluginRegistry()
        container1 = make_async_container(MockProvider())
        container2 = make_async_container(MockProvider())
        plugin1 = FastAPIDishkaPlugin(container1)
        plugin2 = FastAPIDishkaPlugin(container2)
        registry.add(plugin1)
        registry.add(plugin2)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == DOUBLE_PLUGIN_COUNT
        assert plugin1 in plugins
        assert plugin2 in plugins


class TestPluginRegistryHasPlugin:
    """Test PluginRegistry.has_plugin() method."""

    def test_has_plugin_returns_false_for_empty_registry(self) -> None:
        """Test that has_plugin() returns False for empty registry."""
        registry = PluginRegistry()
        assert registry.has_plugin(FastAPIDishkaPlugin) is False
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is False

    def test_has_plugin_returns_false_for_unregistered_plugin_type(self) -> None:
        """Test that has_plugin() returns False for unregistered plugin types."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container))
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is False
        assert registry.has_plugin(FastAPIExceptionHandlersPlugin) is False

    def test_has_plugin_returns_true_for_registered_plugin(self) -> None:
        """Test that has_plugin() returns True for registered plugins."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        assert registry.has_plugin(FastAPIDishkaPlugin) is True

    def test_has_plugin_returns_true_when_multiple_instances_exist(self) -> None:
        """Test that has_plugin() returns True when multiple instances of type exist."""
        registry = PluginRegistry()
        container1 = make_async_container(MockProvider())
        container2 = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container1))
        registry.add(FastAPIDishkaPlugin(container2))
        assert registry.has_plugin(FastAPIDishkaPlugin) is True


class TestPluginRegistryGetPlugin:
    """Test PluginRegistry.get_plugin() method."""

    def test_get_plugin_returns_none_for_empty_registry(self) -> None:
        """Test that get_plugin() returns None for empty registry."""
        registry = PluginRegistry()
        assert registry.get_plugin(FastAPIDishkaPlugin) is None

    def test_get_plugin_returns_none_for_unregistered_plugin_type(self) -> None:
        """Test that get_plugin() returns None for unregistered plugin types."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container))
        assert registry.get_plugin(FastAPICORSMiddlewarePlugin) is None

    def test_get_plugin_returns_registered_plugin_instance(self) -> None:
        """Test that get_plugin() returns the registered plugin instance."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        retrieved_plugin = registry.get_plugin(FastAPIDishkaPlugin)
        assert retrieved_plugin is not None
        assert retrieved_plugin is plugin

    def test_get_plugin_returns_first_matching_plugin_when_multiple_exist(self) -> None:
        """Test that get_plugin() returns first matching plugin when multiple exist."""
        registry = PluginRegistry()
        container1 = make_async_container(MockProvider())
        container2 = make_async_container(MockProvider())
        plugin1 = FastAPIDishkaPlugin(container1)
        plugin2 = FastAPIDishkaPlugin(container2)
        registry.add(plugin1)
        registry.add(plugin2)
        retrieved_plugin = registry.get_plugin(FastAPIDishkaPlugin)
        assert retrieved_plugin is plugin1  # Should return first one

    def test_get_plugin_returns_correct_type_for_different_plugins(self) -> None:
        """Test that get_plugin() returns correctly typed plugin for different types."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        dishka_plugin = FastAPIDishkaPlugin(container)
        cors_plugin = FastAPICORSMiddlewarePlugin()
        exception_plugin = FastAPIExceptionHandlersPlugin({})
        registry.add(dishka_plugin)
        registry.add(cors_plugin)
        registry.add(exception_plugin)
        retrieved_dishka = registry.get_plugin(FastAPIDishkaPlugin)
        retrieved_cors = registry.get_plugin(FastAPICORSMiddlewarePlugin)
        retrieved_exception = registry.get_plugin(FastAPIExceptionHandlersPlugin)
        assert isinstance(retrieved_dishka, FastAPIDishkaPlugin)
        assert isinstance(retrieved_cors, FastAPICORSMiddlewarePlugin)
        assert isinstance(retrieved_exception, FastAPIExceptionHandlersPlugin)
        assert retrieved_dishka is dishka_plugin
        assert retrieved_cors is cors_plugin
        assert retrieved_exception is exception_plugin

    def test_get_plugin_returns_same_instance_on_multiple_calls(self) -> None:
        """Test that get_plugin() returns same instance on multiple calls."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        retrieved1 = registry.get_plugin(FastAPIDishkaPlugin)
        retrieved2 = registry.get_plugin(FastAPIDishkaPlugin)
        assert retrieved1 is retrieved2
        assert retrieved1 is plugin


class TestPluginRegistryGetAllPlugins:
    """Test PluginRegistry.get_all_plugins() method."""

    def test_get_all_plugins_returns_empty_iterator_for_empty_registry(self) -> None:
        """Test that get_all_plugins() returns empty iterator for empty registry."""
        registry = PluginRegistry()
        plugins = list(registry.get_all_plugins())
        assert plugins == []
        assert len(plugins) == EMPTY_PLUGIN_COUNT

    def test_get_all_plugins_returns_all_registered_plugins(self) -> None:
        """Test that get_all_plugins() returns all registered plugins."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        dishka_plugin = FastAPIDishkaPlugin(container)
        cors_plugin = FastAPICORSMiddlewarePlugin()
        exception_plugin = FastAPIExceptionHandlersPlugin({})
        registry.add(dishka_plugin)
        registry.add(cors_plugin)
        registry.add(exception_plugin)
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TRIPLE_PLUGIN_COUNT
        assert dishka_plugin in plugins
        assert cors_plugin in plugins
        assert exception_plugin in plugins

    def test_get_all_plugins_returns_plugins_in_registration_order(self) -> None:
        """Test that get_all_plugins() returns plugins in registration order."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin1 = FastAPIDishkaPlugin(container)
        plugin2 = FastAPICORSMiddlewarePlugin()
        plugin3 = FastAPIExceptionHandlersPlugin({})
        registry.add(plugin1)
        registry.add(plugin2)
        registry.add(plugin3)
        plugins = list(registry.get_all_plugins())
        assert plugins[FIRST_PLUGIN_INDEX] is plugin1
        assert plugins[SECOND_PLUGIN_INDEX] is plugin2
        assert plugins[THIRD_PLUGIN_INDEX] is plugin3

    def test_get_all_plugins_returns_iterator(self) -> None:
        """Test that get_all_plugins() returns an iterator."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container))
        iterator = registry.get_all_plugins()
        assert isinstance(iterator, Iterator)

    def test_get_all_plugins_iterator_can_be_consumed_multiple_times(self) -> None:
        """Test that get_all_plugins() iterator can be consumed multiple times."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        iterator1 = registry.get_all_plugins()
        iterator2 = registry.get_all_plugins()
        plugins1 = list(iterator1)
        plugins2 = list(iterator2)
        assert len(plugins1) == SINGLE_PLUGIN_COUNT
        assert len(plugins2) == SINGLE_PLUGIN_COUNT
        assert plugins1 == plugins2

    def test_get_all_plugins_includes_duplicate_instances(self) -> None:
        """Test that get_all_plugins() includes duplicate plugin instances."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        registry.add(plugin)  # Add same instance again
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == DOUBLE_PLUGIN_COUNT
        assert plugins[FIRST_PLUGIN_INDEX] is plugin
        assert plugins[SECOND_PLUGIN_INDEX] is plugin


class TestPluginRegistryIsolation:
    """Test PluginRegistry isolation and independence."""

    def test_registries_are_independent(self) -> None:
        """Test that different registry instances are independent."""
        registry1 = PluginRegistry()
        registry2 = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry1.add(plugin)
        assert registry1.has_plugin(FastAPIDishkaPlugin) is True
        assert registry2.has_plugin(FastAPIDishkaPlugin) is False
        assert registry1.get_plugin(FastAPIDishkaPlugin) is plugin
        assert registry2.get_plugin(FastAPIDishkaPlugin) is None

    def test_registry_preserves_state_after_queries(self) -> None:
        """Test that registry state is preserved after queries."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        # Perform multiple queries
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.get_plugin(FastAPIDishkaPlugin) is plugin
        assert list(registry.get_all_plugins()) == [plugin]
        # State should still be preserved
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.get_plugin(FastAPIDishkaPlugin) is plugin


class TestPluginRegistryIntegration:
    """Test PluginRegistry integration with entrypoints."""

    def test_entrypoint_has_plugin_registry_property(self) -> None:
        """Test that entrypoint has plugin_registry property."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        assert hasattr(entrypoint, "plugin_registry")
        assert isinstance(entrypoint.plugin_registry, PluginRegistry)

    def test_plugin_registry_is_initially_empty(self) -> None:
        """Test that entrypoint's plugin registry is initially empty."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        registry = entrypoint.plugin_registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is False
        assert registry.get_plugin(FastAPIDishkaPlugin) is None
        assert list(registry.get_all_plugins()) == []

    def test_use_plugin_registers_plugin_in_registry(self) -> None:
        """Test that use_plugin() registers plugin in registry."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        entrypoint.use_plugin(FastAPIDishkaPlugin(container))
        registry = entrypoint.plugin_registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        retrieved_plugin = registry.get_plugin(FastAPIDishkaPlugin)
        assert retrieved_plugin is not None

    def test_use_plugin_registers_multiple_plugins(self) -> None:
        """Test that use_plugin() registers multiple plugins in registry."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        entrypoint.use_plugin(FastAPIDishkaPlugin(container))
        entrypoint.use_plugin(FastAPICORSMiddlewarePlugin())
        entrypoint.use_plugin(FastAPIExceptionHandlersPlugin({}))
        registry = entrypoint.plugin_registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is True
        assert registry.has_plugin(FastAPIExceptionHandlersPlugin) is True
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TRIPLE_PLUGIN_COUNT

    def test_use_plugin_registers_plugins_in_order(self) -> None:
        """Test that use_plugin() registers plugins in application order."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        dishka_plugin = FastAPIDishkaPlugin(container)
        cors_plugin = FastAPICORSMiddlewarePlugin()
        entrypoint.use_plugin(dishka_plugin)
        entrypoint.use_plugin(cors_plugin)
        registry = entrypoint.plugin_registry
        plugins = list(registry.get_all_plugins())
        assert plugins[FIRST_PLUGIN_INDEX] is dishka_plugin
        assert plugins[SECOND_PLUGIN_INDEX] is cors_plugin

    def test_use_preset_registers_all_preset_plugins(self) -> None:
        """Test that use_preset() registers all plugins from preset."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        preset: PluginPreset[FastAPIEntrypoint] = PluginPreset(
            FastAPIDishkaPlugin(container),
            FastAPICORSMiddlewarePlugin(),
            FastAPIExceptionHandlersPlugin({}),
        )
        entrypoint.use_preset(preset)
        registry = entrypoint.plugin_registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is True
        assert registry.has_plugin(FastAPIExceptionHandlersPlugin) is True
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TRIPLE_PLUGIN_COUNT

    def test_use_preset_and_use_plugin_combine_correctly(self) -> None:
        """Test that use_preset() and use_plugin() combine correctly."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        preset: PluginPreset[FastAPIEntrypoint] = PluginPreset(
            FastAPIDishkaPlugin(container),
            FastAPICORSMiddlewarePlugin(),
        )
        entrypoint.use_preset(preset)
        entrypoint.use_plugin(FastAPIExceptionHandlersPlugin({}))
        registry = entrypoint.plugin_registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPICORSMiddlewarePlugin) is True
        assert registry.has_plugin(FastAPIExceptionHandlersPlugin) is True
        plugins = list(registry.get_all_plugins())
        assert len(plugins) == TRIPLE_PLUGIN_COUNT

    def test_plugin_can_access_registry_via_entrypoint(self) -> None:
        """Test that plugins can access registry via entrypoint."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        container = make_async_container(MockProvider())
        entrypoint.use_plugin(FastAPIDishkaPlugin(container))
        # Simulate plugin checking for another plugin
        registry = entrypoint.plugin_registry
        has_dishka = registry.has_plugin(FastAPIDishkaPlugin)
        assert has_dishka is True
        retrieved_plugin = registry.get_plugin(FastAPIDishkaPlugin)
        assert retrieved_plugin is not None

    def test_registry_reflects_plugin_changes_immediately(self) -> None:
        """Test that registry reflects plugin changes immediately."""
        app = FastAPI()
        entrypoint = FastAPIEntrypoint(app=app)
        registry = entrypoint.plugin_registry
        # Initially empty
        assert registry.has_plugin(FastAPIDishkaPlugin) is False
        # Add plugin
        container = make_async_container(MockProvider())
        entrypoint.use_plugin(FastAPIDishkaPlugin(container))
        # Should be immediately available
        assert registry.has_plugin(FastAPIDishkaPlugin) is True

    def test_multiple_entrypoints_have_independent_registries(self) -> None:
        """Test that multiple entrypoints have independent registries."""
        app1 = FastAPI()
        app2 = FastAPI()
        entrypoint1 = FastAPIEntrypoint(app=app1)
        entrypoint2 = FastAPIEntrypoint(app=app2)
        container = make_async_container(MockProvider())
        entrypoint1.use_plugin(FastAPIDishkaPlugin(container))
        assert entrypoint1.plugin_registry.has_plugin(FastAPIDishkaPlugin) is True
        assert entrypoint2.plugin_registry.has_plugin(FastAPIDishkaPlugin) is False


class TestPluginRegistryEdgeCases:
    """Test PluginRegistry edge cases and error conditions."""

    def test_get_plugin_with_nonexistent_type_returns_none(self) -> None:
        """Test that get_plugin() with nonexistent type returns None."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container))
        # Query for a type that was never registered
        assert registry.get_plugin(FastAPIHealthCheckPlugin) is None

    def test_has_plugin_with_nonexistent_type_returns_false(self) -> None:
        """Test that has_plugin() with nonexistent type returns False."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        registry.add(FastAPIDishkaPlugin(container))
        assert registry.has_plugin(FastAPIHealthCheckPlugin) is False

    def test_get_all_plugins_creates_new_iterator_each_call(self) -> None:
        """Test that get_all_plugins() creates a new iterator on each call."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        # Get first iterator and consume it
        iterator1 = registry.get_all_plugins()
        plugins1 = list(iterator1)
        assert len(plugins1) == SINGLE_PLUGIN_COUNT
        # Get second iterator - should be fresh and contain all plugins
        iterator2 = registry.get_all_plugins()
        plugins2 = list(iterator2)
        assert len(plugins2) == SINGLE_PLUGIN_COUNT
        assert plugins1 == plugins2

    def test_get_all_plugins_reflects_current_state(self) -> None:
        """Test that get_all_plugins() reflects current registry state."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        # Initially empty
        assert len(list(registry.get_all_plugins())) == EMPTY_PLUGIN_COUNT
        # Add plugin
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        assert len(list(registry.get_all_plugins())) == SINGLE_PLUGIN_COUNT
        # Add another plugin
        registry.add(FastAPICORSMiddlewarePlugin())
        assert len(list(registry.get_all_plugins())) == DOUBLE_PLUGIN_COUNT

    def test_get_plugin_returns_none_after_consuming_iterator(self) -> None:
        """Test that get_plugin() still works after consuming iterator."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        # Consume iterator
        list(registry.get_all_plugins())
        # get_plugin should still work
        assert registry.get_plugin(FastAPIDishkaPlugin) is plugin

    def test_has_plugin_works_after_multiple_queries(self) -> None:
        """Test that has_plugin() works correctly after multiple queries."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        # Multiple queries should not affect state
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
        # Should still work
        assert registry.get_plugin(FastAPIDishkaPlugin) is plugin

    def test_get_all_plugins_iterator_is_independent(self) -> None:
        """Test that multiple iterators are independent."""
        registry = PluginRegistry()
        container = make_async_container(MockProvider())
        plugin1 = FastAPIDishkaPlugin(container)
        plugin2 = FastAPICORSMiddlewarePlugin()
        registry.add(plugin1)
        registry.add(plugin2)
        # Create two iterators
        iterator1 = registry.get_all_plugins()
        iterator2 = registry.get_all_plugins()
        # Consume first iterator partially
        next(iterator1)
        # Second iterator should still have all plugins
        plugins2 = list(iterator2)
        assert len(plugins2) == DOUBLE_PLUGIN_COUNT
        assert plugin1 in plugins2
        assert plugin2 in plugins2

    def test_registry_handles_empty_plugin_list_gracefully(self) -> None:
        """Test that registry handles operations on empty list gracefully."""
        registry = PluginRegistry()
        # All operations should work on empty registry
        assert registry.has_plugin(FastAPIDishkaPlugin) is False
        assert registry.get_plugin(FastAPIDishkaPlugin) is None
        assert list(registry.get_all_plugins()) == []
        # Should still work after these operations
        container = make_async_container(MockProvider())
        plugin = FastAPIDishkaPlugin(container)
        registry.add(plugin)
        assert registry.has_plugin(FastAPIDishkaPlugin) is True
