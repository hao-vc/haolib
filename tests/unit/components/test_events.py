"""Unit tests for event system."""

import asyncio
from collections.abc import Callable
from dataclasses import FrozenInstanceError, dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self

import pytest

from haolib.components.events import (
    ComponentEventListener,
    ComponentEventResult,
    EventEmitter,
)
from haolib.components.plugins.registry import PluginRegistry

if TYPE_CHECKING:
    from haolib.components.plugins.abstract import AbstractPlugin, AbstractPluginPreset

# Test constants
PRIORITY_LOW = 1
PRIORITY_MEDIUM = 5
PRIORITY_HIGH = 10
PRIORITY_DEFAULT = 0
TEST_VALUE = 42
TWO_HANDLERS = 2
HANDLER_RESULT_2 = 2
AGGREGATE_SUM = 60


class MockComponent:
    """Mock component for testing.

    This class is structurally compatible with AbstractComponent protocol.
    Pyright may complain about structural typing, but mypy correctly recognizes it.
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


@dataclass(frozen=True)
class MockEvent:
    """Mock event for testing."""

    component: MockComponent
    value: int = 0

    @property
    def identifier(self) -> str:
        """Event identifier."""
        return "mock.event"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[MockEvent, Any]]], ComponentEventResult[MockEvent, Any]]:  # pyright: ignore[reportInvalidTypeArguments]
        """Composer function for mock event."""
        return lambda results: results[-1] if results else ComponentEventResult(event=self, result=None)


class TestComponentEventResult:
    """Tests for ComponentEventResult."""

    def test_result_creation(self) -> None:
        """Test creating event result."""
        component = MockComponent()
        event = MockEvent(component=component, value=TEST_VALUE)
        result = ComponentEventResult(event=event, result="test_result")
        assert result.event == event
        assert result.result == "test_result"

    def test_result_frozen(self) -> None:
        """Test that result is frozen (immutable)."""
        component = MockComponent()
        event = MockEvent(component=component)
        result = ComponentEventResult(event=event, result="test")
        with pytest.raises(FrozenInstanceError):
            result.result = "modified"  # type: ignore[misc]

    def test_result_equality(self) -> None:
        """Test result equality."""
        component = MockComponent()
        event = MockEvent(component=component)
        result1 = ComponentEventResult(event=event, result="test")
        result2 = ComponentEventResult(event=event, result="test")
        result3 = ComponentEventResult(event=event, result="different")
        assert result1 == result2
        assert result1 != result3

    def test_result_hash(self) -> None:
        """Test that result is hashable."""
        component = MockComponent()
        event = MockEvent(component=component)
        result1 = ComponentEventResult(event=event, result="test")
        result2 = ComponentEventResult(event=event, result="test")
        result3 = ComponentEventResult(event=event, result="different")
        assert hash(result1) == hash(result2)
        assert hash(result1) != hash(result3)


class TestComponentEventListener:
    """Tests for ComponentEventListener."""

    def test_listener_creation(self) -> None:
        """Test creating event listener."""
        component = MockComponent()
        _event = MockEvent(component=component)

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        listener = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        assert listener.handler == handler
        assert listener.priority == PRIORITY_HIGH

    def test_listener_frozen(self) -> None:
        """Test that listener is frozen (immutable)."""
        component = MockComponent()
        _event = MockEvent(component=component)

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        listener = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        with pytest.raises(FrozenInstanceError):
            listener.priority = 20  # type: ignore[misc]

    def test_listener_equality(self) -> None:
        """Test listener equality."""
        component = MockComponent()
        _event = MockEvent(component=component)

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        listener1 = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        listener2 = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        listener3 = ComponentEventListener(handler=handler, priority=20)
        assert listener1 == listener2
        assert listener1 != listener3

    def test_listener_hash(self) -> None:
        """Test that listener is hashable."""
        component = MockComponent()
        _event = MockEvent(component=component)

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        listener1 = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        listener2 = ComponentEventListener(handler=handler, priority=PRIORITY_HIGH)
        listener3 = ComponentEventListener(handler=handler, priority=20)
        assert hash(listener1) == hash(listener2)
        assert hash(listener1) != hash(listener3)


class TestEventEmitter:
    """Tests for EventEmitter."""

    def test_init(self) -> None:
        """Test emitter initialization."""
        emitter = EventEmitter[MockComponent]()
        assert emitter.listeners == {}

    def test_listeners_property(self) -> None:
        """Test listeners property."""
        emitter = EventEmitter[MockComponent]()
        assert isinstance(emitter.listeners, dict)
        assert len(emitter.listeners) == 0

    def test_subscribe_new_event_type(self) -> None:
        """Test subscribing to new event type."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="processed")

        emitter.subscribe(MockEvent, handler)
        assert MockEvent in emitter.listeners
        assert len(emitter.listeners[MockEvent]) == 1  # type: ignore[index]
        assert emitter.listeners[MockEvent][0].handler == handler  # type: ignore[index]

    def test_subscribe_multiple_handlers(self) -> None:
        """Test subscribing multiple handlers to same event."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            return ComponentEventResult(event=event, result=1)

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            return ComponentEventResult(event=event, result=2)

        emitter.subscribe(MockEvent, handler1)
        emitter.subscribe(MockEvent, handler2)
        assert len(emitter.listeners[MockEvent]) == TWO_HANDLERS  # type: ignore[index]

    def test_subscribe_priority_ordering(self) -> None:
        """Test that handlers are ordered by priority."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            return ComponentEventResult(event=event, result=1)

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            return ComponentEventResult(event=event, result=2)

        def handler3(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            return ComponentEventResult(event=event, result=3)

        emitter.subscribe(MockEvent, handler1, priority=PRIORITY_HIGH)
        emitter.subscribe(MockEvent, handler2, priority=PRIORITY_MEDIUM)
        emitter.subscribe(MockEvent, handler3, priority=PRIORITY_LOW)
        assert emitter.listeners[MockEvent][0].priority == PRIORITY_LOW  # type: ignore[index]
        assert emitter.listeners[MockEvent][1].priority == PRIORITY_MEDIUM  # type: ignore[index]
        assert emitter.listeners[MockEvent][2].priority == PRIORITY_HIGH  # type: ignore[index]

    def test_subscribe_default_priority(self) -> None:
        """Test that default priority is 0."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        emitter.subscribe(MockEvent, handler)
        assert emitter.listeners[MockEvent][0].priority == PRIORITY_DEFAULT  # type: ignore[index]

    def test_unsubscribe_existing_handler(self) -> None:
        """Test unsubscribing existing handler."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        emitter.subscribe(MockEvent, handler)
        assert len(emitter.listeners[MockEvent]) == 1  # type: ignore[index]
        emitter.unsubscribe(MockEvent, handler)
        assert len(emitter.listeners[MockEvent]) == 0  # type: ignore[index]

    def test_unsubscribe_non_existing_handler(self) -> None:
        """Test unsubscribing non-existing handler."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test1")

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test2")

        emitter.subscribe(MockEvent, handler1)
        emitter.unsubscribe(MockEvent, handler2)
        assert len(emitter.listeners[MockEvent]) == 1  # type: ignore[index]

    def test_unsubscribe_non_existing_event_type(self) -> None:
        """Test unsubscribing from non-existing event type."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test")

        emitter.unsubscribe(MockEvent, handler)
        # Should not raise

    def test_unsubscribe_one_of_multiple_handlers(self) -> None:
        """Test unsubscribing one handler when multiple exist."""
        emitter = EventEmitter[MockComponent]()
        _component = MockComponent()

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test1")

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            return ComponentEventResult(event=event, result="test2")

        emitter.subscribe(MockEvent, handler1)
        emitter.subscribe(MockEvent, handler2)
        assert len(emitter.listeners[MockEvent]) == TWO_HANDLERS  # type: ignore[index]
        emitter.unsubscribe(MockEvent, handler1)
        assert len(emitter.listeners[MockEvent]) == 1  # type: ignore[index]
        assert emitter.listeners[MockEvent][0].handler == handler2  # type: ignore[index]

    @pytest.mark.asyncio
    async def test_emit_no_listeners(self) -> None:
        """Test emitting event with no listeners."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, Any] = await emitter.emit(event)
        assert isinstance(result, ComponentEventResult)
        assert result.event == event
        assert result.result is None

    @pytest.mark.asyncio
    async def test_emit_sync_handler(self) -> None:
        """Test emitting event to sync handler."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        handler_called = False
        received_event: MockEvent | None = None

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            nonlocal handler_called, received_event
            handler_called = True
            received_event = event
            return ComponentEventResult(event=event, result=TEST_VALUE)

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component, value=TEST_VALUE)
        result: ComponentEventResult[MockEvent, int] = await emitter.emit(event)
        assert handler_called is True
        assert received_event is not None
        assert received_event.value == TEST_VALUE
        assert result.result == TEST_VALUE

    @pytest.mark.asyncio
    async def test_emit_async_handler(self) -> None:
        """Test emitting event to async handler."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        handler_called = False
        received_event: MockEvent | None = None

        async def handler(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            nonlocal handler_called, received_event
            handler_called = True
            received_event = event
            await asyncio.sleep(0.01)
            return ComponentEventResult(event=event, result=TEST_VALUE)

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component, value=TEST_VALUE)
        result: ComponentEventResult[MockEvent, int] = await emitter.emit(event)
        assert handler_called is True
        assert received_event is not None
        assert received_event.value == TEST_VALUE
        assert result.result == TEST_VALUE

    @pytest.mark.asyncio
    async def test_emit_multiple_handlers(self) -> None:
        """Test emitting event to multiple handlers."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        calls: list[int] = []

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            calls.append(1)
            return ComponentEventResult(event=event, result=1)

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            calls.append(2)
            return ComponentEventResult(event=event, result=2)

        emitter.subscribe(MockEvent, handler1)
        emitter.subscribe(MockEvent, handler2)
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, int] = await emitter.emit(event)
        assert len(calls) == TWO_HANDLERS
        assert 1 in calls
        assert HANDLER_RESULT_2 in calls
        # Composer returns last result by default
        assert result.result == HANDLER_RESULT_2

    @pytest.mark.asyncio
    async def test_emit_handlers_executed_by_priority(self) -> None:
        """Test that handlers are executed in priority order."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        call_order: list[int] = []

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            call_order.append(1)
            return ComponentEventResult(event=event, result=1)

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            call_order.append(2)
            return ComponentEventResult(event=event, result=2)

        def handler3(event: MockEvent) -> ComponentEventResult[MockEvent, int]:
            call_order.append(3)
            return ComponentEventResult(event=event, result=3)

        emitter.subscribe(MockEvent, handler1, priority=PRIORITY_HIGH)
        emitter.subscribe(MockEvent, handler2, priority=PRIORITY_MEDIUM)
        emitter.subscribe(MockEvent, handler3, priority=PRIORITY_LOW)
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, int] = await emitter.emit(event)
        assert call_order == [3, 2, 1]
        # Composer returns last result by default
        assert result.result == 1

    @pytest.mark.asyncio
    async def test_emit_with_args(self) -> None:
        """Test emitting event with additional args."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        received_args: tuple = ()
        received_kwargs: dict = {}

        def handler(
            event: MockEvent, *args: object, **kwargs: object
        ) -> ComponentEventResult[MockEvent, dict[str, Any]]:
            nonlocal received_args, received_kwargs
            received_args = args
            received_kwargs = kwargs  # type: ignore[assignment]
            return ComponentEventResult(event=event, result={"args": args, "kwargs": kwargs})

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, dict[str, Any]] = await emitter.emit(
            event, "arg1", "arg2", key1="value1", key2="value2"
        )
        assert received_args == ("arg1", "arg2")
        assert received_kwargs == {"key1": "value1", "key2": "value2"}
        assert result.result["args"] == ("arg1", "arg2")
        assert result.result["kwargs"] == {"key1": "value1", "key2": "value2"}

    @pytest.mark.asyncio
    async def test_emit_async_handler_with_args(self) -> None:
        """Test emitting event to async handler with args."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        received_args: tuple = ()
        received_kwargs: dict = {}

        async def handler(
            event: MockEvent, *args: object, **kwargs: object
        ) -> ComponentEventResult[MockEvent, dict[str, Any]]:
            nonlocal received_args, received_kwargs
            received_args = args
            received_kwargs = kwargs  # type: ignore[assignment]
            return ComponentEventResult(event=event, result={"args": args, "kwargs": kwargs})

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, dict[str, Any]] = await emitter.emit(event, "arg1", key1="value1")
        assert received_args == ("arg1",)
        assert received_kwargs == {"key1": "value1"}
        assert result.result["args"] == ("arg1",)
        assert result.result["kwargs"] == {"key1": "value1"}

    @pytest.mark.asyncio
    async def test_emit_mixed_sync_async_handlers(self) -> None:
        """Test emitting event to mix of sync and async handlers."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        calls: list[str] = []

        def sync_handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            calls.append("sync")
            return ComponentEventResult(event=event, result="sync")

        async def async_handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            calls.append("async")
            await asyncio.sleep(0.01)
            return ComponentEventResult(event=event, result="async")

        emitter.subscribe(MockEvent, sync_handler)
        emitter.subscribe(MockEvent, async_handler)
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, str] = await emitter.emit(event)
        assert len(calls) == TWO_HANDLERS
        assert "sync" in calls
        assert "async" in calls
        # Composer returns last result by default
        assert result.result == "async"

    @pytest.mark.asyncio
    async def test_emit_handler_exception(self) -> None:
        """Test that handler exceptions are propagated."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            raise ValueError("Handler error")

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component)
        with pytest.raises(ValueError, match="Handler error"):
            await emitter.emit(event)

    @pytest.mark.asyncio
    async def test_emit_async_handler_exception(self) -> None:
        """Test that async handler exceptions are propagated."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        async def handler(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            raise ValueError("Async handler error")

        emitter.subscribe(MockEvent, handler)
        event = MockEvent(component=component)
        with pytest.raises(ValueError, match="Async handler error"):
            await emitter.emit(event)

    @pytest.mark.asyncio
    async def test_emit_multiple_handlers_one_fails(self) -> None:
        """Test emitting when one handler fails."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        calls: list[str] = []

        def handler1(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            calls.append("handler1")
            return ComponentEventResult(event=event, result="handler1")

        def handler2(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            raise ValueError("Handler2 error")

        def handler3(event: MockEvent) -> ComponentEventResult[MockEvent, str]:
            calls.append("handler3")
            return ComponentEventResult(event=event, result="handler3")

        emitter.subscribe(MockEvent, handler1)
        emitter.subscribe(MockEvent, handler2)
        emitter.subscribe(MockEvent, handler3)
        event = MockEvent(component=component)
        with pytest.raises(ValueError, match="Handler2 error"):
            await emitter.emit(event)
        # handler1 should have been called before handler2 failed
        assert "handler1" in calls
        assert "handler3" not in calls

    @pytest.mark.asyncio
    async def test_emit_handler_returns_wrong_type(self) -> None:
        """Test that handler returning wrong type raises TypeError."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        def handler(event: MockEvent) -> str:  # type: ignore[return-type]
            return "not a ComponentEventResult"

        emitter.subscribe(MockEvent, handler)  # type: ignore[arg-type]
        event = MockEvent(component=component)
        with pytest.raises(TypeError, match="Handler must return ComponentEventResult"):
            await emitter.emit(event)

    @pytest.mark.asyncio
    async def test_emit_handler_returns_none(self) -> None:
        """Test that handler returning None raises TypeError."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        def handler(event: MockEvent) -> None:  # type: ignore[return-type]
            return None

        emitter.subscribe(MockEvent, handler)  # type: ignore[arg-type]
        event = MockEvent(component=component)
        with pytest.raises(TypeError, match="Handler must return ComponentEventResult"):
            await emitter.emit(event)

    @pytest.mark.asyncio
    async def test_emit_custom_composer(self) -> None:
        """Test emitting event with custom composer."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        @dataclass(frozen=True)
        class CustomEvent:
            """Custom event with custom composer."""

            component: MockComponent
            value: int = 0

            @property
            def identifier(self) -> str:
                """Event identifier."""
                return "custom.event"

            @property
            def composer(
                self,
            ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
                """Custom composer that returns first result."""
                return lambda results: results[0] if results else ComponentEventResult(event=self, result=None)

        def handler1(event: CustomEvent) -> ComponentEventResult[CustomEvent, int]:
            return ComponentEventResult(event=event, result=1)

        def handler2(event: CustomEvent) -> ComponentEventResult[CustomEvent, int]:
            return ComponentEventResult(event=event, result=2)

        emitter.subscribe(CustomEvent, handler1)
        emitter.subscribe(CustomEvent, handler2)
        event = CustomEvent(component=component)
        result: ComponentEventResult[CustomEvent, int] = await emitter.emit(event)
        # Custom composer returns first result
        assert result.result == 1

    @pytest.mark.asyncio
    async def test_emit_composer_aggregates_results(self) -> None:
        """Test emitting event with composer that aggregates results."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        @dataclass(frozen=True)
        class AggregateEvent:
            """Event with aggregating composer."""

            component: MockComponent
            value: int = 0

            @property
            def identifier(self) -> str:
                """Event identifier."""
                return "aggregate.event"

            @property
            def composer(
                self,
            ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
                """Composer that sums all results."""
                return (
                    lambda results: ComponentEventResult(
                        event=self, result=sum(r.result for r in results if isinstance(r.result, int))
                    )
                    if results
                    else ComponentEventResult(event=self, result=0)
                )

        def handler1(event: AggregateEvent) -> ComponentEventResult[AggregateEvent, int]:
            return ComponentEventResult(event=event, result=10)

        def handler2(event: AggregateEvent) -> ComponentEventResult[AggregateEvent, int]:
            return ComponentEventResult(event=event, result=20)

        def handler3(event: AggregateEvent) -> ComponentEventResult[AggregateEvent, int]:
            return ComponentEventResult(event=event, result=30)

        emitter.subscribe(AggregateEvent, handler1)
        emitter.subscribe(AggregateEvent, handler2)
        emitter.subscribe(AggregateEvent, handler3)
        event = AggregateEvent(component=component)
        result: ComponentEventResult[AggregateEvent, int] = await emitter.emit(event)
        # Composer sums all results
        assert result.result == AGGREGATE_SUM

    @pytest.mark.asyncio
    async def test_emit_composer_empty_results(self) -> None:
        """Test composer with empty results list."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()

        @dataclass(frozen=True)
        class EmptyResultsEvent:
            """Event that tests empty results handling."""

            component: MockComponent

            @property
            def identifier(self) -> str:
                """Event identifier."""
                return "empty.results.event"

            @property
            def composer(
                self,
            ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
                """Composer that handles empty results."""
                return lambda results: ComponentEventResult(event=self, result="empty") if not results else results[-1]

        event = EmptyResultsEvent(component=component)
        # When no handlers, emit returns default result with None, not composer result
        result: ComponentEventResult[EmptyResultsEvent, Any] = await emitter.emit(event)
        assert result.event == event
        assert result.result is None

    @pytest.mark.asyncio
    async def test_emit_composer_returns_default_when_no_handlers(self) -> None:
        """Test that composer returns default when no handlers are registered."""
        emitter = EventEmitter[MockComponent]()
        component = MockComponent()
        event = MockEvent(component=component)
        result: ComponentEventResult[MockEvent, Any] = await emitter.emit(event)
        assert result.event == event
        assert result.result is None

    def test_composer_with_empty_results(self) -> None:
        """Test composer property with empty results list."""
        component = MockComponent()
        event = MockEvent(component=component)
        composer = event.composer
        # Call composer with empty list to test the else branch
        result = composer([])
        assert isinstance(result, ComponentEventResult)
        assert result.event == event
        assert result.result is None
