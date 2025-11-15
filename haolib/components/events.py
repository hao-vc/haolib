"""Events for components."""

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

if TYPE_CHECKING:
    from haolib.components.abstract import AbstractComponent


@dataclass(frozen=True)
class ComponentEventResult[T_Event: ComponentEvent, T_Result]:
    """Result of a component event.

    Example:
        ```python
        @dataclass
        class MyEvent(ComponentEvent):
            component: MyComponent
            data: str

            @property
            def identifier(self) -> str:
                return "my.event"

        async def handler(event: MyEvent) -> ComponentEventResult[MyEvent, str]:
            return ComponentEventResult(event=event, result="processed")

        result = await handler(event)
        assert result.result == "processed"
        ```

    """

    event: T_Event
    result: T_Result


@dataclass(frozen=True)
class ComponentEventListener[T_Event: ComponentEvent, T_Result]:
    """Event listener for component events."""

    handler: Callable[..., Awaitable[ComponentEventResult[T_Event, T_Result]] | ComponentEventResult[T_Event, T_Result]]
    priority: int


class ComponentEvent[T_Component: AbstractComponent](Protocol):
    """Base protocol for all component events.

    All component events must implement this protocol to ensure
    consistent structure and type safety.

    Example:
        ```python
        @dataclass
        class MyEvent:
            component: MyComponent
            data: str

            @property
            def identifier(self) -> str:
                return "my.event"

            @property
            def composer(self) -> Callable[[list[ComponentEventResult[Self, Any]]], ComponentEventResult[Self, Any]]:
                # Use last result by default
                return lambda results: results[-1] if results else ComponentEventResult(event=self, result=None)
        ```

    """

    @property
    def component(self) -> T_Component:
        """Component that emitted the event."""
        ...

    @property
    def identifier(self) -> str:
        """Event identifier. E.g. "entrypoint.startup", "entrypoint.shutdown", "storage.before_read", etc."""
        ...

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Self, Any]]], ComponentEventResult[Self, Any]]:
        """Composer function to apply to the event results.

        Default implementation returns the last result.
        Override this property to customize result composition.

        Returns:
            A function that takes a list of results and returns a single composed result.

        """
        return lambda results: results[-1] if results else ComponentEventResult[Self, Any](event=self, result=None)


class EventEmitter[T_Component: AbstractComponent]:
    """Event emitter for component events.

    Example:
        ```python
        emitter = EventEmitter[MyComponent]()

        async def handler(event: MyEvent) -> ComponentEventResult[MyEvent, str]:
            return ComponentEventResult(event=event, result="processed")

        emitter.subscribe(MyEvent, handler, priority=0)
        result = await emitter.emit(event)
        assert result.result == "processed"
        ```

    """

    def __init__(self) -> None:
        """Initialize event emitter."""
        self._listeners: dict[
            type[ComponentEvent[T_Component]], list[ComponentEventListener[ComponentEvent[T_Component], Any]]
        ] = {}

    @property
    def listeners(
        self,
    ) -> dict[type[ComponentEvent[T_Component]], list[ComponentEventListener[ComponentEvent[T_Component], Any]]]:
        """Get all listeners."""
        return self._listeners

    def subscribe[T_Event: ComponentEvent, T_Result](
        self,
        event_type: type[T_Event],
        handler: Callable[
            ..., Awaitable[ComponentEventResult[T_Event, T_Result]] | ComponentEventResult[T_Event, T_Result]
        ],
        priority: int = 0,
    ) -> None:
        """Subscribe to an event.

        Args:
            event_type: Event type
            handler: Handler function that returns ComponentEventResult (can be async or sync)
            priority: Handler priority (lower = executed first)

        Example:
            ```python
            async def handler(event: MyEvent) -> ComponentEventResult[MyEvent, str]:
                return ComponentEventResult(event=event, result="processed")

            emitter.subscribe(MyEvent, handler, priority=0)
            ```

        """
        if event_type not in self._listeners:
            self._listeners[event_type] = []  # type: ignore[assignment]

        listener = ComponentEventListener[T_Event, T_Result](handler=handler, priority=priority)
        self._listeners[event_type].append(  # type: ignore[index]
            cast("ComponentEventListener[ComponentEvent[T_Component], Any]", listener)
        )
        # Sort by priority
        self._listeners[event_type].sort(key=lambda x: x.priority)  # type: ignore[index]

    def unsubscribe[T_Event: ComponentEvent, T_Result](
        self,
        event_type: type[T_Event],
        handler: Callable[
            ..., Awaitable[ComponentEventResult[T_Event, T_Result]] | ComponentEventResult[T_Event, T_Result]
        ],
    ) -> None:
        """Unsubscribe from an event.

        Args:
            event_type: Event type
            handler: Handler function to unsubscribe

        """
        if event_type not in self._listeners:
            return

        self._listeners[event_type] = [  # type: ignore[assignment, index]
            x
            for x in self._listeners[event_type]
            if x.handler != handler  # type: ignore[index]
        ]

    async def emit[T_Event: ComponentEvent, T_Result](
        self, event: T_Event, *args: Any, **kwargs: Any
    ) -> ComponentEventResult[T_Event, T_Result]:
        """Emit an event to all subscribers.

        Args:
            event: Event
            *args: Positional arguments to pass to handlers
            **kwargs: Keyword arguments to pass to handlers

        Returns:
            Composed result from all handlers based on event's composer function.

        Example:
            ```python
            result = await emitter.emit(event)
            assert isinstance(result, ComponentEventResult)
            assert result.result is not None
            ```

        """
        event_type = type(event)
        if event_type not in self._listeners or len(self._listeners[event_type]) == 0:  # type: ignore[index]
            # Return default result if no handlers
            return ComponentEventResult[T_Event, T_Result](event=event, result=None)  # type: ignore[arg-type]

        results: list[ComponentEventResult[T_Event, T_Result]] = []

        for listener in self._listeners[event_type]:  # type: ignore[index]
            result_maybe_awaitable = listener.handler(event, *args, **kwargs)

            result = (
                await result_maybe_awaitable if inspect.isawaitable(result_maybe_awaitable) else result_maybe_awaitable
            )

            # Type check: ensure result is ComponentEventResult
            if not isinstance(result, ComponentEventResult):
                msg = f"Handler must return ComponentEventResult, got {type(result)}"
                raise TypeError(msg)

            results.append(cast("ComponentEventResult[T_Event, T_Result]", result))

        # Compose results using event's composer
        # Cast to match composer's expected type (Self vs T_Event)
        results_for_composer = [cast("ComponentEventResult[Any, Any]", r) for r in results]
        composed = event.composer(results_for_composer)
        return cast("ComponentEventResult[T_Event, T_Result]", composed)
