"""Events for components."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from haolib.components.abstract import AbstractComponent


@dataclass(frozen=True)
class ComponentEventListener:
    """Event listener for component events."""

    handler: Callable[..., Awaitable[None] | None]
    priority: int


class ComponentEvent[T_Component: AbstractComponent](Protocol):
    """Base protocol for all component events.

    All component events must implement this protocol to ensure
    consistent structure and type safety.
    """

    @property
    def component(self) -> T_Component:
        """Component that emitted the event."""
        ...

    @property
    def identifier(self) -> str:
        """Event identifier. E.g. "entrypoint.startup", "entrypoint.shutdown", "storage.before_read", etc."""
        ...


class EventEmitter[T_Component: AbstractComponent]:
    """Event emitter for component events."""

    def __init__(self) -> None:
        """Initialize event emitter."""
        self._listeners: dict[type[ComponentEvent[T_Component]], list[ComponentEventListener]] = {}

    @property
    def listeners(self) -> dict[type[ComponentEvent[T_Component]], list[ComponentEventListener]]:
        """Get all listeners."""
        return self._listeners

    def subscribe[T_Event: ComponentEvent](
        self,
        event_type: type[T_Event],
        handler: Callable[[T_Event], Awaitable[None] | None],
        priority: int = 0,
    ) -> None:
        """Subscribe to an event.

        Args:
            event_type: Event type
            handler: Handler function (can be async or sync)
            priority: Handler priority (lower = executed first)

        """
        if event_type not in self._listeners:
            self._listeners[event_type] = []

        self._listeners[event_type].append(ComponentEventListener(handler=handler, priority=priority))
        # Sort by priority
        self._listeners[event_type].sort(key=lambda x: x.priority)

    def unsubscribe[T_Event: ComponentEvent](
        self, event_type: type[T_Event], handler: Callable[[T_Event], Awaitable[None] | None]
    ) -> None:
        """Unsubscribe from an event.

        Args:
            event_type: Event type
            handler: Handler function to unsubscribe

        """
        if event_type not in self._listeners:
            return

        self._listeners[event_type] = [x for x in self._listeners[event_type] if x.handler != handler]

    async def emit[T_Event: ComponentEvent](self, event: T_Event, *args: Any, **kwargs: Any) -> None:
        """Emit an event to all subscribers.

        Args:
            event: Event
            *args: Positional arguments to pass to handlers
            **kwargs: Keyword arguments to pass to handlers

        """
        if type(event) not in self._listeners:
            return

        for listener in self._listeners[type(event)]:
            if asyncio.iscoroutinefunction(listener.handler):
                await listener.handler(event, *args, **kwargs)
            else:
                listener.handler(event, *args, **kwargs)
