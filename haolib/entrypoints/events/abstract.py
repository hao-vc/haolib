"""Events for entrypoints."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from haolib.components.events import ComponentEventResult
from haolib.entrypoints.abstract import AbstractEntrypoint


@dataclass(frozen=True)
class EntrypointStartupEvent:
    """Entrypoint startup event.

    Implements ComponentEvent[AbstractEntrypoint] protocol.
    """

    component: AbstractEntrypoint
    identifier: str = "entrypoint.startup"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function to apply to the event results."""
        return lambda results: results[-1] if results else ComponentEventResult(event=self, result=None)


@dataclass(frozen=True)
class EntrypointShutdownEvent:
    """Entrypoint shutdown event.

    Implements ComponentEvent[AbstractEntrypoint] protocol.
    """

    component: AbstractEntrypoint
    identifier: str = "entrypoint.shutdown"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function to apply to the event results."""
        return lambda results: results[-1] if results else ComponentEventResult(event=self, result=None)
