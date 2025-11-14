"""Events for entrypoints."""

from dataclasses import dataclass

from haolib.entrypoints.abstract import AbstractEntrypoint


@dataclass(frozen=True)
class EntrypointStartupEvent:
    """Entrypoint startup event.

    Implements ComponentEvent[AbstractEntrypoint] protocol.
    """

    component: AbstractEntrypoint
    identifier: str = "entrypoint.startup"


@dataclass(frozen=True)
class EntrypointShutdownEvent:
    """Entrypoint shutdown event.

    Implements ComponentEvent[AbstractEntrypoint] protocol.
    """

    component: AbstractEntrypoint
    identifier: str = "entrypoint.shutdown"
