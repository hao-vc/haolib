"""Base entrypoint."""

import abc
from collections.abc import Callable
from typing import Any, Protocol, Self


class AbstractEntrypoint(Protocol):
    """Entrypoint."""

    async def run(self) -> None:
        """Run the entrypoint."""

    @abc.abstractmethod
    def setup_exception_handlers(
        self, exception_handlers: dict[type[Exception], Callable[..., Any]], *args: Any, **kwargs: Any
    ) -> Self:
        """Setup exception handlers.

        Args:
            exception_handlers: The exception handlers.
            *args: The arguments to pass to the exception handlers.
            **kwargs: The keyword arguments to pass to the exception handlers.

        """


class EntrypointInconsistencyError(Exception):
    """Entrypoints inconsistency error."""

    def __init__(self, message: str) -> None:
        """Initialize the entrypoints inconsistency error."""
        super().__init__(message)
