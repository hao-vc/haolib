"""Abstract database transactions."""

from types import TracebackType
from typing import Any, Protocol, Self


class AbstractDatabaseTransaction[DBManipulator: Any](Protocol):
    """Abstract database transaction."""

    async def __aenter__(self) -> Self:
        """Enter the transaction."""
        ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Leave the transaction."""
        ...

    async def get_manipulator(self) -> DBManipulator:
        """Get the database manipulator."""
        ...


class DatabaseTransactionError(Exception):
    """Database transaction error."""
