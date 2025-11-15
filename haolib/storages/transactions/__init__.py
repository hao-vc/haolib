"""Transactions for the storages."""

from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self

if TYPE_CHECKING:
    from haolib.storages.transactions.sqlalchemy import SQLAlchemyStorageTransaction

__all__ = [
    "Transaction",
]


class Transaction(Protocol):
    """Transaction protocol for storage operations.

    Provides ACID guarantees for transactional storages.
    Transactions are now internal to storage implementations.
    """

    async def __aenter__(self) -> Self:
        """Enter transaction context."""
        ...

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exit transaction context (commit or rollback)."""
        ...

    async def commit(self) -> None:
        """Commit the transaction."""
        ...

    async def rollback(self) -> None:
        """Rollback the transaction."""
        ...


# Import after Transaction is defined to avoid circular import
from haolib.storages.transactions.sqlalchemy import SQLAlchemyStorageTransaction  # noqa: E402

__all__.extend(["SQLAlchemyStorageTransaction"])
