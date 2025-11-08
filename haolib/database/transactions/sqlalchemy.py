"""SQLAlchemy transactions."""

from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession

from haolib.database.transactions.abstract import AbstractDatabaseTransaction, DatabaseTransactionError


class SQLAlchemyTransaction(AbstractDatabaseTransaction[AsyncSession]):
    """SQLAlchemy transaction."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the transaction.

        Args:
            session (AsyncSession): The session to use for the transaction.

        """
        self._session: AsyncSession | None = session

    async def __aenter__(self) -> Self:
        """Start the transaction."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """End the transaction."""
        # We need to set the session to None to fix the problem with the garbage collection.
        # Problem: when we pass an AsyncSession instance to a repository, it is not closed even after we exit
        # the context manager of the the session. To fix it, we add another abstraction layer on top of the session
        # and set the session to None when we exit the context manager.
        self._session = None

    async def close(self) -> None:
        """Close the session."""
        if self._session is None:
            raise DatabaseTransactionError("Transaction is already closed.")

        self._session = None

    async def get_manipulator(self) -> AsyncSession:
        """Get the session.

        Returns:
            AsyncSession: The session.

        Raises:
            DatabaseTransactionError: If the transaction is closed.

        """
        if self._session is None:
            raise DatabaseTransactionError("Transaction is closed. Did you exit the transaction context manager?")

        return self._session
