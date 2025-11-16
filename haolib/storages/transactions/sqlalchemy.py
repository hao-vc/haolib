"""SQLAlchemy transaction for storages."""

from types import TracebackType
from typing import Self

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from haolib.storages.transactions import Transaction


class SQLAlchemyStorageTransaction(Transaction):
    """SQLAlchemy transaction for storage operations.

    Wraps AsyncSession to provide Transaction protocol.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the transaction.

        Args:
            session: The AsyncSession to use for the transaction.

        """
        self._session: AsyncSession | None = session
        self._in_transaction = False

    async def __aenter__(self) -> Self:
        """Start the transaction."""
        if self._session is None:
            raise RuntimeError("Transaction session is closed.")
        self._in_transaction = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """End the transaction (commit or rollback)."""
        if self._session is None:
            return

        if exc_type is None:
            # No exception - commit
            try:
                await self._session.commit()
            except SQLAlchemyError:
                await self._session.rollback()
                raise
        else:
            # Exception occurred - rollback
            await self._session.rollback()

        # We need to set the session to None to fix the problem with the garbage collection.
        self._session = None

        self._in_transaction = False

    async def commit(self) -> None:
        """Commit the transaction."""
        if self._session is None:
            raise RuntimeError("Transaction session is closed.")
        if not self._in_transaction:
            raise RuntimeError("Transaction is not active.")

        try:
            await self._session.commit()
        except SQLAlchemyError:
            await self._session.rollback()
            raise

    async def rollback(self) -> None:
        """Rollback the transaction."""
        if self._session is None:
            raise RuntimeError("Transaction session is closed.")
        if not self._in_transaction:
            raise RuntimeError("Transaction is not active.")

        await self._session.rollback()

    async def get_session(self) -> AsyncSession:
        """Get the underlying AsyncSession.

        Returns:
            AsyncSession: The session.

        Raises:
            RuntimeError: If the transaction is closed.

        """
        if self._session is None:
            raise RuntimeError("Transaction session is closed.")
        return self._session
