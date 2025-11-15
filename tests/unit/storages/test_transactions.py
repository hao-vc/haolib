"""Unit tests for SQLAlchemyStorageTransaction."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from haolib.storages.transactions.sqlalchemy import SQLAlchemyStorageTransaction


class TestSQLAlchemyStorageTransaction:
    """Tests for SQLAlchemyStorageTransaction."""

    @pytest.mark.asyncio
    async def test_aenter(self) -> None:
        """Test transaction context manager entry."""
        session = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)

        result = await transaction.__aenter__()

        assert result is transaction
        assert transaction._in_transaction is True

    @pytest.mark.asyncio
    async def test_aexit_no_exception(self) -> None:
        """Test transaction context manager exit without exception."""
        session = AsyncMock()
        session.commit = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        await transaction.__aexit__(None, None, None)

        session.commit.assert_called_once()
        assert transaction._in_transaction is False

    @pytest.mark.asyncio
    async def test_aexit_with_exception(self) -> None:
        """Test transaction context manager exit with exception."""
        session = AsyncMock()
        session.rollback = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        await transaction.__aexit__(ValueError, ValueError("test"), None)

        session.rollback.assert_called_once()
        assert transaction._in_transaction is False

    @pytest.mark.asyncio
    async def test_aexit_commit_error(self) -> None:
        """Test transaction context manager exit with commit error."""
        session = AsyncMock()
        session.commit = AsyncMock(side_effect=SQLAlchemyError("commit failed"))
        session.rollback = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        with pytest.raises(SQLAlchemyError):
            await transaction.__aexit__(None, None, None)

        session.commit.assert_called_once()
        session.rollback.assert_called_once()
        # _in_transaction remains True because exception interrupts execution

    @pytest.mark.asyncio
    async def test_aexit_closed_session(self) -> None:
        """Test transaction context manager exit with closed session."""
        transaction = SQLAlchemyStorageTransaction(AsyncMock())
        transaction._session = None

        # Should not raise
        await transaction.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_commit_success(self) -> None:
        """Test successful commit."""
        session = AsyncMock()
        session.commit = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        await transaction.commit()

        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_commit_error(self) -> None:
        """Test commit with error."""
        session = AsyncMock()
        session.commit = AsyncMock(side_effect=SQLAlchemyError("commit failed"))
        session.rollback = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        with pytest.raises(SQLAlchemyError):
            await transaction.commit()

        session.commit.assert_called_once()
        session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_commit_closed_session(self) -> None:
        """Test commit with closed session."""
        transaction = SQLAlchemyStorageTransaction(AsyncMock())
        transaction._session = None

        with pytest.raises(RuntimeError, match="Transaction session is closed"):
            await transaction.commit()

    @pytest.mark.asyncio
    async def test_commit_not_in_transaction(self) -> None:
        """Test commit when not in transaction."""
        session = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = False

        with pytest.raises(RuntimeError, match="Transaction is not active"):
            await transaction.commit()

    @pytest.mark.asyncio
    async def test_rollback_success(self) -> None:
        """Test successful rollback."""
        session = AsyncMock()
        session.rollback = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = True

        await transaction.rollback()

        session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_rollback_closed_session(self) -> None:
        """Test rollback with closed session."""
        transaction = SQLAlchemyStorageTransaction(AsyncMock())
        transaction._session = None

        with pytest.raises(RuntimeError, match="Transaction session is closed"):
            await transaction.rollback()

    @pytest.mark.asyncio
    async def test_rollback_not_in_transaction(self) -> None:
        """Test rollback when not in transaction."""
        session = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)
        transaction._in_transaction = False

        with pytest.raises(RuntimeError, match="Transaction is not active"):
            await transaction.rollback()

    @pytest.mark.asyncio
    async def test_get_session_success(self) -> None:
        """Test getting session successfully."""
        session = AsyncMock()
        transaction = SQLAlchemyStorageTransaction(session)

        result = await transaction.get_session()

        assert result is session

    @pytest.mark.asyncio
    async def test_get_session_closed(self) -> None:
        """Test getting session when closed."""
        transaction = SQLAlchemyStorageTransaction(AsyncMock())
        transaction._session = None

        with pytest.raises(RuntimeError, match="Transaction session is closed"):
            await transaction.get_session()

    @pytest.mark.asyncio
    async def test_aenter_closed_session(self) -> None:
        """Test entering transaction with closed session."""
        transaction = SQLAlchemyStorageTransaction(AsyncMock())
        transaction._session = None

        with pytest.raises(RuntimeError, match="Transaction session is closed"):
            await transaction.__aenter__()
