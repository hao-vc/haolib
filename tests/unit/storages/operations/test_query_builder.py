"""Tests for QueryBuilder."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel
from haolib.storages.data_types.registry import DataTypeRegistry, TypeRegistration
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.operations.concrete import FilterOperation, ReadOperation
from haolib.storages.operations.optimizer.query_builder import QueryBuilder


class UserModel(SQLAlchemyBaseModel):
    """Test user model."""

    __tablename__ = "test_users_query"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    age: Mapped[int] = mapped_column()


class User:
    """Test user class."""

    def __init__(self, id: int, name: str, age: int) -> None:
        self.id = id
        self.name = name
        self.age = age


# Define predicate functions at module level
def predicate_age_ge_18(u: Any) -> bool:
    """Predicate for age >= 18."""
    return u.age >= 18


def predicate_age_le_65(u: Any) -> bool:
    """Predicate for age <= 65."""
    return u.age <= 65


def predicate_name_starts_with_a(u: Any) -> bool:
    """Predicate that cannot be converted."""
    return u.name.startswith("A")


@pytest.fixture
def registry() -> DataTypeRegistry:
    """Create data type registry."""
    reg = DataTypeRegistry()
    reg.register(
        storage_type=UserModel,
        user_type=User,
        to_storage=lambda u: UserModel(id=u.id, name=u.name, age=u.age),
        from_storage=lambda m: User(id=m.id, name=m.name, age=m.age),
    )
    return reg


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create mock session."""
    return AsyncMock()


class TestQueryBuilder:
    """Tests for QueryBuilder."""

    @pytest.mark.asyncio
    async def test_build_async_empty_operations(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async with empty operations."""
        builder = QueryBuilder(registry)
        result = await builder.build_async([], mock_session)

        assert result is None

    @pytest.mark.asyncio
    async def test_build_async_no_read_operation(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async without ReadOperation."""
        builder = QueryBuilder(registry)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        result = await builder.build_async([filter_op], mock_session)

        assert result is None

    @pytest.mark.asyncio
    async def test_build_async_no_registration(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async with unregistered type."""
        builder = QueryBuilder(registry)
        index = ParamIndex(data_type=Any, age=25)
        read_op = ReadOperation(search_index=index)

        # Mock IndexHandler.build_query
        from unittest.mock import patch

        with patch("haolib.storages.operations.optimizer.query_builder.IndexHandler") as mock_handler:
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            mock_handler_instance.build_query = AsyncMock(return_value=select(UserModel))

            result = await builder.build_async([read_op], mock_session)

            assert result is None

    @pytest.mark.asyncio
    async def test_build_async_single_read_operation(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async with single ReadOperation."""
        builder = QueryBuilder(registry)
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        # Mock IndexHandler.build_query
        from unittest.mock import patch

        with patch("haolib.storages.operations.optimizer.query_builder.IndexHandler") as mock_handler:
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            base_query = select(UserModel)
            mock_handler_instance.build_query = AsyncMock(return_value=base_query)

            result = await builder.build_async([read_op], mock_session)

            assert result is not None
            assert isinstance(result, ReadOperation)
            assert isinstance(result.search_index, SQLQueryIndex)
            # data_type is automatically extracted from query, cannot be accessed directly
            # Just verify the optimized index is created correctly

    @pytest.mark.asyncio
    async def test_build_async_with_filter(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async with ReadOperation and FilterOperation."""
        builder = QueryBuilder(registry)
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        filter_op = FilterOperation(predicate=predicate_age_ge_18)

        # Mock IndexHandler.build_query
        from unittest.mock import patch

        with patch("haolib.storages.operations.optimizer.query_builder.IndexHandler") as mock_handler:
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            base_query = select(UserModel)
            mock_handler_instance.build_query = AsyncMock(return_value=base_query)

            result = await builder.build_async([read_op, filter_op], mock_session)

            assert result is not None
            assert isinstance(result, ReadOperation)
            assert isinstance(result.search_index, SQLQueryIndex)

    @pytest.mark.asyncio
    async def test_build_async_filter_cannot_convert(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async when filter cannot be converted."""
        builder = QueryBuilder(registry)
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        # Filter with method call (cannot be converted)
        filter_op = FilterOperation(predicate=predicate_name_starts_with_a)

        # Mock IndexHandler.build_query
        from unittest.mock import patch

        with patch("haolib.storages.operations.optimizer.query_builder.IndexHandler") as mock_handler:
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            base_query = select(UserModel)
            mock_handler_instance.build_query = AsyncMock(return_value=base_query)

            result = await builder.build_async([read_op, filter_op], mock_session)

            assert result is None

    @pytest.mark.asyncio
    async def test_build_async_multiple_filters(self, registry: DataTypeRegistry, mock_session: AsyncMock) -> None:
        """Test build_async with multiple filters."""
        builder = QueryBuilder(registry)
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        filter1 = FilterOperation(predicate=predicate_age_ge_18)
        filter2 = FilterOperation(predicate=predicate_age_le_65)

        # Mock IndexHandler.build_query
        from unittest.mock import patch

        with patch("haolib.storages.operations.optimizer.query_builder.IndexHandler") as mock_handler:
            mock_handler_instance = MagicMock()
            mock_handler.return_value = mock_handler_instance
            base_query = select(UserModel)
            mock_handler_instance.build_query = AsyncMock(return_value=base_query)

            result = await builder.build_async([read_op, filter1, filter2], mock_session)

            assert result is not None
            assert isinstance(result, ReadOperation)
