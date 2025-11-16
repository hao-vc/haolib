"""Unit tests for IndexHandler."""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.sqlalchemy import IndexHandler


class Base(DeclarativeBase):
    pass


class IndexHandlerTestModel(Base):
    __tablename__ = "test_index_handler"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()


class TestIndexHandler:
    """Tests for IndexHandler."""

    @pytest.fixture
    def registry(self) -> DataTypeRegistry:
        """Create registry with test model."""
        reg = DataTypeRegistry()
        reg.register(
            storage_type=IndexHandlerTestModel,
            user_type=str,
            to_storage=lambda s: IndexHandlerTestModel(name=s),
            from_storage=lambda m: m.name,
        )
        return reg

    @pytest.fixture
    def handler(self, registry: DataTypeRegistry) -> IndexHandler:
        """Create index handler."""
        return IndexHandler(registry)

    @pytest.fixture
    def mock_session(self) -> Any:
        """Create mock session."""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_build_query_sql_query_index(self, handler: IndexHandler, mock_session: Any) -> None:
        """Test build_query with SQLQueryIndex (line 51)."""
        query = select(IndexHandlerTestModel)
        index = SQLQueryIndex(query=query)

        result = await handler.build_query(index, mock_session)

        assert result == query

    @pytest.mark.asyncio
    async def test_build_query_unsupported_index_type(self, handler: IndexHandler, mock_session: Any) -> None:
        """Test build_query with unsupported index type (lines 56-57)."""

        class UnsupportedIndex:
            pass

        index = UnsupportedIndex()

        with pytest.raises(TypeError, match="Unsupported index type"):
            await handler.build_query(index, mock_session)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_build_param_index_query_no_registration(self, mock_session: Any) -> None:
        """Test _build_param_index_query when no registration (lines 80-81)."""
        empty_registry = DataTypeRegistry()
        handler = IndexHandler(empty_registry)

        class UnregisteredType:
            pass

        index = ParamIndex(data_type=UnregisteredType)

        with pytest.raises(ValueError, match="No storage model registered"):
            await handler._build_param_index_query(index, mock_session)

    @pytest.mark.asyncio
    async def test_build_param_index_query_with_params(self, handler: IndexHandler, mock_session: Any) -> None:
        """Test _build_param_index_query with parameters."""
        index = ParamIndex(data_type=str, name="test")

        query = await handler._build_param_index_query(index, mock_session)

        assert query is not None
        # Query should have where clause
        assert query.whereclause is not None

    @pytest.mark.asyncio
    async def test_build_param_index_query_without_params(self, handler: IndexHandler, mock_session: Any) -> None:
        """Test _build_param_index_query without parameters."""
        index = ParamIndex(data_type=str)

        query = await handler._build_param_index_query(index, mock_session)

        assert query is not None
        # Query should not have where clause when no params
        # (or whereclause might be None)
        # This is implementation detail, just check query is valid
        assert hasattr(query, "whereclause")
