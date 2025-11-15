"""Unit tests for storage indexes."""

import pytest
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from haolib.storages.indexes.path import PathIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.vector import VectorSearchIndex


class Base(DeclarativeBase):
    """Base class for test models."""


class IndexTestModel(Base):
    """Test model for index tests."""

    __tablename__ = "test_indexes"
    id: Mapped[int] = mapped_column(primary_key=True)


class TestSQLQueryIndex:
    """Tests for SQLQueryIndex."""

    def test_init(self) -> None:
        """Test SQLQueryIndex initialization."""
        query = select(IndexTestModel)
        index = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        assert index.data_type == int
        assert index.index_name == "test"
        assert index.query == query

    def test_repr(self) -> None:
        """Test SQLQueryIndex string representation."""
        query = select(IndexTestModel)
        index = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        repr_str = repr(index)
        assert "SQLQueryIndex" in repr_str
        assert "int" in repr_str
        assert "test" in repr_str

    def test_eq(self) -> None:
        """Test SQLQueryIndex equality."""
        query1 = select(IndexTestModel)
        query2 = select(IndexTestModel)
        index1 = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query1,
        )
        index2 = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query2,
        )
        # Queries are different objects, so indexes are not equal
        assert index1 != index2

        # Same query object
        index3 = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query1,
        )
        assert index1 == index3

    def test_eq_different_type(self) -> None:
        """Test SQLQueryIndex equality with different type."""
        query = select(IndexTestModel)
        index = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        assert index != "not an index"

    def test_hash(self) -> None:
        """Test SQLQueryIndex hash."""
        query = select(IndexTestModel)
        index = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        # Hash should work (uses id(query) which is stable)
        hash_val = hash(index)
        assert isinstance(hash_val, int)

    def test_eq_with_same_query_object(self) -> None:
        """Test SQLQueryIndex equality with same query object (line 90)."""
        query = select(IndexTestModel)
        index1 = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        index2 = SQLQueryIndex(
            data_type=int,
            index_name="test",
            query=query,
        )
        # Same query object should make them equal
        assert index1 == index2


class TestVectorSearchIndex:
    """Tests for VectorSearchIndex."""

    def test_init(self) -> None:
        """Test VectorSearchIndex initialization."""
        index = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        assert index.data_type == str
        assert index.index_name == "test"
        assert index.query_text == "search"
        assert index.limit == 10
        assert index.threshold == 0.7

    def test_init_with_defaults(self) -> None:
        """Test VectorSearchIndex initialization with custom defaults."""
        index = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
            limit=20,
            threshold=0.8,
        )
        assert index.limit == 20
        assert index.threshold == 0.8

    def test_repr(self) -> None:
        """Test VectorSearchIndex string representation."""
        index = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        repr_str = repr(index)
        assert "VectorSearchIndex" in repr_str
        assert "str" in repr_str
        assert "test" in repr_str
        assert "search" in repr_str

    def test_eq(self) -> None:
        """Test VectorSearchIndex equality."""
        index1 = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        index2 = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        assert index1 == index2

    def test_eq_different(self) -> None:
        """Test VectorSearchIndex equality with different values."""
        index1 = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search1",
        )
        index2 = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search2",
        )
        assert index1 != index2

    def test_eq_different_type(self) -> None:
        """Test VectorSearchIndex equality with different type."""
        index = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        assert index != "not an index"

    def test_hash(self) -> None:
        """Test VectorSearchIndex hash."""
        index = VectorSearchIndex(
            data_type=str,
            index_name="test",
            query_text="search",
        )
        hash_val = hash(index)
        assert isinstance(hash_val, int)


class TestPathIndex:
    """Tests for PathIndex."""

    def test_init(self) -> None:
        """Test PathIndex initialization."""
        index = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        assert index.data_type == bytes
        assert index.index_name == "test"
        assert index.path == "/test/path"

    def test_repr(self) -> None:
        """Test PathIndex string representation."""
        index = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        repr_str = repr(index)
        assert "PathIndex" in repr_str
        assert "bytes" in repr_str
        assert "test" in repr_str
        assert "/test/path" in repr_str

    def test_eq(self) -> None:
        """Test PathIndex equality."""
        index1 = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        index2 = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        assert index1 == index2

    def test_eq_different(self) -> None:
        """Test PathIndex equality with different values."""
        index1 = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path1",
        )
        index2 = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path2",
        )
        assert index1 != index2

    def test_eq_different_type(self) -> None:
        """Test PathIndex equality with different type."""
        index = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        assert index != "not an index"

    def test_hash(self) -> None:
        """Test PathIndex hash."""
        index = PathIndex(
            data_type=bytes,
            index_name="test",
            path="/test/path",
        )
        hash_val = hash(index)
        assert isinstance(hash_val, int)


class TestParamIndex:
    """Tests for ParamIndex."""

    def test_create_index_function(self) -> None:
        """Test create_index convenience function (line 95)."""
        from haolib.storages.indexes.params import create_index

        index = create_index(int, age=25, active=True)

        assert index.data_type == int
        assert index.index_name == "dynamic"
        assert index.params == {"age": 25, "active": True}
