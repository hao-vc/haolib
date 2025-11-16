"""SQL query index for relational databases."""

from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import Select
from sqlalchemy.orm import DeclarativeBase

from haolib.storages.indexes.abstract import SearchIndex

if TYPE_CHECKING:
    from haolib.storages.data_types.registry import DataTypeRegistry

T_Data = TypeVar("T_Data")


class SQLQueryIndex[T_Data](SearchIndex[T_Data]):
    """SQL index for relational databases (SQLAlchemy).

    Used for SQL-based storages like PostgreSQL, MySQL, etc.
    The query can be a SQLAlchemy Select, Update, or Delete statement.
    Data type is automatically extracted from the query when needed.

    Example:
        ```python
        from sqlalchemy import select

        index = SQLQueryIndex(
            query=select(UserModel).where(UserModel.email == "john@example.com")
        )
        await storage.execute(reado(search_index=index))
        ```

    """

    def __init__(
        self,
        query: Select[Any],
    ) -> None:
        """Create SQL query index.

        Args:
            query: SQLAlchemy query (Select, Update, Delete).

        """
        self.__haolib_query__ = query

    @property
    def query(self) -> Select[Any]:
        """SQLAlchemy query (Select, Update, Delete).

        Returns:
            SQLAlchemy query.

        """
        return self.__haolib_query__

    @property
    def data_type(self) -> type[T_Data]:
        """Type of data to search.

        Note: This property requires a DataTypeRegistry to extract the type from query.
        In practice, the type is extracted automatically by IndexHandler when needed.

        Returns:
            Type of data (extracted from query via registry).

        Raises:
            NotImplementedError: If called directly without registry context.
                In practice, IndexHandler._get_data_type_from_query() should be used instead.

        """
        # This property is required by SearchIndex protocol, but in practice
        # the type is extracted by IndexHandler._get_data_type_from_query().
        # We raise NotImplementedError to indicate this should not be called directly.
        msg = (
            "SQLQueryIndex.data_type cannot be accessed directly. "
            "Type is automatically extracted from query by IndexHandler when needed."
        )
        raise NotImplementedError(msg)

    def __repr__(self) -> str:
        """String representation."""
        return f"SQLQueryIndex(query={self.query!r})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, SQLQueryIndex):
            return False
        return self.query == other.query

    def __hash__(self) -> int:
        """Hash for frozen dataclass-like behavior."""
        return hash(id(self.query))
