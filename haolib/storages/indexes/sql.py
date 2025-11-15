"""SQL query index for relational databases."""

from typing import Any, TypeVar

from sqlalchemy import Select

from haolib.storages.indexes.abstract import SearchIndex

T_Data = TypeVar("T_Data")


class SQLQueryIndex[T_Data](SearchIndex[T_Data]):
    """SQL index for relational databases (SQLAlchemy).

    Used for SQL-based storages like PostgreSQL, MySQL, etc.
    The query can be a SQLAlchemy Select, Update, or Delete statement.

    Example:
        ```python
        from sqlalchemy import select

        index = SQLQueryIndex(
            data_type=User,
            index_name="by_email",
            query=select(UserModel).where(UserModel.email == "john@example.com")
        )
        await storage.execute(reado(search_index=index))
        ```

    """

    def __init__(
        self,
        data_type: type[T_Data],
        index_name: str,
        query: Select[Any],
    ) -> None:
        """Create SQL query index.

        Args:
            data_type: Type of data to search.
            index_name: Name of the index.
            query: SQLAlchemy query (Select, Update, Delete).

        """
        self.__haolib_data_type__ = data_type
        self.__haolib_index_name__ = index_name
        self.__haolib_query__ = query

    @property
    def data_type(self) -> type[T_Data]:
        """Type of data to search.

        Returns:
            Type of data.

        """
        return self.__haolib_data_type__

    @property
    def index_name(self) -> str:
        """Name of the index.

        Returns:
            Index name.

        """
        return self.__haolib_index_name__

    @property
    def query(self) -> Select[Any]:
        """SQLAlchemy query (Select, Update, Delete).

        Returns:
            SQLAlchemy query.

        """
        return self.__haolib_query__

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"SQLQueryIndex(data_type={self.data_type.__name__}, index_name={self.index_name!r}, query={self.query!r})"
        )

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, SQLQueryIndex):
            return False
        return self.data_type == other.data_type and self.index_name == other.index_name and self.query == other.query

    def __hash__(self) -> int:
        """Hash for frozen dataclass-like behavior."""
        return hash((self.data_type, self.index_name, id(self.query)))
