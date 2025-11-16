"""Abstract search index protocol."""

from typing import Protocol, TypeVar, runtime_checkable

T_Data = TypeVar("T_Data")


@runtime_checkable
class SearchIndex[T_Data](Protocol):
    """Base protocol for search indexes.

    Index is explicitly bound to a data type it searches for.
    Each storage implementation can have its own index types.

    Example:
        ```python
        # SQL index
        index = SQLQueryIndex(
            query=select(UserModel).where(UserModel.email == "john@example.com")
        )

        # Path index for S3
        index = PathIndex(
            data_type=Document,
            path="documents/{category}/{filename}"
        )
        ```

    """

    @property
    def data_type(self) -> type[T_Data]:
        """Type of data this index searches for.

        Returns:
            Type of data.

        """
        ...
