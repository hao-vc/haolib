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
            data_type=User,
            index_name="by_email",
            query=select(UserModel).where(UserModel.email == "john@example.com")
        )

        # Path index for S3
        index = PathIndex(
            data_type=Document,
            index_name="by_path",
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

    @property
    def index_name(self) -> str:
        """Name of the index for identification.

        Returns:
            Index name.

        """
        ...
