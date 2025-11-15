"""Path index for file-based storages (S3, filesystem)."""

from typing import TypeVar

from haolib.storages.indexes.abstract import SearchIndex

T_Data = TypeVar("T_Data")


class PathIndex[T_Data](SearchIndex[T_Data]):
    """Index for storages with paths (S3, filesystem).

    Used for object storage like S3, Azure Blob, or local filesystem.

    Example:
        ```python
        index = PathIndex(
            data_type=Document,
            index_name="by_path",
            path="documents/reports/report.pdf"
        )
        await storage.execute(reado(search_index=index))
        ```

    """

    def __init__(
        self,
        data_type: type[T_Data],
        index_name: str,
        path: str,
    ) -> None:
        """Create path index.

        Args:
            data_type: Type of data to search.
            index_name: Name of the index.
            path: Path to the data.

        """
        self.__haolib_data_type__ = data_type
        self.__haolib_index_name__ = index_name
        self.__haolib_path__ = path

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
    def path(self) -> str:
        """Path to the data.

        Returns:
            Path.

        """
        return self.__haolib_path__

    def __repr__(self) -> str:
        """String representation."""
        return f"PathIndex(data_type={self.data_type.__name__}, index_name={self.index_name!r}, path={self.path!r})"

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, PathIndex):
            return False
        return self.data_type == other.data_type and self.index_name == other.index_name and self.path == other.path

    def __hash__(self) -> int:
        """Hash for frozen dataclass-like behavior."""
        return hash((self.data_type, self.index_name, self.path))
