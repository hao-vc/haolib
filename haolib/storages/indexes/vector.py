"""Vector search index for vector databases (QDrant, Pinecone)."""

from typing import TypeVar

from haolib.storages.indexes.abstract import SearchIndex

T_Data = TypeVar("T_Data")


class VectorSearchIndex[T_Data](SearchIndex[T_Data]):
    """Vector search index for semantic search.

    Used for vector databases like QDrant, Pinecone, Weaviate, etc.
    Performs semantic/vector similarity search.

    Example:
        ```python
        index = VectorSearchIndex(
            data_type=Article,
            query_text="machine learning",
            limit=10,
            threshold=0.7
        )
        results = await storage.execute(reado(search_index=index))
        ```

    """

    def __init__(
        self,
        data_type: type[T_Data],
        query_text: str,
        limit: int = 10,
        threshold: float = 0.7,
    ) -> None:
        """Create vector search index.

        Args:
            data_type: Type of data to search.
            query_text: Query text for semantic search.
            limit: Maximum number of results to return.
            threshold: Similarity threshold (0.0 to 1.0).

        """
        self.__haolib_data_type__ = data_type
        self.__haolib_query_text__ = query_text
        self.__haolib_limit__ = limit
        self.__haolib_threshold__ = threshold

    @property
    def data_type(self) -> type[T_Data]:
        """Type of data to search.

        Returns:
            Type of data.

        """
        return self.__haolib_data_type__

    @property
    def query_text(self) -> str:
        """Query text for semantic search.

        Returns:
            Query text.

        """
        return self.__haolib_query_text__

    @property
    def limit(self) -> int:
        """Maximum number of results to return.

        Returns:
            Limit.

        """
        return self.__haolib_limit__

    @property
    def threshold(self) -> float:
        """Similarity threshold (0.0 to 1.0).

        Returns:
            Threshold.

        """
        return self.__haolib_threshold__

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"VectorSearchIndex(data_type={self.data_type.__name__}, "
            f"query_text={self.query_text!r}, "
            f"limit={self.limit}, threshold={self.threshold})"
        )

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, VectorSearchIndex):
            return False
        return (
            self.data_type == other.data_type
            and self.query_text == other.query_text
            and self.limit == other.limit
            and self.threshold == other.threshold
        )

    def __hash__(self) -> int:
        """Hash for frozen dataclass-like behavior."""
        return hash((self.data_type, self.query_text, self.limit, self.threshold))
