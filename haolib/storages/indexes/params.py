"""Parameter-based search index."""

from typing import Any, TypeVar

from haolib.storages.indexes.abstract import SearchIndex

T_Data = TypeVar("T_Data")


class ParamIndex[T_Data](SearchIndex[T_Data]):
    """Index built from parameters.

    Used when you need to create an index from parameters
    rather than using a predefined one.

    Example:
        ```python
        # Create index from parameters
        index = ParamIndex(User, age=25, is_active=True)
        await storage.execute(reado(search_index=index))

        # Or using convenience function
        from haolib.storages.indexes import index
        user_index = index(User, age=25, is_active=True)
        await storage.execute(reado(search_index=user_index))
        ```

    """

    def __init__(
        self,
        data_type: type[T_Data],
        **params: Any,
    ) -> None:
        """Create index from parameters.

        Args:
            data_type: Type of data to search.
            **params: Search parameters (e.g., age=25, email="john@example.com").

        """
        self.__haolib_data_type__ = data_type
        self.params = params

    @property
    def data_type(self) -> type[T_Data]:
        """Type of data to search.

        Returns:
            Type of data.

        """
        return self.__haolib_data_type__


def create_index[T_Data](
    data_type: type[T_Data],
    **params: Any,
) -> ParamIndex[T_Data]:
    """Create a parameter-based index.

    Convenience function for creating ParamIndex.

    Args:
        data_type: Type of data to search.
        **params: Search parameters.

    Returns:
        ParamIndex for the given data type.

    Example:
        ```python
        from haolib.storages.indexes import create_index

        user_index = create_index(User, age=25, is_active=True)
        await storage.execute(reado(search_index=user_index))
        ```

    """
    return ParamIndex(data_type=data_type, **params)
