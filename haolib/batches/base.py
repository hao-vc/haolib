"""Base interface for batches."""

from typing import Protocol, Self


class BaseBatch[T_Id, T](Protocol):
    """Base interface for batches."""

    async def add_dict_data(self, data: dict[T_Id, T]) -> Self:
        """Return the batch from a dict.

        Returns:
            Self: The batch.


        """
        ...

    async def add_list_data(self, data: list[T]) -> Self:
        """Return the batch from a list.

        Returns:
            Self: The batch.

        """
        ...

    async def add_set_data(self, data: set[T]) -> Self:
        """Return the batch from a set.

        Returns:
            Self: The batch.

        """
        ...

    async def to_dict(self) -> dict[T_Id, T]:
        """Return the batch as a dict.

        Returns:
            dict[T_Id, T]: The batch as a dict.

        """
        ...

    async def to_list(self) -> list[T]:
        """Return the batch as a list.

        Returns:
            list[T]: The batch as a list.

        """
        ...

    async def to_set(self) -> set[T]:
        """Return the batch as a set.

        Returns:
            set[T]: The batch as a set.

        """
        ...

    async def get_first(self, exception: Exception | type[Exception]) -> T:
        """Get the first item in the batch.

        Args:
            exception (Exception | type[Exception]): The exception to raise if the batch is empty.

        Returns:
            T: The first item in the batch.

        Raises:
            exception: If the batch is empty.

        """
        ...

    async def get_size(self) -> int:
        """Get the size of the batch.

        Returns:
            int: The size of the batch.

        """
        ...

    async def get_unique_size(self) -> int:
        """Get the size of unique items in the batch.

        Returns:
            int: The size of unique items in the batch.

        """
        ...
