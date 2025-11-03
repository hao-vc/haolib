"""Base interface for batches."""

from collections.abc import Callable, Iterator
from typing import Protocol, Self, overload


class AbstractBatch[T_Key, T_Value](Protocol):
    """Base interface for batches.

    Batch here is defined as a collection of items that must be ordered,
    support indexing using ID (or, in general, any grouping key), and be able to be iterated over.
    """

    key_getter: Callable[[T_Value], T_Key]

    def __iter__(self) -> Iterator[T_Value]:
        """Iterate over the batch."""
        ...

    def __next__(self) -> T_Value:
        """Return the next item in the batch."""
        ...

    def __len__(self) -> int:
        """Get the size of the batch.

        Returns:
            int: The size of the batch.

        """
        ...

    def merge_dict(self, data: dict[T_Key, T_Value]) -> Self:
        """Merge data to the batch from a dict.

        Merging here means that the data will be added to the batch,
        and if the item already exists, it will be overwritten.

        Returns:
            Self: The batch.

        """
        ...

    def merge_list(self, data: list[T_Value]) -> Self:
        """Merge data to the batch from a list.

        Merging here means that the data will be added to the batch,
        and if the item already exists, it will be overwritten.

        Returns:
            Self: The batch.

        """
        ...

    def merge_set(self, data: set[T_Value]) -> Self:
        """Merge data to the batch from a set.

        Merging here means that the data will be added to the batch,
        and if the item already exists, it will be overwritten.

        Returns:
            Self: The batch.

        """
        ...

    def to_dict(self) -> dict[T_Key, T_Value]:
        """Get the batch as a dict.

        Returns:
            dict[T_Key, T_Value]: The batch as a dict.

        """
        ...

    def to_list(self) -> list[T_Value]:
        """Get the batch as a list.

        Returns:
            list[T_Value]: The batch as a list.

        """
        ...

    def to_set(self) -> set[T_Value]:
        """Get the batch as a set.

        Returns:
            set[T_Value]: The batch as a set.

        """
        ...

    @overload
    def get_by_index(self, index: int, exception: Exception | type[Exception]) -> T_Value: ...

    @overload
    def get_by_index(self, index: int, exception: None = None) -> T_Value | None: ...

    def get_by_index(self, index: int, exception: Exception | type[Exception] | None = None) -> T_Value | None:
        """Get the item by index.

        First item is at index 0

        The support of negative indexing depends on the implementation.


        Args:
            index (int): The index of the item.
            exception (Exception | type[Exception] | None): The exception to raise if the index is out of range.

        Returns:
            T | None: The item.

        """
        ...

    @overload
    def get_by_key(self, key: T_Key, exception: Exception | type[Exception]) -> T_Value: ...

    @overload
    def get_by_key(
        self,
        key: T_Key,
        exception: None = None,
    ) -> T_Value | None: ...

    def get_by_key(
        self,
        key: T_Key,
        exception: Exception | type[Exception] | None = None,
    ) -> T_Value | None:
        """Get the item by key.

        Args:
            key (T_Key): The key of the item.
            exception (Exception | type[Exception] | None): The exception to raise if the item is not found.


        Returns:
            T_Value | None: The item.

        """
        ...

    def get_keys(self) -> set[T_Key]:
        """Get the keys of the batch.

        Returns:
            set[T_Key]: The keys of the batch.

        """
        ...
